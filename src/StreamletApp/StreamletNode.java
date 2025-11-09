package StreamletApp;

import URB.URBNode;
import utils.application.*;
import utils.communication.Address;
import utils.communication.PeerInfo;
import utils.logs.AppLogger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

record SeenProposal(int leader, int epoch) {
}

public class StreamletNode {
    public static final int BLOCK_CHAIN_PRINT_EPOCH_FREQUENCY = 5;
    private static final int CONFUSION_START = 0;
    private static final int CONFUSION_DURATION = 2;
    private final int deltaInSeconds;
    private final LocalDateTime start;
    private final int numberOfDistinctNodes;
    private final TransactionPoolSimulator transactionPoolSimulator;
    private final AtomicInteger currentEpoch = new AtomicInteger(0);
    private final AtomicInteger currentLeaderId = new AtomicInteger(0);

    private final int localId;
    private final URBNode urbNode;
    private final BlockchainManager blockchainManager;
    private final Map<Block, Set<Integer>> votedBlocks = new HashMap<>();
    private final BlockingQueue<Message> derivableQueue = new LinkedBlockingQueue<>(1000);
    private final Set<SeenProposal> seenProposals = new HashSet<>();
    private final ConcurrentLinkedQueue<Transaction> clientPendingTransactionsQueue = new ConcurrentLinkedQueue<>();


    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final CountDownLatch waitForCatchUp = new CountDownLatch(1);
    private final boolean isClientGeneratingTransactions;
    private final Address myClientAddress;

    private Random random = new Random(1L); // To determine epoch leader
    private volatile boolean needsToRecover = false;

    public StreamletNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo, int deltaInSeconds,
                         LocalDateTime start, boolean isClientGeneratingTransactions, Address myClientAddress)
            throws IOException {
        localId = localPeerInfo.id();
        numberOfDistinctNodes = 1 + remotePeersInfo.size();
        this.deltaInSeconds = deltaInSeconds;
        this.start = start;
        transactionPoolSimulator = new TransactionPoolSimulator(numberOfDistinctNodes);
        blockchainManager = new BlockchainManager();
        urbNode = new URBNode(localPeerInfo, remotePeersInfo, derivableQueue::add);
        this.isClientGeneratingTransactions = isClientGeneratingTransactions;
        this.myClientAddress = myClientAddress;
    }

    public void startProtocol() throws InterruptedException {
        launchThreads();

        long epochDuration = 2L * deltaInSeconds;
        long nanoSecondsToWait = waitStartOrRecover();
        scheduler.scheduleAtFixedRate(
            this::safeAdvanceEpoch, nanoSecondsToWait, (long) (epochDuration*1e9), TimeUnit.NANOSECONDS
        );
    }

    private void launchThreads() {
        executor.submit(() -> {
            try {
                urbNode.startURBNode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        executor.submit(this::consumeMessages);
        if (isClientGeneratingTransactions) executor.submit(this::receiveClientTransactionsRequests);
    }

    private long waitStartOrRecover() {
        LocalDateTime now = LocalDateTime.now();
        if (now.isBefore(start)) return waitStart();
        if (now.isAfter(start)) return recover();
        return 0;
    }

    private long waitStart() {
        AppLogger.logInfo("Waiting for protocol to start...");
        long nanoSecondsToWait = ChronoUnit.NANOS.between(LocalDateTime.now(), start);
        return nanoSecondsToWait > 0 ? nanoSecondsToWait : 0;
    }

    private long recover() {
        AppLogger.logInfo("(Re)joining in late...");
        needsToRecover = true;

        int epochDuration = 2 * this.deltaInSeconds;
        long protocolAgeInSeconds = ChronoUnit.SECONDS.between(start, LocalDateTime.now());
        // Ignore the seconds spent on the current epoch
        protocolAgeInSeconds -= protocolAgeInSeconds % epochDuration;
        int ongoingEpoch = (int) (protocolAgeInSeconds / epochDuration);
        currentEpoch.compareAndSet(0, ongoingEpoch + 1);

        LocalDateTime nextEpochDate = start.plusSeconds(protocolAgeInSeconds + epochDuration);
        long nanoSecondsToWait = ChronoUnit.NANOS.between(
            LocalDateTime.now(), nextEpochDate
        );
        return nanoSecondsToWait > 0 ? nanoSecondsToWait : 0;
    }

    private void catchUp(int fromEpoch, int toEpoch) {
        MissingEpochs missingEpochs = new MissingEpochs(fromEpoch, toEpoch);
        Message join = new Message(MessageType.JOIN, missingEpochs, localId);
        urbNode.broadcastFromLocal(join);

        try {
            waitForCatchUp.await();
        } catch (InterruptedException e) {
            AppLogger.logError("Failed to wait for missing blocks - Interrupted", e);
        }
    }

    private void safeAdvanceEpoch() {
        try {
            advanceEpoch();
        } catch (Exception e) {
            AppLogger.logError("Error advancing epoch: " + e.getMessage(), e);
        }
    }

    private void advanceEpoch() {
        int epoch = currentEpoch.get();
        // After catching up, this node receives the already advanced Random (if not in confusion epoch)
        if (needsToRecover) catchUp(blockchainManager.getLastEpoch() + 1, epoch);
        else calculateLeaderId(epoch);
        AppLogger.logInfo("#### EPOCH = " + epoch + " LEADER = " + currentLeaderId + " ####");

        if (localId == currentLeaderId.get()) {
            try {
                if (!isClientGeneratingTransactions || !clientPendingTransactionsQueue.isEmpty()) {
                    AppLogger.logDebug("Node " + localId + " is leader: proposing new block");
                    proposeNewBlock(epoch);
                }
                proposeNewBlock(epoch);
            } catch (NoSuchAlgorithmException e) {
                AppLogger.logError("Error proposing new block: " + e.getMessage(), e);
            }
        }
        if (epoch != 0 && epoch % BLOCK_CHAIN_PRINT_EPOCH_FREQUENCY == 0)
            blockchainManager.printBiggestFinalizedChain();
        currentEpoch.incrementAndGet();
    }

    private void consumeMessages() {
        final Queue<Message> bufferedMessages = new LinkedList<>();
        try {
            while (true) {
                Message message = derivableQueue.poll(100, TimeUnit.MILLISECONDS);
                if (message == null) continue;

                if (inConfusionEpoch(currentEpoch.get())) {
                    bufferedMessages.add(message);
                    continue;
                }

                // Store new proposals until this node is done recovering
                if (needsToRecover && message.type().equals(MessageType.PROPOSE)) {
                    bufferedMessages.add(message);
                    continue;
                }

                // This node cannot help if it is also recovering
                if (needsToRecover && message.type().equals(MessageType.JOIN)) {
                    continue;
                }

                if (!needsToRecover)
                    while (!bufferedMessages.isEmpty())
                        handleMessageDelivery(bufferedMessages.poll());

                handleMessageDelivery(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            AppLogger.logWarning("Message consumer thread interrupted");
        }
    }

    private void proposeNewBlock(int epoch) throws NoSuchAlgorithmException {
        Block parent = blockchainManager.getBiggestNotarizedChain().getLast();
        Transaction[] transactions;
        if (isClientGeneratingTransactions) {
            transactions = new Transaction[clientPendingTransactionsQueue.size()];
            int i = 0;
            while (!clientPendingTransactionsQueue.isEmpty()) {
                transactions[i++] = clientPendingTransactionsQueue.poll();
            }
        } else {
            transactions = transactionPoolSimulator.generateTransactions();
        }

        Block newBlock = new Block(parent.getSHA1(), epoch, parent.length() + 1, transactions);
        AppLogger.logDebug("Proposed block: " + newBlock + " with transactions: " + Arrays.toString(transactions));
        urbNode.broadcastFromLocal(new Message(MessageType.PROPOSE, newBlock, localId));
    }

    private void handleMessageDelivery(Message message) {
        AppLogger.logDebug("Delivering message from " + message.sender() + ": " + message.type());
        switch (message.type()) {
            case PROPOSE -> handlePropose(message);
            case VOTE -> handleVote(message);
            case JOIN -> handleJoin(message);
            case UPDATE -> handleUpdate(message);
            default -> {}
        }
    }

    private void handlePropose(Message message) {
        Block fullBlock = (Block) message.content();
        SeenProposal proposal = new SeenProposal(message.sender(), fullBlock.epoch());

        if (seenProposals.contains(proposal)
                || !blockchainManager.onPropose(fullBlock))
            return;
        seenProposals.add(proposal);

        Block blockHeader = new Block(fullBlock.parentHash(), fullBlock.epoch(), fullBlock.length(), new Transaction[0]);
        urbNode.broadcastFromLocal(new Message(MessageType.VOTE, blockHeader, localId));
        AppLogger.logDebug("Voted for block from leader " + message.sender() + " epoch " + fullBlock.epoch());
    }


    private void handleVote(Message message) {
        Block block = (Block) message.content();
        votedBlocks.computeIfAbsent(block, _ -> new HashSet<>()).add(message.sender());

        if (votedBlocks.get(block).size() > numberOfDistinctNodes / 2) {
            blockchainManager.notarizeBlock(block);
        }
    }

    private void handleJoin(Message message) {
        if (message.sender() == localId) return;

        MissingEpochs missingEpochs = (MissingEpochs) message.content();
        LinkedList<Block> missingBlocks = blockchainManager.blocksFromToEpoch(missingEpochs.from(), missingEpochs.to());

        Message catchUp = new Message(
            MessageType.UPDATE,
            new CatchUp(message.sender(), missingBlocks, random, currentLeaderId.get(), currentEpoch.get()),
            localId
        );
        urbNode.broadcastFromLocal(catchUp);
    }

    private void handleUpdate(Message message) {
        if (!needsToRecover) return;
        CatchUp catchUp = (CatchUp) message.content();
        if (catchUp.slackerId() != localId) return;

        this.random = catchUp.leaderRand();
        this.currentLeaderId.set(catchUp.leaderId());
        blockchainManager.insertMissingBlocks(catchUp.missingChain());

        needsToRecover = false;
        AppLogger.logInfo("Finished recovering");
        waitForCatchUp.countDown();
    }

    private boolean inConfusionEpoch(int epoch) {
        return epoch >= CONFUSION_START && epoch <= CONFUSION_START + CONFUSION_DURATION - 1;
    }

    private void calculateLeaderId(int epoch) {
        if (inConfusionEpoch(epoch)) currentLeaderId.set(epoch % numberOfDistinctNodes);
        else currentLeaderId.set(random.nextInt(numberOfDistinctNodes));
    }

    private void receiveClientTransactionsRequests() {
        try (ServerSocket serverSocket = new ServerSocket(myClientAddress.port())) {
            AppLogger.logInfo("Transaction client server listening on port " + myClientAddress.port());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> handleReceiveClientRequest(clientSocket));
            }
        } catch (IOException e) {
            AppLogger.logError("Error in transaction client server: " + e.getMessage(), e);
        }
    }

    private void handleReceiveClientRequest(Socket clientSocket) {
        AppLogger.logDebug("Handling client " + clientSocket.getInetAddress() + " connection...");
        try (Socket s = clientSocket;
             ObjectInputStream ois = new ObjectInputStream(s.getInputStream())) {

            while (true) {
                try {
                    Transaction transaction = (Transaction) ois.readObject();
                    AppLogger.logInfo("Received transaction from client " + s.getInetAddress() + ": " + transaction);
                    clientPendingTransactionsQueue.add(transaction);
                } catch (ClassNotFoundException e) {
                    AppLogger.logError("Received unknown object from client " + s.getInetAddress(), e);
                }
            }

        } catch (IOException e) {
            AppLogger.logInfo("Client " + clientSocket.getInetAddress() + " disconnected.");
        }
    }
}