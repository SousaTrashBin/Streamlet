
package app;

import urb.URBNode;
import utils.application.*;
import utils.communication.Address;
import utils.communication.PeerInfo;
import utils.logs.AppLogger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

record SeenProposal(int leader, int epoch) {
}

public class StreamletNode {
    public static final int BLOCKCHAIN_PRINT_EPOCH_INTERVAL = 5;
    private static final int CONFUSION_EPOCH_START = 0;
    private static final int CONFUSION_EPOCH_DURATION = 2;
    private static final int BLOCKCHAIN_PERSISTENCE_INTERVAL = 10;
    private final int deltaInSeconds;
    private final LocalDateTime protocolStartTime;
    private final int numberOfNodes;
    private final TransactionPoolSimulator transactionPoolSimulator;
    private final AtomicInteger currentEpoch = new AtomicInteger(0);
    private final Random epochLeaderRandomizer = new Random(1L);

    private final int localNodeId;
    private final URBNode urbNode;
    private final BlockchainManager blockchainManager;
    private final Map<Block, Set<Integer>> blockVotes = new HashMap<>();
    private final BlockingQueue<Message> deliveredMessagesQueue = new LinkedBlockingQueue<>(1000);
    private final Set<SeenProposal> seenProposals = new HashSet<>();
    private final ConcurrentLinkedQueue<Transaction> pendingClientTransactions = new ConcurrentLinkedQueue<>();

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService epochScheduler = Executors.newSingleThreadScheduledExecutor();
    private final boolean isClientGeneratingTransactions;
    private final Address clientServerAddress;

    private volatile boolean needsToRecover = false;

    public StreamletNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo, int deltaInSeconds,
                         LocalDateTime protocolStartTime, boolean isClientGeneratingTransactions, Address clientServerAddress)
            throws IOException {
        localNodeId = localPeerInfo.id();
        numberOfNodes = 1 + remotePeersInfo.size();
        this.deltaInSeconds = deltaInSeconds;
        this.protocolStartTime = protocolStartTime;
        transactionPoolSimulator = new TransactionPoolSimulator(numberOfNodes);
        blockchainManager = new BlockchainManager(Path.of("output", "node_%d".formatted(localNodeId))); // output/node_1/log.txt output/node_1/blockChain.txt
        urbNode = new URBNode(localPeerInfo, remotePeersInfo, deliveredMessagesQueue::add);
        this.isClientGeneratingTransactions = isClientGeneratingTransactions;
        this.clientServerAddress = clientServerAddress;
    }

    public void startProtocol() {
        launchBackgroundThreads();

        long epochDurationNanos = 2L * deltaInSeconds * 1_000_000_000L;
        long delayUntilFirstEpochNanos = calculateDelayUntilFirstEpoch();
        epochScheduler.scheduleAtFixedRate(
                this::safeAdvanceEpoch, delayUntilFirstEpochNanos, epochDurationNanos, TimeUnit.NANOSECONDS
        );
    }

    private void launchBackgroundThreads() {
        executor.submit(() -> {
            try {
                urbNode.startURBNode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        executor.submit(this::processDeliveredMessages);
        if (isClientGeneratingTransactions) {
            executor.submit(this::acceptClientTransactions);
        }
    }

    private long calculateDelayUntilFirstEpoch() {
        LocalDateTime now = LocalDateTime.now();
        if (now.isBefore(protocolStartTime)) {
            return calculateDelayUntilProtocolStart();
        }
        if (now.isAfter(protocolStartTime)) {
            return recoverAndGetDelayToNextEpoch();
        }
        return 0;
    }

    private long calculateDelayUntilProtocolStart() {
        AppLogger.logInfo("Waiting for protocol to start...");
        long delayNanos = ChronoUnit.NANOS.between(LocalDateTime.now(), protocolStartTime);
        return Math.max(delayNanos, 0);
    }

    private long recoverAndGetDelayToNextEpoch() {
        AppLogger.logInfo("(Re)joining protocol that has already started...");
        needsToRecover = true;

        int epochDurationSeconds = 2 * this.deltaInSeconds;
        long elapsedSeconds = ChronoUnit.SECONDS.between(protocolStartTime, LocalDateTime.now());

        long completedEpochsSeconds = elapsedSeconds - (elapsedSeconds % epochDurationSeconds);
        int completedEpochs = (int) (completedEpochsSeconds / epochDurationSeconds);
        currentEpoch.compareAndSet(0, completedEpochs + 1);

        LocalDateTime nextEpochStartTime = protocolStartTime.plusSeconds(completedEpochsSeconds + epochDurationSeconds);
        long delayToNextEpochNanos = ChronoUnit.NANOS.between(LocalDateTime.now(), nextEpochStartTime);
        return Math.max(delayToNextEpochNanos, 0);
    }

    private void requestMissingBlocksFromPeers(int fromEpoch, int toEpoch) {
        MissingEpochRange missingEpochRange = new MissingEpochRange(fromEpoch, toEpoch);
        Message joinRequest = new Message(MessageType.JOIN, missingEpochRange, localNodeId);
        urbNode.broadcastFromLocal(joinRequest);

        synchronizeEpochWithPeers(toEpoch);
    }

    private void synchronizeEpochWithPeers(int targetEpoch) {
        for (int epoch = 0; epoch < targetEpoch; epoch++) {
            determineEpochLeader(epoch);
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

        if (needsToRecover) {
            requestMissingBlocksFromPeers(blockchainManager.getLastNotarizedEpoch() + 1, epoch);
            needsToRecover = false;
        }

        if (epoch != 0 && epoch % BLOCKCHAIN_PRINT_EPOCH_INTERVAL == 0) {
            blockchainManager.printBiggestFinalizedChain();
        }

        if (epoch != 0 && epoch % BLOCKCHAIN_PERSISTENCE_INTERVAL == 0) {
            blockchainManager.persistToFile();
        }
        int epochLeader = determineEpochLeader(epoch);
        AppLogger.logInfo("#### EPOCH = " + epoch + " LEADER = " + epochLeader + " ####");

        if (localNodeId == epochLeader) {
            try {
                if (!isClientGeneratingTransactions || !pendingClientTransactions.isEmpty()) {
                    AppLogger.logDebug("Node " + localNodeId + " is leader: proposing new block");
                    proposeNewBlock(epoch);
                }
            } catch (NoSuchAlgorithmException e) {
                AppLogger.logError("Error proposing new block: " + e.getMessage(), e);
            }
        }

        currentEpoch.incrementAndGet();
    }

    private void processDeliveredMessages() {
        final Queue<Message> bufferedMessages = new LinkedList<>();
        try {
            while (true) {
                Message message = deliveredMessagesQueue.poll(100, TimeUnit.MILLISECONDS);
                if (message == null) continue;

                if (isInConfusionPhase(currentEpoch.get())) {
                    bufferedMessages.add(message);
                    continue;
                }

                while (!bufferedMessages.isEmpty()) {
                    processMessage(bufferedMessages.poll());
                }

                processMessage(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            AppLogger.logWarning("Message consumer thread interrupted");
        }
    }

    private void proposeNewBlock(int epoch) throws NoSuchAlgorithmException {
        Block parentBlock = blockchainManager.getBiggestNotarizedChain().getLast();
        Transaction[] transactions = collectBlockTransactions();

        Block newBlock = new Block(
                parentBlock.getSHA1(),
                epoch,
                parentBlock.length() + 1,
                transactions
        );
        AppLogger.logDebug("Proposed block: " + newBlock + " with transactions: " + Arrays.toString(transactions));
        urbNode.broadcastFromLocal(new Message(MessageType.PROPOSE, newBlock, localNodeId));
    }

    private Transaction[] collectBlockTransactions() {
        if (isClientGeneratingTransactions) {
            Transaction[] transactions = new Transaction[pendingClientTransactions.size()];
            int index = 0;
            while (!pendingClientTransactions.isEmpty()) {
                transactions[index++] = pendingClientTransactions.poll();
            }
            return transactions;
        } else {
            return transactionPoolSimulator.generateTransactions();
        }
    }

    private void processMessage(Message message) {
        AppLogger.logDebug("Processing message from " + message.sender() + ": " + message.type());
        switch (message.type()) {
            case JOIN -> handleJoinRequest(message);
            case PROPOSE -> handleProposalMessage(message);
            case VOTE -> handleVoteMessage(message);
            case UPDATE -> handleUpdateMessage(message);
        }
    }

    private void handleProposalMessage(Message message) {
        Block proposedBlock = (Block) message.content();
        SeenProposal proposal = new SeenProposal(message.sender(), proposedBlock.epoch());

        if (seenProposals.contains(proposal) || !blockchainManager.onPropose(proposedBlock)) {
            return;
        }
        seenProposals.add(proposal);

        Block blockHeader = new Block(
                proposedBlock.parentHash(),
                proposedBlock.epoch(),
                proposedBlock.length(),
                new Transaction[0]
        );
        urbNode.broadcastFromLocal(new Message(MessageType.VOTE, blockHeader, localNodeId));
        AppLogger.logDebug("Voted for block from leader " + message.sender() + " epoch " + proposedBlock.epoch());
    }

    private void handleVoteMessage(Message message) {
        Block block = (Block) message.content();
        blockVotes.computeIfAbsent(block, _ -> new HashSet<>()).add(message.sender());

        int totalVotes = blockVotes.get(block).size();

        if (totalVotes > numberOfNodes / 2) {
            blockchainManager.notarizeBlock(block);
        }
    }

    private void handleJoinRequest(Message message) {
        if (message.sender() == localNodeId) return;

        MissingEpochRange requestedRange = (MissingEpochRange) message.content();
        List<BlockNode> missingBlocks = blockchainManager.getBlocksInEpochRange(
                requestedRange.from(),
                requestedRange.to()
        );

        Message catchUpResponse = new Message(
                MessageType.UPDATE,
                new CatchUp(message.sender(), missingBlocks, currentEpoch.get()),
                localNodeId
        );
        urbNode.broadcastFromLocal(catchUpResponse);
    }

    private void handleUpdateMessage(Message message) {
        CatchUp catchUp = (CatchUp) message.content();
        if (catchUp.slackerId() != localNodeId) return;
        blockchainManager.insertMissingBlocks(catchUp.missingChain());
    }

    private boolean isInConfusionPhase(int epoch) {
        return epoch >= CONFUSION_EPOCH_START && epoch < CONFUSION_EPOCH_START + CONFUSION_EPOCH_DURATION;
    }

    private int determineEpochLeader(int epoch) {
        return isInConfusionPhase(epoch) ? epoch % numberOfNodes : epochLeaderRandomizer.nextInt(numberOfNodes);
    }

    private void acceptClientTransactions() {
        try (ServerSocket serverSocket = new ServerSocket(clientServerAddress.port())) {
            AppLogger.logInfo("Transaction server listening on port " + clientServerAddress.port());
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> handleClientConnection(clientSocket));
            }
        } catch (IOException e) {
            AppLogger.logError("Error in transaction server: " + e.getMessage(), e);
        }
    }

    private void handleClientConnection(Socket clientSocket) {
        AppLogger.logDebug("Handling client connection from " + clientSocket.getInetAddress() + "...");
        try (Socket s = clientSocket;
             ObjectInputStream ois = new ObjectInputStream(s.getInputStream())) {

            while (true) {
                try {
                    Transaction transaction = (Transaction) ois.readObject();
                    AppLogger.logInfo("Received transaction from client " + s.getInetAddress() + ": " + transaction);
                    pendingClientTransactions.add(transaction);
                } catch (ClassNotFoundException e) {
                    AppLogger.logError("Received unknown object from client " + s.getInetAddress(), e);
                }
            }

        } catch (IOException e) {
            AppLogger.logInfo("Client " + clientSocket.getInetAddress() + " disconnected.");
        }
    }
}