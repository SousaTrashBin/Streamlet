package application;

import urb.URBNode;
import utils.application.Block;
import utils.application.Message;
import utils.application.MessageType;
import utils.application.Transaction;
import utils.communication.Address;
import utils.communication.PeerInfo;
import utils.logs.AppLogger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

record SeenProposal(int leader, int epoch) {
}

public class StreamletNode {
    private static final int CONFUSION_START = 20;
    private static final int CONFUSION_DURATION = 12;

    private final int localId;
    private final int deltaInSeconds;
    private final int numberOfDistinctNodes;

    private final URBNode urbNode;
    private final Random random = new Random(1L);
    private final BlockchainManager blockchainManager;
    private final TransactionPoolSimulator transactionPoolSimulator;
    private final AtomicInteger currentEpoch = new AtomicInteger(0);

    private final Map<Block, Set<Integer>> votedBlocks = new HashMap<>();
    private final BlockingQueue<Message> derivableQueue = new LinkedBlockingQueue<>(1000);
    private final Set<SeenProposal> seenProposals = new HashSet<>();
    private final ConcurrentLinkedQueue<Transaction> clientPendingTransactionsQueue = new ConcurrentLinkedQueue<>();

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Address myClientAddress;

    public StreamletNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo, int deltaInSeconds, Address myClientAddress)
            throws IOException {
        localId = localPeerInfo.id();
        numberOfDistinctNodes = 1 + remotePeersInfo.size();
        this.deltaInSeconds = deltaInSeconds;
        transactionPoolSimulator = new TransactionPoolSimulator(numberOfDistinctNodes);
        blockchainManager = new BlockchainManager();
        urbNode = new URBNode(localPeerInfo, remotePeersInfo, derivableQueue::add);
        this.myClientAddress = myClientAddress;
    }

    public void startProtocol() throws InterruptedException {
        launchThreads();
        urbNode.waitForAllPeersToConnect();

        long epochDuration = 2L * deltaInSeconds;
        scheduler.scheduleAtFixedRate(this::safeAdvanceEpoch, epochDuration, epochDuration, TimeUnit.SECONDS);
    }

    private void safeAdvanceEpoch() {
        try {
            advanceEpoch();
        } catch (Exception e) {
            AppLogger.logError("Error advancing epoch: " + e.getMessage(), e);
        }
    }

    private void advanceEpoch() {
        int epoch = currentEpoch.incrementAndGet();
        int currentLeaderId = calculateLeaderId(epoch);
        AppLogger.logInfo("#### EPOCH = " + epoch + " LEADER= " + currentLeaderId + " ####");

        if (localId == currentLeaderId) {
            try {
                AppLogger.logDebug("Node " + localId + " is leader: proposing new block");
                proposeNewBlock(epoch);
            } catch (NoSuchAlgorithmException e) {
                AppLogger.logError("Error proposing new block: " + e.getMessage(), e);
            }
        }

        if (epoch % 5 == 0) blockchainManager.printBlockchainTree();
    }

    private void launchThreads() {
        executor.submit(() -> {
            try {
                urbNode.startURBNode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                AppLogger.logWarning("URBNode thread interrupted");
            }
        });
        executor.submit(this::consumeMessages);
        executor.submit(this::receiveClientTransactionsRequests);
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

                while (!bufferedMessages.isEmpty()) handleMessageDelivery(bufferedMessages.poll());
                handleMessageDelivery(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            AppLogger.logWarning("Message consumer thread interrupted");
        }
    }

    private void proposeNewBlock(int epoch) throws NoSuchAlgorithmException {
        Optional<Block> parentOpt = blockchainManager.getNotarizedTips().stream()
                .max(Comparator.comparingInt(Block::length).thenComparing(Block::epoch));

        if (parentOpt.isEmpty()) return;

        Block parent = parentOpt.get();
        Transaction[] transactions;

        if (!clientPendingTransactionsQueue.isEmpty()) {
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
        }
    }

    private void handlePropose(Message message) {
        Block block = (Block) message.content();
        SeenProposal proposal = new SeenProposal(message.sender(), block.epoch());

        if (seenProposals.contains(proposal) || !blockchainManager.extendNotarizedAnyChainTip(block))
            return;

        seenProposals.add(proposal);
        blockchainManager.addPendingBlock(block);

        Block voteBlock = new Block(block.parentHash(), block.epoch(), block.length(), new Transaction[0]);
        urbNode.broadcastFromLocal(new Message(MessageType.VOTE, voteBlock, localId));
        AppLogger.logDebug("Voted for block from leader " + message.sender() + " epoch " + block.epoch());
    }

    private void handleVote(Message message) {
        Block block = (Block) message.content();
        votedBlocks.computeIfAbsent(block, _ -> new HashSet<>()).add(message.sender());

        if (blockchainManager.extendNotarizedAnyChainTip(block)
                && votedBlocks.get(block).size() > numberOfDistinctNodes / 2) {

            blockchainManager.notarizeBlock(block);
            AppLogger.logInfo("Block notarized: epoch " + block.epoch() + " length " + block.length());
        }
    }

    private boolean inConfusionEpoch(int epoch) {
        return epoch >= CONFUSION_START && epoch < CONFUSION_START + CONFUSION_DURATION + 1;
    }

    private int calculateLeaderId(int epoch) {
        return inConfusionEpoch(epoch) ? epoch % numberOfDistinctNodes : random.nextInt(numberOfDistinctNodes);
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
