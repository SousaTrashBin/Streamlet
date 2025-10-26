package GroupCommunication;

import utils.application.Message;
import utils.communication.KeyType;
import utils.communication.MessageWithReceiver;
import utils.communication.PeerInfo;
import utils.logs.AppLogger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class P2PNode implements Runnable, AutoCloseable {
    private static final long RETRY_DELAY_MS = 2500;
    private final PeerInfo localPeerInfo;
    private final CountDownLatch allPeersConnectedLatch;
    private final Map<Integer, PeerInfo> peerInfoById;
    private final ConcurrentLinkedQueue<MessageWithReceiver> outgoingMessageQueue = new ConcurrentLinkedQueue<>();
    private final BlockingQueue<Message> incomingMessageQueue = new LinkedBlockingQueue<>();
    private final Selector ioSelector = Selector.open();
    private final Map<Integer, SocketChannel> peerConnections = new ConcurrentHashMap<>();
    private final Set<Integer> connectedPeers = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Long> peerConnectionBackoff = new ConcurrentHashMap<>();
    private ServerSocketChannel serverChannel;

    public P2PNode(PeerInfo localPeerInfo, List<PeerInfo> remotePeersInfo) throws IOException {
        this.localPeerInfo = localPeerInfo;
        this.peerInfoById = remotePeersInfo.stream()
                .collect(Collectors.toMap(PeerInfo::id, Function.identity()));
        this.allPeersConnectedLatch = new CountDownLatch(peerInfoById.size());
        initializeServerSocket();
    }

    private void initializeServerSocket() throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(localPeerInfo.address().port()));
        serverChannel.register(ioSelector, SelectionKey.OP_ACCEPT);
        AppLogger.logDebug("Server socket initialized on port " + localPeerInfo.address().port());
    }

    private void attemptConnectionToPeer(PeerInfo remotePeer) {
        peerConnections.computeIfPresent(remotePeer.id(), (_, existing) -> {
            try {
                if (existing.isConnected() || existing.isConnectionPending()) return existing;
                if (existing.isOpen()) {
                    existing.keyFor(ioSelector).cancel();
                    existing.close();
                    AppLogger.logDebug("Closed stale connection to peer " + remotePeer.id());
                }
            } catch (IOException e) {
                AppLogger.logError("Error closing stale connection to peer " + remotePeer.id() + ": " + e.getMessage(), null);
            }
            return null;
        });

        try {
            SocketChannel clientChannel = SocketChannel.open();
            clientChannel.configureBlocking(false);
            clientChannel.connect(new InetSocketAddress(remotePeer.address().ip(), remotePeer.address().port()));
            clientChannel.register(ioSelector, SelectionKey.OP_CONNECT, remotePeer);
            peerConnections.put(remotePeer.id(), clientChannel);
            AppLogger.logDebug("Attempting connection to peer " + remotePeer.id());
        } catch (IOException e) {
            AppLogger.logError("Failed to initiate connection to peer " + remotePeer.id() + ": " + e.getMessage(), null);
        }
    }

    @Override
    public void run() {
        try {
            runEventLoop();
        } catch (IOException | ClassNotFoundException e) {
            AppLogger.logError("Unexpected error in P2PNode event loop: " + e.getMessage(), e);
        }
    }

    private void runEventLoop() throws IOException, ClassNotFoundException {
        while (true) {
            attemptToConnectToPeers();
            ioSelector.select(1000);
            processOutgoingMessages();

            for (Iterator<SelectionKey> it = ioSelector.selectedKeys().iterator(); it.hasNext(); ) {
                SelectionKey key = it.next();
                it.remove();
                if (!key.isValid()) continue;
                handleKey(key);
            }
        }
    }

    private void handleKey(SelectionKey key) {
        try {
            switch (KeyType.fromSelectionKey(key)) {
                case CONNECT -> handleConnectComplete(key);
                case ACCEPT -> handleIncomingConnection(key);
                case READ -> handleIncomingMessage(key);
            }
        } catch (IOException e) {
            handleConnectionFailure(key, e);
        }
    }

    private void handleConnectionFailure(SelectionKey key, IOException e) {
        if (!(key.channel() instanceof SocketChannel channel)) {
            key.cancel();
            try {
                key.channel().close();
            } catch (IOException ex) {
                AppLogger.logWarning("Failed to close non-socket channel: " + ex.getMessage());
            }
            return;
        }

        Integer peerId = switch (key.attachment()) {
            case Integer id -> id;
            case PeerInfo peerInfo -> peerInfo.id();
            default -> findPeerIdByChannel(channel);
        };

        if (peerId != null) {
            peerConnections.remove(peerId, channel);
            connectedPeers.remove(peerId);
        }

        AppLogger.logError("Connection to peer " + peerId + " failed: " + e.getMessage(), null);
        try {
            key.cancel();
            channel.close();
        } catch (IOException ex) {
            AppLogger.logWarning("Error closing channel after failure with peer " + peerId + ": " + ex.getMessage());
        }
    }

    private Integer findPeerIdByChannel(SocketChannel channel) {
        return peerConnections.entrySet().stream()
                .filter(entry -> entry.getValue() == channel)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    private void attemptToConnectToPeers() {
        long now = System.currentTimeMillis();
        for (PeerInfo remotePeer : peerInfoById.values()) {
            if (shouldAttemptConnection(remotePeer.id(), now)) {
                peerConnectionBackoff.put(remotePeer.id(), now);
                attemptConnectionToPeer(remotePeer);
            }
        }
    }

    private boolean shouldAttemptConnection(Integer peerId, long now) {
        SocketChannel channel = peerConnections.get(peerId);
        boolean needsConnection = channel == null || !channel.isOpen() || !channel.isConnected();
        long lastAttempt = peerConnectionBackoff.getOrDefault(peerId, 0L);
        return needsConnection && now - lastAttempt > RETRY_DELAY_MS;
    }

    private void handleConnectComplete(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        PeerInfo remotePeer = (PeerInfo) key.attachment();

        try {
            clientChannel.finishConnect();
        } catch (IOException e) {
            key.cancel();
            clientChannel.close();
            peerConnections.remove(remotePeer.id(), clientChannel);
            AppLogger.logError("Failed to finish connection to peer " + remotePeer.id() + ": " + e.getMessage(), null);
            return;
        }

        SocketChannel existingConnection = peerConnections.get(remotePeer.id());

        if (existingConnection != null && existingConnection != clientChannel) {
            if (localPeerInfo.id() > remotePeer.id()) { // tie-break
                clientChannel.close();
                key.cancel();
                peerConnections.remove(remotePeer.id(), clientChannel);
                return;
            } else {
                existingConnection.keyFor(ioSelector).cancel();
                existingConnection.close();
            }
        }

        peerConnections.put(remotePeer.id(), clientChannel);

        ByteBuffer idBuffer = ByteBuffer.allocate(4).putInt(localPeerInfo.id());
        idBuffer.flip();
        clientChannel.write(idBuffer);

        if (connectedPeers.add(remotePeer.id())) {
            allPeersConnectedLatch.countDown();
            peerConnectionBackoff.remove(remotePeer.id());
        }

        AppLogger.logDebug(localPeerInfo.id() + " connected to peer " + remotePeer.id());
        clientChannel.register(ioSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, remotePeer.id());
    }


    private void handleIncomingConnection(SelectionKey key) throws IOException {
        SocketChannel incomingChannel = ((ServerSocketChannel) key.channel()).accept();
        incomingChannel.configureBlocking(false);

        ByteBuffer idBuffer = ByteBuffer.allocate(4);
        while (idBuffer.hasRemaining()) {
            int read = incomingChannel.read(idBuffer);
            if (read == -1) {
                incomingChannel.close();
                AppLogger.logWarning("Incoming connection closed before reading peer ID.");
                return;
            }
        }
        idBuffer.flip();
        int remotePeerId = idBuffer.getInt();

        SocketChannel existingConnection = peerConnections.get(remotePeerId);

        if (existingConnection != null && existingConnection != incomingChannel) {
            if (remotePeerId > localPeerInfo.id()) {
                incomingChannel.close(); // tie-break
                return;
            } else {
                existingConnection.keyFor(ioSelector).cancel();
                existingConnection.close();
            }
        }

        peerConnections.put(remotePeerId, incomingChannel);

        if (connectedPeers.add(remotePeerId)) {
            allPeersConnectedLatch.countDown();
            peerConnectionBackoff.remove(remotePeerId);
        }

        AppLogger.logDebug(localPeerInfo.id() + " accepted connection from peer " + remotePeerId);
        incomingChannel.register(ioSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, remotePeerId);
    }


    private void handleIncomingMessage(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            Optional.ofNullable(readMessageFromChannel(channel))
                    .ifPresent(incomingMessageQueue::add);
        } catch (IOException | ClassNotFoundException e) {
            AppLogger.logError("Error reading message from peer: " + e.getMessage(), null);
            handleConnectionFailure(key, new IOException("Failed reading message", e));
        }
    }

    private Message readMessageFromChannel(SocketChannel channel) throws IOException, ClassNotFoundException {
        int messageLength = readIntFromChannel(channel);

        ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);
        readFully(channel, messageBuffer);

        return Message.fromBytes(messageBuffer.array());
    }

    private int readIntFromChannel(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        readFully(channel, buffer);
        buffer.flip();
        return buffer.getInt();
    }

    private void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) throw new IOException("Connection closed while reading data");
        }
    }

    public void enqueueOutgoingMessage(MessageWithReceiver messageWithReceiver) {
        outgoingMessageQueue.add(messageWithReceiver);
        ioSelector.wakeup();
    }

    public Message receiveMessage() throws InterruptedException {
        return incomingMessageQueue.take();
    }

    public void enqueueIncomingMessage(Message message) {
        incomingMessageQueue.add(message);
    }

    public void waitForAllPeersConnected() throws InterruptedException {
        allPeersConnectedLatch.await();
    }

    private void processOutgoingMessages() {
        MessageWithReceiver messageWithReceiver;
        while ((messageWithReceiver = outgoingMessageQueue.poll()) != null) {
            sendMessageToPeer(messageWithReceiver.receiverId(), messageWithReceiver.message());
        }
    }

    private void sendMessageToPeer(Integer receiverId, Message message) {
        if (Objects.equals(receiverId, localPeerInfo.id())) return;

        SocketChannel peerChannel = peerConnections.get(receiverId);
        if (peerChannel == null || !peerChannel.isConnected()) {
            AppLogger.logWarning("Cannot send message; peer " + receiverId + " is not connected.");
            return;
        }

        try {
            byte[] messageBytes = message.toBytes();
            ByteBuffer buffer = ByteBuffer.allocate(4 + messageBytes.length)
                    .putInt(messageBytes.length)
                    .put(messageBytes);
            buffer.flip();
            writeFully(peerChannel, buffer);
        } catch (IOException e) {
            AppLogger.logError("Failed to send message to peer " + receiverId + ": " + e.getMessage(), null);
        }
    }

    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) == -1) {
                throw new IOException("Connection closed while writing data");
            }
        }
    }

    @Override
    public void close() {
        try {
            serverChannel.close();
        } catch (IOException e) {
            AppLogger.logError("Error closing server channel for peer " + localPeerInfo.id() + ": " + e.getMessage(), null);
        }

        for (SocketChannel channel : peerConnections.values()) {
            try {
                channel.close();
            } catch (IOException e) {
                AppLogger.logError("Error closing peer channel for peer " + localPeerInfo.id() + ": " + e.getMessage(), null);
            }
        }

        try {
            ioSelector.close();
            AppLogger.logDebug("P2PNode resources closed for peer " + localPeerInfo.id());
        } catch (IOException e) {
            AppLogger.logError("Error closing selector for peer " + localPeerInfo.id() + ": " + e.getMessage(), null);
        }
    }

}
