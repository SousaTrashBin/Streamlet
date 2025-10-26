package URB;

import GroupCommunication.P2PNode;
import utils.application.Message;
import utils.application.MessageType;
import utils.communication.MessageWithReceiver;
import utils.communication.PeerInfo;
import utils.logs.AppLogger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class URBNode {
    private final Set<Message> deliveredMessages = new HashSet<>();
    private final List<Integer> remotePeerIds;
    private final P2PNode networkLayer;
    private final int localPeerId;
    private final URBCallback callback;
    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    public URBNode(PeerInfo localPeerInfo,
                   List<PeerInfo> remotePeersInfo,
                   URBCallback callback) throws IOException {
        localPeerId = localPeerInfo.id();
        remotePeerIds = remotePeersInfo.stream().map(PeerInfo::id).toList();
        networkLayer = new P2PNode(localPeerInfo, remotePeersInfo);
        executor.submit(networkLayer);
        this.callback = callback;
    }

    public void waitForAllPeersToConnect() throws InterruptedException {
        networkLayer.waitForAllPeersConnected();
    }

    public void startURBNode() throws InterruptedException {
        waitForAllPeersToConnect();
        AppLogger.logInfo("P2PNode " + localPeerId + " is ready");
        executor.submit(this::processIncomingMessages);
    }

    public void broadcastToPeers(Message message) {
        remotePeerIds.stream()
                .filter(peerId -> !Objects.equals(peerId, message.sender()))
                .map(peerId -> new MessageWithReceiver(peerId, message))
                .forEach(networkLayer::enqueueOutgoingMessage);
    }

    private void processIncomingMessages() {
        while (true) {
            try {
                Message receivedMessage = networkLayer.receiveMessage();
                deliverMessage(receivedMessage);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void deliverMessage(Message message) {
        if (!deliveredMessages.add(message)) {
            return;
        }
        switch (message.type()) {
            case ECHO -> {
                if (message.content() instanceof Message contentMessage) {
                    broadcastFromLocal(contentMessage);
                }
            }
            case PROPOSE, VOTE -> {
                Message echoMessage = new Message(MessageType.ECHO, message, localPeerId);
                broadcastToPeers(echoMessage);
                deliverToApplication(message);
            }
        }
    }

    private void deliverToApplication(Message message) {
        callback.onDelivery(message);
    }

    public void broadcastFromLocal(Message message) {
        networkLayer.enqueueIncomingMessage(message);
    }
}