import StreamletApp.StreamletNode;
import utils.ConfigParser;
import utils.communication.PeerInfo;
import utils.logs.AppLogger;


void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 1) {
        System.out.println("Usage: Streamlet <nodeId>");
        return;
    }

    int nodeId = Integer.parseInt(args[0]);

    ConfigParser.ConfigData configData = ConfigParser.parseConfig();

    List<PeerInfo> peerInfos = configData.peers;
    AppLogger.updateLoggerLevel(configData.logLevel);
    PeerInfo localPeer = peerInfos.get(nodeId);

    List<PeerInfo> remotePeers = peerInfos.stream().filter(p -> p.id() != nodeId).toList();
    AppLogger.logDebug(remotePeers.toString());

    AppLogger.logInfo("Waiting all peers to connect...");
    StreamletNode node = new StreamletNode(localPeer, remotePeers, 1, configData.isClientGeneratingTransactions, configData.servers.get(nodeId));
    node.startProtocol();
}
