package protocol;

public class PeerDisconnectedMessage extends Message {
    private String reason;

    public PeerDisconnectedMessage(String nodeId, String time, String reason) {
        this.type = MessageType.PEER_DISCONNECTED;
        this.nodeId = nodeId;
        this.time = time;
        this.reason = reason;
    }

    public PeerDisconnectedMessage() {
        this.type = MessageType.PEER_DISCONNECTED;
    }

    public String getReason() {
        return reason;
    }
}
