package protocol;

public class PingMessage extends Message {
    public PingMessage(String nodeId, String time) {
        this.type = MessageType.PING;
        this.nodeId = nodeId;
        this.time = time;
    }
    public PingMessage() {
        this.type = MessageType.PING;
    }
}
