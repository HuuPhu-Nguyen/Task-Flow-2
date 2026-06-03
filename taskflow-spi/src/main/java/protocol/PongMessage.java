package protocol;

public class PongMessage extends Message {
    public PongMessage(String nodeId, String time) {
        this.type = MessageType.PONG;
        this.nodeId = nodeId;
        this.time = time;
    }
    public PongMessage(){
        this.type = MessageType.PONG;
    }
}