package transport;

public interface TransportAcknowledgement {
    void ack() throws Exception;

    void requeue() throws Exception;

    void reject() throws Exception;
}
