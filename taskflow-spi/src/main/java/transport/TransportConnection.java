package transport;

import protocol.Message;

public interface TransportConnection extends AutoCloseable {
    String nodeId();

    boolean isOpen();

    void send(Message message);

    @Override
    void close();
}
