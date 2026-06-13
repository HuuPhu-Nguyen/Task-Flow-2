package transport;

import protocol.Message;

public interface TransportConnection extends AutoCloseable {
    String nodeId();

    boolean isOpen();

    boolean send(Message message);

    @Override
    void close();
}
