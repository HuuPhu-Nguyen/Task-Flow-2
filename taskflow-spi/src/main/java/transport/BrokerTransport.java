package transport;

public interface BrokerTransport extends AutoCloseable {
    void declareTopology() throws Exception;

    void publish(OutboundTransportMessage message) throws Exception;

    default void publishToPeer(TransportRoute route,
                               String peerNodeId,
                               OutboundTransportMessage message) throws Exception {
        publish(message);
    }

    String subscribe(TransportRoute route, TransportMessageHandler handler) throws Exception;

    default String subscribePeer(TransportRoute route,
                                 String peerNodeId,
                                 TransportMessageHandler handler) throws Exception {
        return subscribe(route, handler);
    }

    void cancel(String consumerTag) throws Exception;

    @Override
    void close() throws Exception;
}
