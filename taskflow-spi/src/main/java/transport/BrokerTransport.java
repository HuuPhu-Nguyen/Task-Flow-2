package transport;

public interface BrokerTransport extends AutoCloseable {
    void declareTopology() throws Exception;

    void publish(OutboundTransportMessage message) throws Exception;

    String subscribe(TransportRoute route, TransportMessageHandler handler) throws Exception;

    void cancel(String consumerTag) throws Exception;

    @Override
    void close() throws Exception;
}
