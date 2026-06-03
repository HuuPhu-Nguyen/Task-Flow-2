package transport;

@FunctionalInterface
public interface TransportMessageHandler {
    void handle(InboundTransportMessage delivery) throws Exception;
}
