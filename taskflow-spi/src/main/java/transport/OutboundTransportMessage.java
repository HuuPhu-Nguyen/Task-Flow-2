package transport;

import protocol.Message;

public record OutboundTransportMessage(
        TransportRoute route,
        String fromNodeId,
        Message message
) {
    public OutboundTransportMessage {
        if (route == null) {
            throw new IllegalArgumentException("route is required");
        }
        if (fromNodeId == null || fromNodeId.isBlank()) {
            throw new IllegalArgumentException("fromNodeId is required");
        }
        if (message == null) {
            throw new IllegalArgumentException("message is required");
        }
    }
}
