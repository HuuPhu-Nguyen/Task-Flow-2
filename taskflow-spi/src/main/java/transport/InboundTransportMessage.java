package transport;

import protocol.Message;

public record InboundTransportMessage(
        TransportRoute route,
        String fromNodeId,
        Message message,
        TransportAcknowledgement acknowledgement
) {}
