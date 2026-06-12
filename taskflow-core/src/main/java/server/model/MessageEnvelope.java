package server.model;

import protocol.Message;
import transport.TransportAcknowledgement;

public record MessageEnvelope(Message message, String fromNodeId, TransportAcknowledgement acknowledgement) {
    public MessageEnvelope(Message message, String fromNodeId) {
        this(message, fromNodeId, null);
    }
}
