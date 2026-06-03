package server.model;

import protocol.Message;

public record MessageEnvelope(Message message, String fromNodeId) {}