package transport.rabbitmq;

import protocol.PeerIdentity;

public final class RabbitMqRuntimeDefaults {
    public static final String COORDINATOR_NODE_ID = "RABBITMQ_COORDINATOR";
    public static final String PEER_ID_ENV = PeerIdentity.PEER_ID_ENV;
    public static final String PEER_ID_PREFIX = "RABBITMQ_PEER";

    private RabbitMqRuntimeDefaults() {
    }
}
