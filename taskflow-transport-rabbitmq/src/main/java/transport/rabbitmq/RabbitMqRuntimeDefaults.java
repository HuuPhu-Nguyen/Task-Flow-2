package transport.rabbitmq;

public final class RabbitMqRuntimeDefaults {
    public static final String COORDINATOR_NODE_ID = "RABBITMQ_COORDINATOR";
    public static final String PEER_ID_ENV = "TASKFLOW_PEER_ID";
    public static final String PEER_ID_PREFIX = "RABBITMQ_PEER";

    private RabbitMqRuntimeDefaults() {
    }
}
