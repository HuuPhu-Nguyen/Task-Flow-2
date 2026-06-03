package transport.rabbitmq;

import transport.TransportRoute;

import java.util.EnumMap;
import java.util.Map;

public class RabbitMqTopology {
    private final RabbitMqTransportConfig config;
    private final Map<TransportRoute, String> queueNames;

    public RabbitMqTopology(RabbitMqTransportConfig config) {
        this.config = config;
        this.queueNames = buildQueueNames(config.queuePrefix());
    }

    public String exchangeName() {
        return config.exchangeName();
    }

    public boolean durable() {
        return config.durable();
    }

    public String queueName(TransportRoute route) {
        return queueNames.get(route);
    }

    public Map<TransportRoute, String> queueNames() {
        return Map.copyOf(queueNames);
    }

    private Map<TransportRoute, String> buildQueueNames(String prefix) {
        Map<TransportRoute, String> names = new EnumMap<>(TransportRoute.class);
        names.put(TransportRoute.JOB_SUBMIT, prefix + ".jobs");
        names.put(TransportRoute.TASK_ASSIGN, prefix + ".tasks");
        names.put(TransportRoute.TASK_RESULT, prefix + ".task-results");
        names.put(TransportRoute.JOB_RESULT, prefix + ".job-results");
        names.put(TransportRoute.HEARTBEAT, prefix + ".heartbeats");
        return names;
    }
}
