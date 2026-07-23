package transport.rabbitmq;

import transport.TransportRoute;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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

    public boolean deadLetterEnabled() {
        return config.deadLetterEnabled();
    }

    public String deadLetterExchangeName() {
        return config.deadLetterExchangeName();
    }

    public String deadLetterQueueName() {
        return config.deadLetterQueueName();
    }

    public String deadLetterRoutingKey() {
        return config.deadLetterRoutingKey();
    }

    public String deadLetterQuarantineQueueName() {
        if (config.deadLetterEnabled()) {
            return config.deadLetterQueueName() + ".quarantine";
        }
        return config.queuePrefix() + ".quarantine";
    }

    public String deadLetterQuarantineRoutingKey() {
        if (config.deadLetterEnabled()) {
            return config.deadLetterRoutingKey() + ".quarantine";
        }
        return "quarantine";
    }

    public String quarantineExchangeName() {
        if (config.deadLetterEnabled()) {
            return config.deadLetterExchangeName();
        }
        return config.exchangeName() + ".quarantine";
    }

    public List<Long> retryDelaysMillis() {
        return config.retryDelaysMillis();
    }

    public int maxDeliveryAttempts() {
        return config.maxDeliveryAttempts();
    }

    public int retryStageCount() {
        return config.retryDelaysMillis().size();
    }

    public String retryExchangeName(int retryStage) {
        long delayMillis = retryDelayMillis(retryStage);
        return config.exchangeName() + ".retry." + retryStage + "." + delayMillis + "ms";
    }

    public String retryQueueName(int retryStage) {
        long delayMillis = retryDelayMillis(retryStage);
        return config.queuePrefix() + ".retry." + retryStage + "." + delayMillis + "ms";
    }

    public Map<String, Object> retryQueueArguments(int retryStage) {
        Map<String, Object> arguments = new LinkedHashMap<>();
        arguments.put("x-message-ttl", retryDelayMillis(retryStage));
        arguments.put("x-dead-letter-exchange", config.exchangeName());
        return Map.copyOf(arguments);
    }

    public Map<String, Object> queueArguments() {
        if (!config.deadLetterEnabled()) {
            return Map.of();
        }
        Map<String, Object> arguments = new LinkedHashMap<>();
        arguments.put("x-dead-letter-exchange", config.deadLetterExchangeName());
        arguments.put("x-dead-letter-routing-key", config.deadLetterRoutingKey());
        return Map.copyOf(arguments);
    }

    public String queueName(TransportRoute route) {
        return queueNames.get(route);
    }

    public Map<TransportRoute, String> queueNames() {
        return Map.copyOf(queueNames);
    }

    public String peerQueueName(TransportRoute route, String peerNodeId) {
        return config.queuePrefix()
                + ".peer."
                + peerSegment(peerNodeId)
                + "."
                + routeToken(route);
    }

    public String peerRoutingKey(TransportRoute route, String peerNodeId) {
        return route.routingKey() + "." + peerSegment(peerNodeId);
    }

    public TransportRoute routeForRoutingKey(String routingKey) {
        if (routingKey == null || routingKey.isBlank()) {
            return null;
        }
        String candidate = routingKey.trim();
        for (TransportRoute route : TransportRoute.values()) {
            String normalRoutingKey = route.routingKey();
            if (normalRoutingKey.equals(candidate) || candidate.startsWith(normalRoutingKey + ".")) {
                return route;
            }
        }
        return null;
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

    private String peerSegment(String peerNodeId) {
        if (peerNodeId == null || peerNodeId.isBlank()) {
            throw new IllegalArgumentException("peerNodeId is required");
        }
        return peerNodeId.trim().replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private String routeToken(TransportRoute route) {
        if (route == null) {
            throw new IllegalArgumentException("route is required");
        }
        return route.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    private long retryDelayMillis(int retryStage) {
        if (retryStage <= 0 || retryStage > retryStageCount()) {
            throw new IllegalArgumentException(
                    "retryStage must be between 1 and " + retryStageCount()
            );
        }
        return config.retryDelaysMillis().get(retryStage - 1);
    }
}
