package transport.rabbitmq;

import java.util.Map;

public record RabbitMqTransportConfig(
        String host,
        int port,
        String username,
        String password,
        String virtualHost,
        String exchangeName,
        String queuePrefix,
        boolean durable,
        int prefetchCount
) {
    public static final int DEFAULT_PORT = 5672;

    public RabbitMqTransportConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username is required");
        }
        if (password == null) {
            throw new IllegalArgumentException("password is required");
        }
        if (virtualHost == null || virtualHost.isBlank()) {
            throw new IllegalArgumentException("virtualHost is required");
        }
        if (exchangeName == null || exchangeName.isBlank()) {
            throw new IllegalArgumentException("exchangeName is required");
        }
        if (queuePrefix == null || queuePrefix.isBlank()) {
            throw new IllegalArgumentException("queuePrefix is required");
        }
        if (prefetchCount <= 0) {
            throw new IllegalArgumentException("prefetchCount must be positive");
        }
    }

    public static RabbitMqTransportConfig localDefaults() {
        return new RabbitMqTransportConfig(
                "localhost",
                DEFAULT_PORT,
                "guest",
                "guest",
                "/",
                "taskflow.exchange",
                "taskflow",
                true,
                3
        );
    }

    public static RabbitMqTransportConfig fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    public static RabbitMqTransportConfig fromEnvironment(Map<String, String> env) {
        RabbitMqTransportConfig defaults = localDefaults();
        return new RabbitMqTransportConfig(
                value(env, "TASKFLOW_RABBITMQ_HOST", defaults.host()),
                intValue(env, "TASKFLOW_RABBITMQ_PORT", defaults.port()),
                value(env, "TASKFLOW_RABBITMQ_USERNAME", defaults.username()),
                value(env, "TASKFLOW_RABBITMQ_PASSWORD", defaults.password()),
                value(env, "TASKFLOW_RABBITMQ_VHOST", defaults.virtualHost()),
                value(env, "TASKFLOW_RABBITMQ_EXCHANGE", defaults.exchangeName()),
                value(env, "TASKFLOW_RABBITMQ_QUEUE_PREFIX", defaults.queuePrefix()),
                booleanValue(env, "TASKFLOW_RABBITMQ_DURABLE", defaults.durable()),
                intValue(env, "TASKFLOW_RABBITMQ_PREFETCH", defaults.prefetchCount())
        );
    }

    private static String value(Map<String, String> env, String key, String fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int intValue(Map<String, String> env, String key, int fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value);
    }

    private static boolean booleanValue(Map<String, String> env, String key, boolean fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : Boolean.parseBoolean(value);
    }
}
