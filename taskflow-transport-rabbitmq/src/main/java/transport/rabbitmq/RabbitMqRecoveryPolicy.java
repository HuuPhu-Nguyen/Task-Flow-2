package transport.rabbitmq;

import java.util.Map;

/**
 * Bounded delay and per-attempt timeout policy shared by initial RabbitMQ
 * connection attempts and automatic post-connect recovery.
 */
public record RabbitMqRecoveryPolicy(
        int connectionTimeoutMillis,
        long initialRetryDelayMillis,
        long maxRetryDelayMillis,
        double backoffMultiplier
) {
    public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 5_000;
    public static final long DEFAULT_INITIAL_RETRY_DELAY_MILLIS = 1_000L;
    public static final long DEFAULT_MAX_RETRY_DELAY_MILLIS = 30_000L;
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0D;

    public RabbitMqRecoveryPolicy {
        if (connectionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectionTimeoutMillis must be positive");
        }
        if (initialRetryDelayMillis <= 0L) {
            throw new IllegalArgumentException("initialRetryDelayMillis must be positive");
        }
        if (maxRetryDelayMillis < initialRetryDelayMillis) {
            throw new IllegalArgumentException(
                    "maxRetryDelayMillis must be at least initialRetryDelayMillis"
            );
        }
        if (!Double.isFinite(backoffMultiplier) || backoffMultiplier < 1.0D) {
            throw new IllegalArgumentException("backoffMultiplier must be finite and at least 1.0");
        }
    }

    public static RabbitMqRecoveryPolicy defaults() {
        return new RabbitMqRecoveryPolicy(
                DEFAULT_CONNECTION_TIMEOUT_MILLIS,
                DEFAULT_INITIAL_RETRY_DELAY_MILLIS,
                DEFAULT_MAX_RETRY_DELAY_MILLIS,
                DEFAULT_BACKOFF_MULTIPLIER
        );
    }

    public static RabbitMqRecoveryPolicy fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    public static RabbitMqRecoveryPolicy fromEnvironment(Map<String, String> env) {
        RabbitMqRecoveryPolicy defaults = defaults();
        return new RabbitMqRecoveryPolicy(
                intValue(
                        env,
                        "TASKFLOW_RABBITMQ_CONNECTION_TIMEOUT_MS",
                        defaults.connectionTimeoutMillis()
                ),
                longValue(
                        env,
                        "TASKFLOW_RABBITMQ_RECOVERY_INITIAL_DELAY_MS",
                        defaults.initialRetryDelayMillis()
                ),
                longValue(
                        env,
                        "TASKFLOW_RABBITMQ_RECOVERY_MAX_DELAY_MS",
                        defaults.maxRetryDelayMillis()
                ),
                doubleValue(
                        env,
                        "TASKFLOW_RABBITMQ_RECOVERY_BACKOFF_MULTIPLIER",
                        defaults.backoffMultiplier()
                )
        );
    }

    /**
     * Returns the capped delay after the supplied one-based failed-attempt
     * count. The calculation saturates instead of overflowing.
     */
    public long retryDelayMillis(int failedAttempts) {
        if (failedAttempts <= 0) {
            throw new IllegalArgumentException("failedAttempts must be positive");
        }
        double scaled = initialRetryDelayMillis
                * Math.pow(backoffMultiplier, failedAttempts - 1.0D);
        if (!Double.isFinite(scaled) || scaled >= maxRetryDelayMillis) {
            return maxRetryDelayMillis;
        }
        return Math.max(initialRetryDelayMillis, (long) Math.ceil(scaled));
    }

    private static int intValue(Map<String, String> env, String key, int fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value);
    }

    private static long longValue(Map<String, String> env, String key, long fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value);
    }

    private static double doubleValue(Map<String, String> env, String key, double fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : Double.parseDouble(value);
    }
}
