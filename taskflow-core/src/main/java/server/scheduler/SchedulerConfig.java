package server.scheduler;

import java.util.Map;

public record SchedulerConfig(
        long taskTimeoutMillis,
        int maxTasksPerPeer,
        int maxTaskRetries,
        long metricsLogIntervalMillis,
        double peerScoreLoadWeight,
        double peerScoreLatencyWeight,
        double peerScoreDurationWeight,
        double peerScoreFailureWeight,
        double peerScoreLatencyBaselineMillis,
        double peerScoreDurationBaselineMillis,
        double peerScoreEwmaAlpha
) {
    public static final long DEFAULT_TASK_TIMEOUT_MILLIS = 60_000L;
    public static final int DEFAULT_MAX_TASKS_PER_PEER = 3;
    public static final int DEFAULT_MAX_TASK_RETRIES = 20;
    public static final long DEFAULT_METRICS_LOG_INTERVAL_MILLIS = 10_000L;
    public static final double DEFAULT_PEER_SCORE_LOAD_WEIGHT = 6.0;
    public static final double DEFAULT_PEER_SCORE_LATENCY_WEIGHT = 2.0;
    public static final double DEFAULT_PEER_SCORE_DURATION_WEIGHT = 1.5;
    public static final double DEFAULT_PEER_SCORE_FAILURE_WEIGHT = 4.0;
    public static final double DEFAULT_PEER_SCORE_LATENCY_BASELINE_MILLIS = 200.0;
    public static final double DEFAULT_PEER_SCORE_DURATION_BASELINE_MILLIS = 5_000.0;
    public static final double DEFAULT_PEER_SCORE_EWMA_ALPHA = 0.2;

    public SchedulerConfig {
        requirePositive(taskTimeoutMillis, "taskTimeoutMillis");
        requirePositive(maxTasksPerPeer, "maxTasksPerPeer");
        requirePositive(maxTaskRetries, "maxTaskRetries");
        requirePositive(metricsLogIntervalMillis, "metricsLogIntervalMillis");
        requireNonNegative(peerScoreLoadWeight, "peerScoreLoadWeight");
        requireNonNegative(peerScoreLatencyWeight, "peerScoreLatencyWeight");
        requireNonNegative(peerScoreDurationWeight, "peerScoreDurationWeight");
        requireNonNegative(peerScoreFailureWeight, "peerScoreFailureWeight");
        requirePositive(peerScoreLatencyBaselineMillis, "peerScoreLatencyBaselineMillis");
        requirePositive(peerScoreDurationBaselineMillis, "peerScoreDurationBaselineMillis");
        if (peerScoreEwmaAlpha <= 0.0 || peerScoreEwmaAlpha > 1.0) {
            throw new IllegalArgumentException("peerScoreEwmaAlpha must be in (0, 1].");
        }
    }

    public static SchedulerConfig defaults() {
        return new SchedulerConfig(
                DEFAULT_TASK_TIMEOUT_MILLIS,
                DEFAULT_MAX_TASKS_PER_PEER,
                DEFAULT_MAX_TASK_RETRIES,
                DEFAULT_METRICS_LOG_INTERVAL_MILLIS,
                DEFAULT_PEER_SCORE_LOAD_WEIGHT,
                DEFAULT_PEER_SCORE_LATENCY_WEIGHT,
                DEFAULT_PEER_SCORE_DURATION_WEIGHT,
                DEFAULT_PEER_SCORE_FAILURE_WEIGHT,
                DEFAULT_PEER_SCORE_LATENCY_BASELINE_MILLIS,
                DEFAULT_PEER_SCORE_DURATION_BASELINE_MILLIS,
                DEFAULT_PEER_SCORE_EWMA_ALPHA
        );
    }

    public static SchedulerConfig fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    public static SchedulerConfig fromEnvironment(Map<String, String> env) {
        SchedulerConfig defaults = defaults();
        return new SchedulerConfig(
                longValue(env, "TASKFLOW_TASK_TIMEOUT_MS", defaults.taskTimeoutMillis()),
                intValue(env, "TASKFLOW_MAX_TASKS_PER_PEER", defaults.maxTasksPerPeer()),
                intValue(env, "TASKFLOW_MAX_TASK_RETRIES", defaults.maxTaskRetries()),
                longValue(env, "TASKFLOW_METRICS_LOG_INTERVAL_MS", defaults.metricsLogIntervalMillis()),
                doubleValue(env, "TASKFLOW_SCORE_LOAD_WEIGHT", defaults.peerScoreLoadWeight()),
                doubleValue(env, "TASKFLOW_SCORE_LATENCY_WEIGHT", defaults.peerScoreLatencyWeight()),
                doubleValue(env, "TASKFLOW_SCORE_DURATION_WEIGHT", defaults.peerScoreDurationWeight()),
                doubleValue(env, "TASKFLOW_SCORE_FAILURE_WEIGHT", defaults.peerScoreFailureWeight()),
                doubleValue(env, "TASKFLOW_SCORE_LATENCY_BASELINE_MS", defaults.peerScoreLatencyBaselineMillis()),
                doubleValue(env, "TASKFLOW_SCORE_DURATION_BASELINE_MS", defaults.peerScoreDurationBaselineMillis()),
                doubleValue(env, "TASKFLOW_SCORE_EWMA_ALPHA", defaults.peerScoreEwmaAlpha())
        );
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

    private static void requirePositive(long value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be positive.");
        }
    }

    private static void requirePositive(double value, String field) {
        if (value <= 0.0) {
            throw new IllegalArgumentException(field + " must be positive.");
        }
    }

    private static void requireNonNegative(double value, String field) {
        if (value < 0.0) {
            throw new IllegalArgumentException(field + " must be non-negative.");
        }
    }
}
