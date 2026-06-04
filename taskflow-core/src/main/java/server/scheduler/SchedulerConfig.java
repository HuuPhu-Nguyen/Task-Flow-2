package server.scheduler;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
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
    public static final String CONFIG_PATH_ENV = "TASKFLOW_CONFIG";
    public static final Path DEFAULT_CONFIG_PATH = Path.of("config", "taskflow.yml");

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

    public static SchedulerConfig fromRuntime() {
        return fromRuntime(System.getenv());
    }

    public static SchedulerConfig fromRuntime(Map<String, String> env) {
        Path configuredPath = configuredPath(env);
        Map<String, Object> fileConfig = Map.of();
        if (configuredPath != null) {
            boolean explicitPath = hasValue(env, CONFIG_PATH_ENV);
            if (Files.exists(configuredPath)) {
                fileConfig = readConfigFile(configuredPath);
            } else if (explicitPath) {
                throw new IllegalArgumentException("Configured TaskFlow config file does not exist: " + configuredPath);
            }
        }
        return fromSources(fileConfig, env);
    }

    public static SchedulerConfig fromFile(Path path) {
        return fromSources(readConfigFile(path), Map.of());
    }

    public static SchedulerConfig fromEnvironment(Map<String, String> env) {
        return fromSources(Map.of(), env);
    }

    private static SchedulerConfig fromSources(Map<String, Object> fileConfig, Map<String, String> env) {
        SchedulerConfig defaults = defaults();
        Map<String, Object> scheduler = childMap(fileConfig, "scheduler");
        Map<String, Object> scoring = childMap(scheduler, "scoring");
        return new SchedulerConfig(
                longValue(scheduler, env, "taskTimeoutMs", "TASKFLOW_TASK_TIMEOUT_MS", defaults.taskTimeoutMillis()),
                intValue(scheduler, env, "maxTasksPerPeer", "TASKFLOW_MAX_TASKS_PER_PEER", defaults.maxTasksPerPeer()),
                intValue(scheduler, env, "maxTaskRetries", "TASKFLOW_MAX_TASK_RETRIES", defaults.maxTaskRetries()),
                longValue(scheduler, env, "metricsLogIntervalMs", "TASKFLOW_METRICS_LOG_INTERVAL_MS",
                        defaults.metricsLogIntervalMillis()),
                doubleValue(scoring, env, "loadWeight", "TASKFLOW_SCORE_LOAD_WEIGHT",
                        defaults.peerScoreLoadWeight()),
                doubleValue(scoring, env, "latencyWeight", "TASKFLOW_SCORE_LATENCY_WEIGHT",
                        defaults.peerScoreLatencyWeight()),
                doubleValue(scoring, env, "durationWeight", "TASKFLOW_SCORE_DURATION_WEIGHT",
                        defaults.peerScoreDurationWeight()),
                doubleValue(scoring, env, "failureWeight", "TASKFLOW_SCORE_FAILURE_WEIGHT",
                        defaults.peerScoreFailureWeight()),
                doubleValue(scoring, env, "latencyBaselineMs", "TASKFLOW_SCORE_LATENCY_BASELINE_MS",
                        defaults.peerScoreLatencyBaselineMillis()),
                doubleValue(scoring, env, "durationBaselineMs", "TASKFLOW_SCORE_DURATION_BASELINE_MS",
                        defaults.peerScoreDurationBaselineMillis()),
                doubleValue(scoring, env, "ewmaAlpha", "TASKFLOW_SCORE_EWMA_ALPHA",
                        defaults.peerScoreEwmaAlpha())
        );
    }

    private static Path configuredPath(Map<String, String> env) {
        String explicitPath = env.get(CONFIG_PATH_ENV);
        if (explicitPath != null && !explicitPath.isBlank()) {
            return Path.of(explicitPath);
        }
        return DEFAULT_CONFIG_PATH;
    }

    private static Map<String, Object> readConfigFile(Path path) {
        try (InputStream input = Files.newInputStream(path)) {
            Object loaded = new Yaml(new SafeConstructor(new LoaderOptions())).load(input);
            if (loaded == null) {
                return Map.of();
            }
            if (!(loaded instanceof Map<?, ?> rawMap)) {
                throw new IllegalArgumentException("TaskFlow config root must be a YAML object: " + path);
            }
            return stringKeyMap(rawMap);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read TaskFlow config file: " + path, e);
        }
    }

    private static int intValue(Map<String, Object> yaml,
                                Map<String, String> env,
                                String yamlKey,
                                String envKey,
                                int fallback) {
        if (hasValue(env, envKey)) {
            return Integer.parseInt(env.get(envKey));
        }
        Object value = yaml.get(yamlKey);
        if (value instanceof Number number) {
            return number.intValue();
        }
        return value == null ? fallback : Integer.parseInt(String.valueOf(value));
    }

    private static long longValue(Map<String, Object> yaml,
                                  Map<String, String> env,
                                  String yamlKey,
                                  String envKey,
                                  long fallback) {
        if (hasValue(env, envKey)) {
            return Long.parseLong(env.get(envKey));
        }
        Object value = yaml.get(yamlKey);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return value == null ? fallback : Long.parseLong(String.valueOf(value));
    }

    private static double doubleValue(Map<String, Object> yaml,
                                      Map<String, String> env,
                                      String yamlKey,
                                      String envKey,
                                      double fallback) {
        if (hasValue(env, envKey)) {
            return Double.parseDouble(env.get(envKey));
        }
        Object value = yaml.get(yamlKey);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return value == null ? fallback : Double.parseDouble(String.valueOf(value));
    }

    private static boolean hasValue(Map<String, String> env, String key) {
        String value = env.get(key);
        return value != null && !value.isBlank();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> childMap(Map<String, Object> source, String key) {
        Object value = source.get(key);
        if (value == null) {
            return Map.of();
        }
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException("TaskFlow config field must be an object: " + key);
    }

    private static Map<String, Object> stringKeyMap(Map<?, ?> rawMap) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nested) {
                value = stringKeyMap(nested);
            }
            out.put(String.valueOf(entry.getKey()), value);
        }
        return out;
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
