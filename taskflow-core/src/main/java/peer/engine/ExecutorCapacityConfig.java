package peer.engine;

import plugin.TaskResourceCatalog;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Participant-owned scalar capacity and per-type concurrency configuration.
 */
public record ExecutorCapacityConfig(
        int totalCapacityUnits,
        int executionPoolSize,
        Map<String, Integer> maxConcurrencyByTaskType
) {
    public static final String TOTAL_CAPACITY_UNITS_ENV =
            "TASKFLOW_EXECUTOR_TOTAL_CAPACITY_UNITS";
    public static final String TYPE_CONCURRENCY_LIMITS_ENV =
            "TASKFLOW_EXECUTOR_TYPE_CONCURRENCY_LIMITS";

    public ExecutorCapacityConfig {
        if (executionPoolSize <= 0) {
            throw new IllegalArgumentException("executionPoolSize must be positive.");
        }
        maxConcurrencyByTaskType = maxConcurrencyByTaskType == null
                ? Map.of()
                : Map.copyOf(maxConcurrencyByTaskType);
        if (maxConcurrencyByTaskType.isEmpty()) {
            if (totalCapacityUnits != 0) {
                throw new IllegalArgumentException(
                        "Requester-only executor capacity must use zero total units."
                );
            }
        } else if (totalCapacityUnits <= 0) {
            throw new IllegalArgumentException("totalCapacityUnits must be positive.");
        }
        for (Map.Entry<String, Integer> entry : maxConcurrencyByTaskType.entrySet()) {
            String normalized = TaskResourceCatalog.normalizeTaskType(entry.getKey());
            if (!normalized.equals(entry.getKey())) {
                throw new IllegalArgumentException(
                        "Concurrency task type must be normalized: " + entry.getKey()
                );
            }
            Integer limit = entry.getValue();
            if (limit == null || limit <= 0 || limit > executionPoolSize) {
                throw new IllegalArgumentException(
                        "Concurrency limit for " + normalized
                                + " must be between 1 and executionPoolSize."
                );
            }
        }
    }

    public static ExecutorCapacityConfig fromEnvironment(
            Set<String> supportedTaskTypes,
            int executionPoolSize
    ) {
        return fromEnvironment(System.getenv(), supportedTaskTypes, executionPoolSize);
    }

    static ExecutorCapacityConfig fromEnvironment(
            Map<String, String> env,
            Set<String> supportedTaskTypes,
            int executionPoolSize
    ) {
        if (supportedTaskTypes == null || supportedTaskTypes.isEmpty()) {
            return new ExecutorCapacityConfig(0, executionPoolSize, Map.of());
        }
        Set<String> normalizedTypes = supportedTaskTypes.stream()
                .map(TaskResourceCatalog::normalizeTaskType)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        int totalUnits = env.containsKey(TOTAL_CAPACITY_UNITS_ENV)
                ? parsePositive(env.get(TOTAL_CAPACITY_UNITS_ENV), TOTAL_CAPACITY_UNITS_ENV)
                : Math.max(1, Runtime.getRuntime().availableProcessors());

        Map<String, Integer> limits = new LinkedHashMap<>();
        for (String taskType : normalizedTypes.stream().sorted().toList()) {
            limits.put(taskType, executionPoolSize);
        }
        if (env.containsKey(TYPE_CONCURRENCY_LIMITS_ENV)) {
            applyOverrides(
                    env.get(TYPE_CONCURRENCY_LIMITS_ENV),
                    normalizedTypes,
                    executionPoolSize,
                    limits
            );
        }
        return new ExecutorCapacityConfig(totalUnits, executionPoolSize, limits);
    }

    private static void applyOverrides(String raw,
                                       Set<String> supportedTaskTypes,
                                       int executionPoolSize,
                                       Map<String, Integer> limits) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException(
                    TYPE_CONCURRENCY_LIMITS_ENV + " must not be blank when provided."
            );
        }
        Set<String> overridden = new java.util.HashSet<>();
        for (String rawEntry : raw.split(",", -1)) {
            if (rawEntry.isBlank()) {
                throw invalidConcurrency(raw);
            }
            String[] pair = rawEntry.split(":", -1);
            if (pair.length != 2 || pair[0].isBlank() || pair[1].isBlank()) {
                throw invalidConcurrency(raw);
            }
            String taskType = TaskResourceCatalog.normalizeTaskType(pair[0]);
            if (!supportedTaskTypes.contains(taskType)) {
                throw new IllegalArgumentException(
                        TYPE_CONCURRENCY_LIMITS_ENV
                                + " contains unknown task type " + taskType + "."
                );
            }
            if (!overridden.add(taskType)) {
                throw new IllegalArgumentException(
                        TYPE_CONCURRENCY_LIMITS_ENV
                                + " contains duplicate task type " + taskType + "."
                );
            }
            int limit = parsePositive(pair[1], TYPE_CONCURRENCY_LIMITS_ENV);
            if (limit > executionPoolSize) {
                throw new IllegalArgumentException(
                        "Concurrency limit for " + taskType
                                + " exceeds execution pool size " + executionPoolSize + "."
                );
            }
            limits.put(taskType, limit);
        }
    }

    private static int parsePositive(String raw, String field) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank when provided.");
        }
        try {
            int value = Integer.parseInt(raw.trim());
            if (value <= 0) {
                throw new IllegalArgumentException(field + " must be positive.");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(field + " must be a positive base-10 integer.", e);
        }
    }

    private static IllegalArgumentException invalidConcurrency(String raw) {
        return new IllegalArgumentException(
                TYPE_CONCURRENCY_LIMITS_ENV
                        + " must use TASK_TYPE:LIMIT entries separated by commas: " + raw
        );
    }
}
