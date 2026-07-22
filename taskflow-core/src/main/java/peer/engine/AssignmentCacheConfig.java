package peer.engine;

import java.util.Map;

public record AssignmentCacheConfig(int maxEntries, long ttlMillis) {
    public static final String MAX_ENTRIES_ENV = "TASKFLOW_ASSIGNMENT_CACHE_MAX_ENTRIES";
    public static final String TTL_MILLIS_ENV = "TASKFLOW_ASSIGNMENT_CACHE_TTL_MS";
    public static final int DEFAULT_MAX_ENTRIES = 4_096;
    public static final long DEFAULT_TTL_MILLIS = 15 * 60 * 1_000L;

    public AssignmentCacheConfig {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive.");
        }
        if (ttlMillis <= 0L) {
            throw new IllegalArgumentException("ttlMillis must be positive.");
        }
    }

    public static AssignmentCacheConfig defaults() {
        return new AssignmentCacheConfig(DEFAULT_MAX_ENTRIES, DEFAULT_TTL_MILLIS);
    }

    public static AssignmentCacheConfig fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    public static AssignmentCacheConfig fromEnvironment(Map<String, String> environment) {
        AssignmentCacheConfig defaults = defaults();
        return new AssignmentCacheConfig(
                intValue(environment, MAX_ENTRIES_ENV, defaults.maxEntries()),
                longValue(environment, TTL_MILLIS_ENV, defaults.ttlMillis())
        );
    }

    private static int intValue(Map<String, String> environment, String name, int fallback) {
        String value = environment.get(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }

    private static long longValue(Map<String, String> environment, String name, long fallback) {
        String value = environment.get(name);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value.trim());
    }
}
