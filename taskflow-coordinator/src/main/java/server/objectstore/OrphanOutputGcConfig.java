package server.objectstore;

import objectstore.ObjectStore;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Bounded runtime configuration for orphan attempt-output collection.
 */
public record OrphanOutputGcConfig(
        boolean enabled,
        long safetyWindowMillis,
        long intervalMillis,
        int batchSize
) {
    public static final String ENABLED_PROPERTY = "taskflow.orphanOutputGcEnabled";
    public static final String ENABLED_ENV = "TASKFLOW_ORPHAN_OUTPUT_GC_ENABLED";
    public static final String SAFETY_WINDOW_PROPERTY =
            "taskflow.orphanOutputGcSafetyWindowMs";
    public static final String SAFETY_WINDOW_ENV =
            "TASKFLOW_ORPHAN_OUTPUT_GC_SAFETY_WINDOW_MS";
    public static final String INTERVAL_PROPERTY = "taskflow.orphanOutputGcIntervalMs";
    public static final String INTERVAL_ENV = "TASKFLOW_ORPHAN_OUTPUT_GC_INTERVAL_MS";
    public static final String BATCH_SIZE_PROPERTY = "taskflow.orphanOutputGcBatchSize";
    public static final String BATCH_SIZE_ENV = "TASKFLOW_ORPHAN_OUTPUT_GC_BATCH_SIZE";

    public static final boolean DEFAULT_ENABLED = true;
    public static final long DEFAULT_SAFETY_WINDOW_MILLIS =
            Duration.ofHours(24).toMillis();
    public static final long DEFAULT_INTERVAL_MILLIS = Duration.ofMinutes(5).toMillis();
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int MIN_BATCH_SIZE = 2;

    public OrphanOutputGcConfig {
        if (safetyWindowMillis <= 0L) {
            throw new IllegalArgumentException("safetyWindowMillis must be positive.");
        }
        if (intervalMillis <= 0L) {
            throw new IllegalArgumentException("intervalMillis must be positive.");
        }
        if (batchSize < MIN_BATCH_SIZE || batchSize > ObjectStore.MAX_LIST_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "batchSize must be between " + MIN_BATCH_SIZE + " and "
                            + ObjectStore.MAX_LIST_PAGE_SIZE + "."
            );
        }
    }

    public static OrphanOutputGcConfig defaults() {
        return new OrphanOutputGcConfig(
                DEFAULT_ENABLED,
                DEFAULT_SAFETY_WINDOW_MILLIS,
                DEFAULT_INTERVAL_MILLIS,
                DEFAULT_BATCH_SIZE
        );
    }

    public static OrphanOutputGcConfig fromRuntime() {
        return fromSources(System.getenv(), System.getProperties());
    }

    static OrphanOutputGcConfig fromSources(
            Map<String, String> environment,
            Properties properties
    ) {
        Objects.requireNonNull(environment, "environment");
        Objects.requireNonNull(properties, "properties");
        OrphanOutputGcConfig defaults = defaults();
        return new OrphanOutputGcConfig(
                booleanValue(
                        environment,
                        properties,
                        ENABLED_ENV,
                        ENABLED_PROPERTY,
                        defaults.enabled()
                ),
                longValue(
                        environment,
                        properties,
                        SAFETY_WINDOW_ENV,
                        SAFETY_WINDOW_PROPERTY,
                        defaults.safetyWindowMillis()
                ),
                longValue(
                        environment,
                        properties,
                        INTERVAL_ENV,
                        INTERVAL_PROPERTY,
                        defaults.intervalMillis()
                ),
                intValue(
                        environment,
                        properties,
                        BATCH_SIZE_ENV,
                        BATCH_SIZE_PROPERTY,
                        defaults.batchSize()
                )
        );
    }

    private static boolean booleanValue(
            Map<String, String> environment,
            Properties properties,
            String environmentName,
            String propertyName,
            boolean fallback
    ) {
        String value = configuredValue(environment, properties, environmentName, propertyName);
        if (value == null) {
            return fallback;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(
                propertyName + "/" + environmentName + " must be true or false."
        );
    }

    private static int intValue(
            Map<String, String> environment,
            Properties properties,
            String environmentName,
            String propertyName,
            int fallback
    ) {
        String value = configuredValue(environment, properties, environmentName, propertyName);
        return value == null ? fallback : Integer.parseInt(value);
    }

    private static long longValue(
            Map<String, String> environment,
            Properties properties,
            String environmentName,
            String propertyName,
            long fallback
    ) {
        String value = configuredValue(environment, properties, environmentName, propertyName);
        return value == null ? fallback : Long.parseLong(value);
    }

    private static String configuredValue(
            Map<String, String> environment,
            Properties properties,
            String environmentName,
            String propertyName
    ) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            value = environment.get(environmentName);
        }
        return value == null || value.isBlank() ? null : value.trim();
    }
}
