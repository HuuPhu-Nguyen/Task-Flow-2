package server.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public record MetricsEndpointConfig(boolean enabled, String host, int port) {
    public static final String ENABLED_PROPERTY = "taskflow.metricsEnabled";
    public static final String ENABLED_ENV = "TASKFLOW_METRICS_ENABLED";
    public static final String HOST_PROPERTY = "taskflow.metricsHost";
    public static final String HOST_ENV = "TASKFLOW_METRICS_HOST";
    public static final String PORT_PROPERTY = "taskflow.metricsPort";
    public static final String PORT_ENV = "TASKFLOW_METRICS_PORT";

    public static final boolean DEFAULT_ENABLED = true;
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9464;

    public MetricsEndpointConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Metrics endpoint host is required.");
        }
        host = host.trim();
        if (host.chars().anyMatch(Character::isWhitespace)
                || host.contains("/")
                || host.contains("\\")) {
            throw new IllegalArgumentException("Metrics endpoint host is invalid.");
        }
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException(
                    "Metrics endpoint port must be between 0 and 65535."
            );
        }
    }

    public static MetricsEndpointConfig defaults() {
        return new MetricsEndpointConfig(DEFAULT_ENABLED, DEFAULT_HOST, DEFAULT_PORT);
    }

    public static MetricsEndpointConfig fromRuntime() {
        return fromSources(System.getenv(), System.getProperties());
    }

    static MetricsEndpointConfig fromSources(
            Map<String, String> environment,
            Properties properties
    ) {
        Objects.requireNonNull(environment, "environment");
        Objects.requireNonNull(properties, "properties");
        MetricsEndpointConfig defaults = defaults();
        return new MetricsEndpointConfig(
                booleanValue(
                        configuredValue(
                                environment,
                                properties,
                                ENABLED_ENV,
                                ENABLED_PROPERTY
                        ),
                        defaults.enabled()
                ),
                stringValue(
                        configuredValue(
                                environment,
                                properties,
                                HOST_ENV,
                                HOST_PROPERTY
                        ),
                        defaults.host()
                ),
                intValue(
                        configuredValue(
                                environment,
                                properties,
                                PORT_ENV,
                                PORT_PROPERTY
                        ),
                        defaults.port()
                )
        );
    }

    private static boolean booleanValue(String value, boolean fallback) {
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
                ENABLED_PROPERTY + "/" + ENABLED_ENV + " must be true or false."
        );
    }

    private static int intValue(String value, int fallback) {
        return value == null ? fallback : Integer.parseInt(value);
    }

    private static String stringValue(String value, String fallback) {
        return value == null ? fallback : value;
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
