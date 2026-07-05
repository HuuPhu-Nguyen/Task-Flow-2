package gui;

import transport.rabbitmq.RabbitMqTransportConfig;

import java.util.Locale;
import java.util.Map;

enum GuiTransportMode {
    TCP(6789, "Host", "Port", "Connect to Coordinator"),
    RABBITMQ(RabbitMqTransportConfig.DEFAULT_PORT, "Broker Host", "Broker Port", "Connect to RabbitMQ Broker");

    private static final String TRANSPORT_ENV = "TASKFLOW_TRANSPORT";

    private final int defaultPort;
    private final String hostLabel;
    private final String portLabel;
    private final String connectButtonText;

    GuiTransportMode(int defaultPort, String hostLabel, String portLabel, String connectButtonText) {
        this.defaultPort = defaultPort;
        this.hostLabel = hostLabel;
        this.portLabel = portLabel;
        this.connectButtonText = connectButtonText;
    }

    int defaultPort() {
        return defaultPort;
    }

    String hostLabel() {
        return hostLabel;
    }

    String portLabel() {
        return portLabel;
    }

    String connectButtonText() {
        return connectButtonText;
    }

    static GuiTransportMode fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    static GuiTransportMode fromEnvironment(Map<String, String> env) {
        String configured = env.getOrDefault(TRANSPORT_ENV, "rabbitmq");
        if (configured == null || configured.isBlank()) {
            return RABBITMQ;
        }
        return switch (configured.trim().toLowerCase(Locale.ROOT)) {
            case "tcp" -> TCP;
            case "rabbitmq" -> RABBITMQ;
            default -> throw new IllegalArgumentException(
                    TRANSPORT_ENV + " must be either tcp or rabbitmq, not '" + configured + "'.");
        };
    }
}
