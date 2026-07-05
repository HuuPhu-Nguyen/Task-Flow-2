package gui;

import org.junit.jupiter.api.Test;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GuiTransportModeTest {
    @Test
    void defaultsToRabbitMq() {
        assertEquals(GuiTransportMode.RABBITMQ, GuiTransportMode.fromEnvironment(Map.of()));
        assertEquals(GuiTransportMode.RABBITMQ, GuiTransportMode.fromEnvironment(Map.of("TASKFLOW_TRANSPORT", " ")));
        assertEquals(6789, GuiTransportMode.TCP.defaultPort());
        assertEquals(RabbitMqTransportConfig.DEFAULT_PORT, GuiTransportMode.RABBITMQ.defaultPort());
    }

    @Test
    void selectsExplicitTcpByEnvironment() {
        GuiTransportMode mode = GuiTransportMode.fromEnvironment(Map.of("TASKFLOW_TRANSPORT", "tcp"));

        assertEquals(GuiTransportMode.TCP, mode);
        assertEquals(6789, mode.defaultPort());
    }

    @Test
    void rejectsUnknownTransport() {
        assertThrows(IllegalArgumentException.class, () ->
                GuiTransportMode.fromEnvironment(Map.of("TASKFLOW_TRANSPORT", "udp")));
    }
}
