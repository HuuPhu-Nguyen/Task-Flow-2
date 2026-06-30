package gui;

import org.junit.jupiter.api.Test;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GuiTransportModeTest {
    @Test
    void defaultsToTcp() {
        assertEquals(GuiTransportMode.TCP, GuiTransportMode.fromEnvironment(Map.of()));
        assertEquals(6789, GuiTransportMode.TCP.defaultPort());
    }

    @Test
    void selectsRabbitMqByEnvironment() {
        GuiTransportMode mode = GuiTransportMode.fromEnvironment(Map.of("TASKFLOW_TRANSPORT", "rabbitmq"));

        assertEquals(GuiTransportMode.RABBITMQ, mode);
        assertEquals(RabbitMqTransportConfig.DEFAULT_PORT, mode.defaultPort());
    }

    @Test
    void rejectsUnknownTransport() {
        assertThrows(IllegalArgumentException.class, () ->
                GuiTransportMode.fromEnvironment(Map.of("TASKFLOW_TRANSPORT", "udp")));
    }
}
