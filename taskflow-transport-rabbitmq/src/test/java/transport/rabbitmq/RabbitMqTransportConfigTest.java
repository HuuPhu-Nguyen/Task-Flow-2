package transport.rabbitmq;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RabbitMqTransportConfigTest {
    @Test
    void usesLocalDefaultsWhenEnvironmentIsEmpty() {
        RabbitMqTransportConfig config = RabbitMqTransportConfig.fromEnvironment(Map.of());

        assertEquals("localhost", config.host());
        assertEquals(5672, config.port());
        assertEquals("guest", config.username());
        assertEquals("/", config.virtualHost());
        assertEquals("taskflow.exchange", config.exchangeName());
        assertEquals("taskflow", config.queuePrefix());
        assertEquals(3, config.prefetchCount());
    }

    @Test
    void readsEnvironmentOverrides() {
        RabbitMqTransportConfig config = RabbitMqTransportConfig.fromEnvironment(Map.of(
                "TASKFLOW_RABBITMQ_HOST", "broker",
                "TASKFLOW_RABBITMQ_PORT", "5673",
                "TASKFLOW_RABBITMQ_USERNAME", "taskflow",
                "TASKFLOW_RABBITMQ_PASSWORD", "secret",
                "TASKFLOW_RABBITMQ_VHOST", "/taskflow",
                "TASKFLOW_RABBITMQ_EXCHANGE", "tf.exchange",
                "TASKFLOW_RABBITMQ_QUEUE_PREFIX", "tf",
                "TASKFLOW_RABBITMQ_DURABLE", "false",
                "TASKFLOW_RABBITMQ_PREFETCH", "9"
        ));

        assertEquals("broker", config.host());
        assertEquals(5673, config.port());
        assertEquals("taskflow", config.username());
        assertEquals("secret", config.password());
        assertEquals("/taskflow", config.virtualHost());
        assertEquals("tf.exchange", config.exchangeName());
        assertEquals("tf", config.queuePrefix());
        assertEquals(false, config.durable());
        assertEquals(9, config.prefetchCount());
    }
}
