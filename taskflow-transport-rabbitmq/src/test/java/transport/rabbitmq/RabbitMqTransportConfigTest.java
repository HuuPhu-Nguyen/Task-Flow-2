package transport.rabbitmq;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertTrue(config.deadLetterEnabled());
        assertEquals("taskflow.dead-letter.exchange", config.deadLetterExchangeName());
        assertEquals("taskflow.dead-letter", config.deadLetterQueueName());
        assertEquals("dead-letter", config.deadLetterRoutingKey());
        assertTrue(config.requeueOnHandlerFailure());
    }

    @Test
    void readsEnvironmentOverrides() {
        RabbitMqTransportConfig config = RabbitMqTransportConfig.fromEnvironment(Map.ofEntries(
                Map.entry("TASKFLOW_RABBITMQ_HOST", "broker"),
                Map.entry("TASKFLOW_RABBITMQ_PORT", "5673"),
                Map.entry("TASKFLOW_RABBITMQ_USERNAME", "taskflow"),
                Map.entry("TASKFLOW_RABBITMQ_PASSWORD", "secret"),
                Map.entry("TASKFLOW_RABBITMQ_VHOST", "/taskflow"),
                Map.entry("TASKFLOW_RABBITMQ_EXCHANGE", "tf.exchange"),
                Map.entry("TASKFLOW_RABBITMQ_QUEUE_PREFIX", "tf"),
                Map.entry("TASKFLOW_RABBITMQ_DURABLE", "false"),
                Map.entry("TASKFLOW_RABBITMQ_PREFETCH", "9"),
                Map.entry("TASKFLOW_RABBITMQ_DEAD_LETTER_ENABLED", "true"),
                Map.entry("TASKFLOW_RABBITMQ_DEAD_LETTER_EXCHANGE", "tf.dlx"),
                Map.entry("TASKFLOW_RABBITMQ_DEAD_LETTER_QUEUE", "tf.dlq"),
                Map.entry("TASKFLOW_RABBITMQ_DEAD_LETTER_ROUTING_KEY", "tf.dead"),
                Map.entry("TASKFLOW_RABBITMQ_REQUEUE_ON_HANDLER_FAILURE", "false")
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
        assertEquals(true, config.deadLetterEnabled());
        assertEquals("tf.dlx", config.deadLetterExchangeName());
        assertEquals("tf.dlq", config.deadLetterQueueName());
        assertEquals("tf.dead", config.deadLetterRoutingKey());
        assertEquals(false, config.requeueOnHandlerFailure());
    }

    @Test
    void rejectsMissingDeadLetterNamesWhenDeadLetteringIsEnabled() {
        RabbitMqTransportConfig defaults = RabbitMqTransportConfig.localDefaults();

        assertThrows(IllegalArgumentException.class, () -> new RabbitMqTransportConfig(
                defaults.host(),
                defaults.port(),
                defaults.username(),
                defaults.password(),
                defaults.virtualHost(),
                defaults.exchangeName(),
                defaults.queuePrefix(),
                defaults.durable(),
                defaults.prefetchCount(),
                true,
                "",
                defaults.deadLetterQueueName(),
                defaults.deadLetterRoutingKey(),
                defaults.requeueOnHandlerFailure()
        ));
    }

    @Test
    void rejectsInvalidBooleanEnvironmentValues() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RabbitMqTransportConfig.fromEnvironment(Map.of(
                        "TASKFLOW_RABBITMQ_DURABLE", "maybe"
                )));

        assertEquals("TASKFLOW_RABBITMQ_DURABLE must be true or false", error.getMessage());
    }
}
