package transport.rabbitmq;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RabbitMqRecoveryPolicyTest {

    @Test
    void defaultsDefineFiniteBoundedRecoveryWork() {
        RabbitMqRecoveryPolicy policy = RabbitMqRecoveryPolicy.defaults();

        assertEquals(5_000, policy.connectionTimeoutMillis());
        assertEquals(1_000L, policy.initialRetryDelayMillis());
        assertEquals(30_000L, policy.maxRetryDelayMillis());
        assertEquals(2.0D, policy.backoffMultiplier());
        assertEquals(1_000L, policy.retryDelayMillis(1));
        assertEquals(2_000L, policy.retryDelayMillis(2));
        assertEquals(30_000L, policy.retryDelayMillis(100));
    }

    @Test
    void environmentOverridesEveryRecoveryBound() {
        RabbitMqRecoveryPolicy policy = RabbitMqRecoveryPolicy.fromEnvironment(Map.of(
                "TASKFLOW_RABBITMQ_CONNECTION_TIMEOUT_MS", "2500",
                "TASKFLOW_RABBITMQ_RECOVERY_INITIAL_DELAY_MS", "125",
                "TASKFLOW_RABBITMQ_RECOVERY_MAX_DELAY_MS", "1000",
                "TASKFLOW_RABBITMQ_RECOVERY_BACKOFF_MULTIPLIER", "1.5"
        ));

        assertEquals(2_500, policy.connectionTimeoutMillis());
        assertEquals(125L, policy.initialRetryDelayMillis());
        assertEquals(1_000L, policy.maxRetryDelayMillis());
        assertEquals(1.5D, policy.backoffMultiplier());
        assertEquals(125L, policy.retryDelayMillis(1));
        assertEquals(188L, policy.retryDelayMillis(2));
        assertEquals(282L, policy.retryDelayMillis(3));
        assertEquals(1_000L, policy.retryDelayMillis(100));
    }

    @Test
    void invalidBoundsFailBeforeAnyConnectionAttempt() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new RabbitMqRecoveryPolicy(0, 1L, 1L, 1.0D)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new RabbitMqRecoveryPolicy(1, 0L, 1L, 1.0D)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new RabbitMqRecoveryPolicy(1, 2L, 1L, 1.0D)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new RabbitMqRecoveryPolicy(1, 1L, 1L, 0.99D)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new RabbitMqRecoveryPolicy(1, 1L, 1L, Double.NaN)
        );
        RabbitMqRecoveryPolicy policy = RabbitMqRecoveryPolicy.defaults();
        assertThrows(IllegalArgumentException.class, () -> policy.retryDelayMillis(0));
    }
}
