package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqTransportHealthTest {
    @Test
    void recoveryCallbacksRemoveAndRestoreConnectionUsability() throws Exception {
        AtomicBoolean connectionOpen = new AtomicBoolean(true);
        AtomicBoolean channelOpen = new AtomicBoolean(true);
        AtomicReference<RecoveryListener> recoveryListener = new AtomicReference<>();
        Connection connection = recoverableConnection(connectionOpen, recoveryListener);
        Channel channel = channel(channelOpen);
        RabbitMqTransport transport = new RabbitMqTransport(
                config(),
                new RabbitMqMessageCodec(),
                RabbitMqRecoveryPolicy.defaults(),
                connection,
                channel
        );

        assertTrue(transport.connectionUsable());
        assertNotNull(recoveryListener.get());

        recoveryListener.get().handleRecoveryStarted((Recoverable) connection);
        assertFalse(transport.connectionUsable());

        recoveryListener.get().handleTopologyRecoveryStarted((Recoverable) connection);
        assertFalse(transport.connectionUsable());

        recoveryListener.get().handleRecovery((Recoverable) connection);
        assertTrue(transport.connectionUsable());

        channelOpen.set(false);
        assertFalse(transport.connectionUsable());
        channelOpen.set(true);
        assertTrue(transport.connectionUsable());

        transport.close();
        assertFalse(transport.connectionUsable());
    }

    private static Connection recoverableConnection(
            AtomicBoolean open,
            AtomicReference<RecoveryListener> recoveryListener
    ) {
        return (Connection) Proxy.newProxyInstance(
                RabbitMqTransportHealthTest.class.getClassLoader(),
                new Class<?>[] {Connection.class, Recoverable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "addRecoveryListener" -> {
                        recoveryListener.set((RecoveryListener) args[0]);
                        yield null;
                    }
                    case "isOpen" -> open.get();
                    case "close", "abort" -> {
                        open.set(false);
                        yield null;
                    }
                    case "toString" -> "recoverable-health-connection";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> defaultValue(method.getReturnType());
                }
        );
    }

    private static Channel channel(AtomicBoolean open) {
        return (Channel) Proxy.newProxyInstance(
                RabbitMqTransportHealthTest.class.getClassLoader(),
                new Class<?>[] {Channel.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isOpen" -> open.get();
                    case "close", "abort" -> {
                        open.set(false);
                        yield null;
                    }
                    case "toString" -> "health-channel";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> defaultValue(method.getReturnType());
                }
        );
    }

    private static RabbitMqTransportConfig config() {
        return new RabbitMqTransportConfig(
                "localhost",
                5672,
                "guest",
                "guest",
                "/",
                "health.exchange",
                "health",
                true,
                3,
                5_000L,
                true,
                "health.dead-letter.exchange",
                "health.dead-letter",
                "health.dead-letter",
                java.util.List.of(1_000L, 5_000L, 30_000L)
        );
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
