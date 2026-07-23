package transport.rabbitmq;

import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqTransportConnectorTest {

    @Test
    void retriesOneAttemptAtATimeWithCappedBackoffThenTransfersOwnership() throws Exception {
        FakeTransport transport = fakeTransport();
        AtomicInteger calls = new AtomicInteger();
        List<Long> delays = new ArrayList<>();
        AtomicReference<RabbitMqTransportConnector> connectorReference = new AtomicReference<>();
        RabbitMqTransportConnector connector = new RabbitMqTransportConnector(
                RabbitMqTransportConfig.localDefaults(),
                recoveryPolicy(),
                () -> {
                    if (calls.incrementAndGet() < 4) {
                        throw new IOException("broker unavailable");
                    }
                    return transport.transport();
                },
                delayMillis -> {
                    assertEquals(
                            RabbitMqTransportConnector.State.WAITING_TO_RETRY,
                            connectorReference.get().state()
                    );
                    delays.add(delayMillis);
                }
        );
        connectorReference.set(connector);

        RabbitMqTransport connected = connector.connect();

        assertSame(transport.transport(), connected);
        assertEquals(4, connector.attempts());
        assertEquals(List.of(10L, 20L, 20L), delays);
        assertEquals(RabbitMqTransportConnector.State.CONNECTED, connector.state());
        assertSame(connected, connector.releaseTransportOwnership());

        connector.close();
        assertEquals(0, transport.channelCloses().get());
        assertEquals(0, transport.connectionCloses().get());
        connected.close();
        assertEquals(1, transport.channelCloses().get());
        assertEquals(1, transport.connectionCloses().get());
    }

    @Test
    void permanentAuthenticationFailureDoesNotLoop() {
        AtomicInteger calls = new AtomicInteger();
        List<Long> delays = new ArrayList<>();
        RabbitMqTransportConnector connector = new RabbitMqTransportConnector(
                RabbitMqTransportConfig.localDefaults(),
                recoveryPolicy(),
                () -> {
                    calls.incrementAndGet();
                    throw new AuthenticationFailureException("bad credentials");
                },
                delays::add
        );

        assertThrows(AuthenticationFailureException.class, connector::connect);
        assertEquals(1, calls.get());
        assertEquals(1, connector.attempts());
        assertTrue(delays.isEmpty());
        assertEquals(RabbitMqTransportConnector.State.FAILED, connector.state());
    }

    @Test
    void wrappedTransportFailureRemainsRetryableButWrappedConfigurationFailureDoesNot() {
        assertTrue(RabbitMqTransportConnector.isRetryable(
                new IllegalStateException("connection wrapper", new IOException("offline"))
        ));
        assertFalse(RabbitMqTransportConnector.isRetryable(
                new IOException("outer", new IllegalArgumentException("invalid config"))
        ));
    }

    @Test
    void shutdownInterruptsLongBackoffAndTerminatesRecoveryOwner() throws Exception {
        CountDownLatch waiting = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
        RabbitMqTransportConnector connector = new RabbitMqTransportConnector(
                RabbitMqTransportConfig.localDefaults(),
                recoveryPolicy(),
                () -> {
                    throw new IOException("broker unavailable");
                },
                delayMillis -> {
                    waiting.countDown();
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                }
        );
        Thread connectorThread = new Thread(() -> {
            try {
                connector.connect();
            } catch (Throwable error) {
                interrupted.set(Thread.currentThread().isInterrupted());
                failure.set(error);
            }
        }, "connector-under-test");

        connectorThread.start();
        assertTrue(waiting.await(1, TimeUnit.SECONDS));
        connector.close();
        connectorThread.join(1_000L);

        assertFalse(connectorThread.isAlive());
        assertInstanceOf(InterruptedException.class, failure.get());
        assertTrue(interrupted.get());
        assertEquals(1, connector.attempts());
        assertEquals(RabbitMqTransportConnector.State.CLOSED, connector.state());
    }

    @Test
    void connectedTransportRemainsOwnedUntilExplicitTransfer() throws Exception {
        FakeTransport transport = fakeTransport();
        RabbitMqTransportConnector connector = new RabbitMqTransportConnector(
                RabbitMqTransportConfig.localDefaults(),
                recoveryPolicy(),
                () -> transport.transport(),
                ignored -> {
                }
        );

        connector.connect();
        connector.close();

        assertEquals(1, transport.channelCloses().get());
        assertEquals(1, transport.connectionCloses().get());
        assertThrows(IllegalStateException.class, connector::releaseTransportOwnership);
    }

    private static RabbitMqRecoveryPolicy recoveryPolicy() {
        return new RabbitMqRecoveryPolicy(50, 10L, 20L, 2.0D);
    }

    private static FakeTransport fakeTransport() throws Exception {
        AtomicInteger channelCloses = new AtomicInteger();
        AtomicInteger connectionCloses = new AtomicInteger();
        Channel channel = (Channel) Proxy.newProxyInstance(
                Channel.class.getClassLoader(),
                new Class<?>[]{Channel.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        channelCloses.incrementAndGet();
                    }
                    return defaultValue(method.getReturnType());
                }
        );
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        connectionCloses.incrementAndGet();
                    }
                    if ("isOpen".equals(method.getName())) {
                        return true;
                    }
                    return defaultValue(method.getReturnType());
                }
        );
        return new FakeTransport(
                new RabbitMqTransport(
                        RabbitMqTransportConfig.localDefaults(),
                        new RabbitMqMessageCodec(),
                        recoveryPolicy(),
                        connection,
                        channel
                ),
                channelCloses,
                connectionCloses
        );
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == void.class) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0F;
        }
        if (returnType == double.class) {
            return 0D;
        }
        return null;
    }

    private record FakeTransport(
            RabbitMqTransport transport,
            AtomicInteger channelCloses,
            AtomicInteger connectionCloses
    ) {
    }
}
