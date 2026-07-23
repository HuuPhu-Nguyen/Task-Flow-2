package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Test;
import protocol.PongMessage;
import transport.DeliveryDisposition;
import transport.OutboundTransportMessage;
import transport.TransportRoute;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RabbitMqTransportDeliveryDispositionTest {
    @Test
    void successfulHandlerDefaultsToAckSuccess() throws Exception {
        RecordingChannel channel = new RecordingChannel();

        try (RabbitMqTransport transport = transport(channel)) {
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                channel.capture(delivery);
            });
            channel.deliver(validHeartbeat());
        }

        assertSettlement(channel, 1, 0, 0);
        assertEquals(DeliveryDisposition.ACK_SUCCESS, channel.acknowledgement.get().settledDisposition());
    }

    @Test
    void handlerCanClassifyDuplicateOrStaleWithoutTransportOverride() throws Exception {
        RecordingChannel channel = new RecordingChannel();

        try (RabbitMqTransport transport = transport(channel)) {
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                channel.capture(delivery);
                delivery.acknowledgement().settle(DeliveryDisposition.ACK_DUPLICATE_OR_STALE);
            });
            channel.deliver(validHeartbeat());
        }

        assertSettlement(channel, 1, 0, 0);
        assertEquals(
                DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                channel.acknowledgement.get().settledDisposition()
        );
    }

    @Test
    void transientHandlerFailureIsTheOnlyExceptionCategoryThatRequeues() throws Exception {
        RecordingChannel channel = new RecordingChannel();

        try (RabbitMqTransport transport = transport(channel)) {
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                channel.capture(delivery);
                throw new IOException("broker dependency unavailable");
            });
            channel.deliver(validHeartbeat());
        }

        assertSettlement(channel, 0, 1, 0);
        assertEquals(DeliveryDisposition.RETRY_TRANSIENT, channel.acknowledgement.get().settledDisposition());
    }

    @Test
    void invalidHandlerFailureIsRejectedWithoutRequeue() throws Exception {
        RecordingChannel channel = new RecordingChannel();

        try (RabbitMqTransport transport = transport(channel)) {
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                channel.capture(delivery);
                throw new IllegalArgumentException("invalid heartbeat");
            });
            channel.deliver(validHeartbeat());
        }

        assertSettlement(channel, 0, 0, 1);
        assertEquals(DeliveryDisposition.REJECT_INVALID, channel.acknowledgement.get().settledDisposition());
    }

    @Test
    void deterministicHandlerFailureIsClassifiedAsPoisonWithoutRequeue() throws Exception {
        RecordingChannel channel = new RecordingChannel();

        try (RabbitMqTransport transport = transport(channel)) {
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                channel.capture(delivery);
                throw new IllegalStateException("deterministic handler invariant failed");
            });
            channel.deliver(validHeartbeat());
        }

        assertSettlement(channel, 0, 0, 1);
        assertEquals(
                DeliveryDisposition.QUARANTINE_POISON,
                channel.acknowledgement.get().settledDisposition()
        );
    }

    @Test
    void malformedDeliveryIsRejectedBeforeHandlerInvocation() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        java.util.concurrent.atomic.AtomicBoolean invoked = new java.util.concurrent.atomic.AtomicBoolean();

        try (RabbitMqTransport transport = transport(channel)) {
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> invoked.set(true));
            channel.deliver("{not-json".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        assertFalse(invoked.get());
        assertSettlement(channel, 0, 0, 1);
    }

    private static RabbitMqTransport transport(RecordingChannel recording) throws Exception {
        Channel channel = recording.proxy();
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "close" -> null;
                    case "isOpen" -> true;
                    default -> defaultValue(method.getReturnType());
                }
        );
        return new RabbitMqTransport(
                RabbitMqTransportConfig.localDefaults(),
                new RabbitMqMessageCodec(),
                connection,
                channel
        );
    }

    private static byte[] validHeartbeat() {
        return new RabbitMqMessageCodec().encode(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                new PongMessage("peer-1", Instant.EPOCH.toString(), List.of("TEXT_ANALYSIS"))
        ));
    }

    private static void assertSettlement(RecordingChannel channel,
                                         int expectedAcks,
                                         int expectedNacks,
                                         int expectedRejects) {
        assertEquals(expectedAcks, channel.ackCount);
        assertEquals(expectedNacks, channel.nackCount);
        assertEquals(expectedRejects, channel.rejectCount);
        assertEquals(1, channel.ackCount + channel.nackCount + channel.rejectCount);
    }

    private static final class RecordingChannel {
        private DeliverCallback deliverCallback;
        private int ackCount;
        private int nackCount;
        private int rejectCount;
        private final AtomicReference<RabbitMqAcknowledgement> acknowledgement = new AtomicReference<>();

        private Channel proxy() {
            return (Channel) Proxy.newProxyInstance(
                    Channel.class.getClassLoader(),
                    new Class<?>[]{Channel.class},
                    this::invoke
            );
        }

        private Object invoke(Object proxy, Method method, Object[] args) {
            return switch (method.getName()) {
                case "basicConsume" -> {
                    deliverCallback = (DeliverCallback) args[2];
                    yield "consumer-1";
                }
                case "basicAck" -> {
                    ackCount++;
                    yield null;
                }
                case "basicNack" -> {
                    nackCount++;
                    assertEquals(true, args[2]);
                    yield null;
                }
                case "basicReject" -> {
                    rejectCount++;
                    assertEquals(false, args[1]);
                    yield null;
                }
                case "close", "basicQos", "confirmSelect", "addReturnListener" -> null;
                case "isOpen" -> true;
                default -> defaultValue(method.getReturnType());
            };
        }

        private void deliver(byte[] body) throws IOException {
            Delivery delivery = new Delivery(
                    new Envelope(17L, false, "taskflow.exchange", TransportRoute.HEARTBEAT.routingKey()),
                    new AMQP.BasicProperties.Builder().build(),
                    body
            );
            assertNotNull(deliverCallback);
            deliverCallback.handle("consumer-1", delivery);
        }

        private void capture(transport.InboundTransportMessage delivery) {
            acknowledgement.set((RabbitMqAcknowledgement) delivery.acknowledgement());
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == char.class) {
            return '\0';
        }
        if (type == float.class) {
            return 0.0f;
        }
        return 0.0d;
    }
}
