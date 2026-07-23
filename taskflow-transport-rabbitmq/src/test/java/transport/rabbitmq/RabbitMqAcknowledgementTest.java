package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import transport.DeliveryDisposition;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqAcknowledgementTest {
    @ParameterizedTest
    @EnumSource(DeliveryDisposition.class)
    void typedDispositionIsRecordedAndSettlesExactlyOnce(DeliveryDisposition disposition)
            throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(channel.proxy(), 41L);

        acknowledgement.settle(disposition);
        acknowledgement.settle(DeliveryDisposition.ACK_SUCCESS);

        assertEquals(disposition, acknowledgement.settledDisposition());
        assertEquals(disposition.acknowledges() ? 1 : 0, channel.ackCount);
        assertEquals(disposition.retries() ? 1 : 0, channel.nackCount);
        assertEquals(disposition.rejects() ? 1 : 0, channel.rejectCount);
        assertEquals(1, channel.ackCount + channel.nackCount + channel.rejectCount);
    }

    @Test
    void ackSettlesDeliveryOnlyOnce() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(channel.proxy(), 42L);

        acknowledgement.ack();
        acknowledgement.ack();
        acknowledgement.requeue();
        acknowledgement.reject();

        assertTrue(acknowledgement.isSettled());
        assertEquals(1, channel.ackCount);
        assertEquals(42L, channel.ackDeliveryTag);
        assertFalse(channel.ackMultiple);
        assertEquals(0, channel.nackCount);
        assertEquals(0, channel.rejectCount);
    }

    @Test
    void requeueSettlesWithBasicNackOnlyOnce() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(channel.proxy(), 43L);

        acknowledgement.requeue();
        acknowledgement.requeue();
        acknowledgement.ack();
        acknowledgement.reject();

        assertTrue(acknowledgement.isSettled());
        assertEquals(0, channel.ackCount);
        assertEquals(1, channel.nackCount);
        assertEquals(43L, channel.nackDeliveryTag);
        assertFalse(channel.nackMultiple);
        assertTrue(channel.nackRequeue);
        assertEquals(0, channel.rejectCount);
    }

    @Test
    void rejectSettlesWithBasicRejectOnlyOnce() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(channel.proxy(), 44L);

        acknowledgement.reject();
        acknowledgement.reject();
        acknowledgement.ack();
        acknowledgement.requeue();

        assertTrue(acknowledgement.isSettled());
        assertEquals(0, channel.ackCount);
        assertEquals(0, channel.nackCount);
        assertEquals(1, channel.rejectCount);
        assertEquals(44L, channel.rejectDeliveryTag);
        assertFalse(channel.rejectRequeue);
    }

    @Test
    void failedSettlementDoesNotMarkDeliverySettled() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        channel.failAck = true;
        RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(channel.proxy(), 45L);

        assertThrows(IOException.class, acknowledgement::ack);
        assertFalse(acknowledgement.isSettled());

        channel.failAck = false;
        acknowledgement.reject();

        assertTrue(acknowledgement.isSettled());
        assertEquals(1, channel.ackCount);
        assertEquals(1, channel.rejectCount);
        assertEquals(45L, channel.rejectDeliveryTag);
    }

    @Test
    void deferLeavesDeliveryUnsettledUntilExplicitSettlement() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(channel.proxy(), 46L);

        acknowledgement.defer();

        assertFalse(acknowledgement.isSettled());
        assertTrue(acknowledgement.isDeferred());
        assertEquals(0, channel.ackCount);

        acknowledgement.ack();

        assertTrue(acknowledgement.isSettled());
        assertFalse(acknowledgement.isDeferred());
        assertEquals(1, channel.ackCount);
        assertEquals(46L, channel.ackDeliveryTag);
    }

    @Test
    void deferIsIgnoredAfterSettlement() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(channel.proxy(), 47L);

        acknowledgement.reject();
        acknowledgement.defer();

        assertTrue(acknowledgement.isSettled());
        assertFalse(acknowledgement.isDeferred());
        assertEquals(1, channel.rejectCount);
    }

    private static class RecordingChannel {
        private int ackCount;
        private int nackCount;
        private int rejectCount;
        private long ackDeliveryTag;
        private long nackDeliveryTag;
        private long rejectDeliveryTag;
        private boolean ackMultiple;
        private boolean nackMultiple;
        private boolean nackRequeue;
        private boolean rejectRequeue;
        private boolean failAck;

        Channel proxy() {
            return (Channel) Proxy.newProxyInstance(
                    Channel.class.getClassLoader(),
                    new Class<?>[]{Channel.class},
                    this::invoke
            );
        }

        private Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return switch (method.getName()) {
                case "basicAck" -> {
                    ackCount++;
                    ackDeliveryTag = (Long) args[0];
                    ackMultiple = (Boolean) args[1];
                    if (failAck) {
                        throw new IOException("ack failed");
                    }
                    yield null;
                }
                case "basicNack" -> {
                    nackCount++;
                    nackDeliveryTag = (Long) args[0];
                    nackMultiple = (Boolean) args[1];
                    nackRequeue = (Boolean) args[2];
                    yield null;
                }
                case "basicReject" -> {
                    rejectCount++;
                    rejectDeliveryTag = (Long) args[0];
                    rejectRequeue = (Boolean) args[1];
                    yield null;
                }
                case "toString" -> "RecordingChannel";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> defaultValue(method.getReturnType());
            };
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
}
