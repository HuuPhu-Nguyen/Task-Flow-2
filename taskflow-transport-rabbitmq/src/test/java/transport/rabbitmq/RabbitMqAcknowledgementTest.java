package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import transport.DeliveryDisposition;
import transport.TransportRoute;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqAcknowledgementTest {
    @ParameterizedTest
    @EnumSource(DeliveryDisposition.class)
    void typedDispositionIsRecordedAndSettlesExactlyOnce(DeliveryDisposition disposition)
            throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RecordingPublisher publisher = new RecordingPublisher();
        RabbitMqAcknowledgement acknowledgement = acknowledgement(channel, publisher, 41L, 1);

        acknowledgement.settle(disposition, "test_reason");
        acknowledgement.settle(DeliveryDisposition.ACK_SUCCESS, "ignored_second_settlement");

        assertEquals(disposition, acknowledgement.settledDisposition());
        assertEquals("test_reason", acknowledgement.settledReasonCode());
        assertEquals(disposition == DeliveryDisposition.REJECT_INVALID ? 1 : 0, channel.rejectCount);
        assertEquals(disposition == DeliveryDisposition.RETRY_TRANSIENT
                        || disposition == DeliveryDisposition.QUARANTINE_POISON ? 1 : 0,
                publisher.publishCount);
        assertEquals(disposition == DeliveryDisposition.REJECT_INVALID ? 0 : 1, channel.ackCount);
        assertEquals(0, channel.nackCount);
        assertEquals(1, channel.ackCount + channel.rejectCount);
    }

    @Test
    void ackSettlesDeliveryOnlyOnce() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement =
                acknowledgement(channel, new RecordingPublisher(), 42L, 1);

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
    void transientRetryPublishesToFirstDelayQueueThenAcknowledgesSource() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RecordingPublisher publisher = new RecordingPublisher();
        RabbitMqAcknowledgement acknowledgement = acknowledgement(channel, publisher, 43L, 1);

        acknowledgement.settle(DeliveryDisposition.RETRY_TRANSIENT, "dependency_unavailable");

        assertTrue(acknowledgement.isSettled());
        assertEquals(1, publisher.publishCount);
        assertEquals("taskflow.exchange.retry.1.1000ms", publisher.exchange);
        assertEquals(TransportRoute.HEARTBEAT.routingKey(), publisher.routingKey);
        assertEquals(2, publisher.properties.getHeaders().get(RabbitMqRetryHeaders.DELIVERY_ATTEMPT));
        assertEquals(1000L,
                publisher.properties.getHeaders().get(RabbitMqRetryHeaders.RETRY_DELAY_MILLIS));
        assertEquals("dependency_unavailable",
                publisher.properties.getHeaders().get(RabbitMqRetryHeaders.FAILURE_REASON));
        assertNull(publisher.properties.getExpiration(),
                "An original per-message expiration must not shorten the fixed retry-queue delay");
        assertEquals(1, channel.ackCount);
        assertEquals(43L, channel.ackDeliveryTag);
        assertEquals(0, channel.nackCount);
        assertEquals(0, channel.rejectCount);
    }

    @Test
    void exhaustedPoisonPublishesOnceToFinalQuarantineThenAcknowledgesSource() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RecordingPublisher publisher = new RecordingPublisher();
        RabbitMqAcknowledgement acknowledgement = acknowledgement(channel, publisher, 44L, 4);

        acknowledgement.settle(
                DeliveryDisposition.QUARANTINE_POISON,
                "deterministic_processing_failure"
        );

        assertEquals(1, publisher.publishCount);
        assertEquals("taskflow.dead-letter.exchange", publisher.exchange);
        assertEquals("dead-letter.quarantine", publisher.routingKey);
        assertEquals(4,
                publisher.properties.getHeaders().get(RabbitMqRetryHeaders.DELIVERY_ATTEMPT));
        assertEquals(true,
                publisher.properties.getHeaders().get(RabbitMqRetryHeaders.RETRY_EXHAUSTED));
        assertEquals("heartbeats",
                publisher.properties.getHeaders().get(RabbitMqRetryHeaders.ORIGINAL_ROUTING_KEY));
        assertEquals(1, channel.ackCount);
        assertEquals(0, channel.rejectCount);
    }

    @Test
    void unconfirmedRetryPublishRejectsSourceWithoutImmediateRequeue() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RecordingPublisher publisher = new RecordingPublisher();
        publisher.confirmed = false;
        RabbitMqAcknowledgement acknowledgement = acknowledgement(channel, publisher, 45L, 1);

        acknowledgement.settle(DeliveryDisposition.RETRY_TRANSIENT, "dependency_unavailable");

        assertTrue(acknowledgement.isSettled());
        assertEquals(1, publisher.publishCount);
        assertEquals(0, channel.ackCount);
        assertEquals(0, channel.nackCount);
        assertEquals(1, channel.rejectCount);
        assertFalse(channel.rejectRequeue);
    }

    @Test
    void rejectInvalidSettlesWithBasicRejectOnlyOnce() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement =
                acknowledgement(channel, new RecordingPublisher(), 46L, 1);

        acknowledgement.reject();
        acknowledgement.reject();
        acknowledgement.ack();
        acknowledgement.requeue();

        assertTrue(acknowledgement.isSettled());
        assertEquals(0, channel.ackCount);
        assertEquals(0, channel.nackCount);
        assertEquals(1, channel.rejectCount);
        assertEquals(46L, channel.rejectDeliveryTag);
        assertFalse(channel.rejectRequeue);
    }

    @Test
    void failedSettlementDoesNotMarkDeliverySettled() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        channel.failAck = true;
        RabbitMqAcknowledgement acknowledgement =
                acknowledgement(channel, new RecordingPublisher(), 47L, 1);

        assertThrows(IOException.class, acknowledgement::ack);
        assertFalse(acknowledgement.isSettled());

        channel.failAck = false;
        acknowledgement.reject();

        assertTrue(acknowledgement.isSettled());
        assertEquals(1, channel.ackCount);
        assertEquals(1, channel.rejectCount);
        assertEquals(47L, channel.rejectDeliveryTag);
    }

    @Test
    void deferLeavesDeliveryUnsettledUntilExplicitSettlement() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement =
                acknowledgement(channel, new RecordingPublisher(), 48L, 1);

        acknowledgement.defer();

        assertFalse(acknowledgement.isSettled());
        assertTrue(acknowledgement.isDeferred());
        assertEquals(0, channel.ackCount);

        acknowledgement.ack();

        assertTrue(acknowledgement.isSettled());
        assertFalse(acknowledgement.isDeferred());
        assertEquals(1, channel.ackCount);
        assertEquals(48L, channel.ackDeliveryTag);
    }

    @Test
    void deferIsIgnoredAfterSettlement() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        RabbitMqAcknowledgement acknowledgement =
                acknowledgement(channel, new RecordingPublisher(), 49L, 1);

        acknowledgement.reject();
        acknowledgement.defer();

        assertTrue(acknowledgement.isSettled());
        assertFalse(acknowledgement.isDeferred());
        assertEquals(1, channel.rejectCount);
    }

    private static RabbitMqAcknowledgement acknowledgement(RecordingChannel channel,
                                                           RecordingPublisher publisher,
                                                           long deliveryTag,
                                                           int deliveryAttempt) {
        RabbitMqTransportConfig config = RabbitMqTransportConfig.localDefaults();
        Delivery delivery = new Delivery(
                new Envelope(
                        deliveryTag,
                        false,
                        config.exchangeName(),
                        TransportRoute.HEARTBEAT.routingKey()
                ),
                new AMQP.BasicProperties.Builder()
                        .messageId("message-" + deliveryTag)
                        .expiration("1")
                        .headers(Map.of(
                                RabbitMqRetryHeaders.DELIVERY_ATTEMPT,
                                deliveryAttempt,
                                RabbitMqRetryHeaders.ORIGINAL_ROUTING_KEY,
                                "spoofed.route",
                                RabbitMqRetryHeaders.ORIGINAL_EXCHANGE,
                                "spoofed.exchange"
                        ))
                        .build(),
                new byte[]{1, 2, 3}
        );
        RabbitMqDeliveryRetry retry = new RabbitMqDeliveryRetry(
                config,
                new RabbitMqTopology(config),
                delivery,
                publisher
        );
        return new RabbitMqAcknowledgement(channel.proxy(), deliveryTag, retry);
    }

    private static final class RecordingPublisher implements RabbitMqSettlementPublisher {
        private int publishCount;
        private boolean confirmed = true;
        private String exchange;
        private String routingKey;
        private AMQP.BasicProperties properties;

        @Override
        public boolean publish(String exchange,
                               String routingKey,
                               AMQP.BasicProperties properties,
                               byte[] body) {
            publishCount++;
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.properties = properties;
            return confirmed;
        }
    }

    private static class RecordingChannel {
        private int ackCount;
        private int nackCount;
        private int rejectCount;
        private long ackDeliveryTag;
        private long rejectDeliveryTag;
        private boolean ackMultiple;
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
