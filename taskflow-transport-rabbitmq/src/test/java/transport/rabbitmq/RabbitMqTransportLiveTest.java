package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.Message;
import protocol.PongMessage;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RabbitMqTransportLiveTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";

    @Test
    void publishesConsumesAcknowledgesSharedRouteAndConsumesPeerRouteAgainstLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        try {
            cleanup(config, topology, "peer-live");

            try (RabbitMqTransport transport = new RabbitMqTransport(config)) {
                transport.declareTopology();

                CountDownLatch heartbeatReceived = new CountDownLatch(1);
                AtomicReference<Throwable> heartbeatFailure = new AtomicReference<>();
                String heartbeatConsumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    assertDelivery(heartbeatReceived, heartbeatFailure,
                            () -> assertHeartbeatDelivery(delivery));
                });
                transport.publish(new OutboundTransportMessage(
                        TransportRoute.HEARTBEAT,
                        "peer-live",
                        new PongMessage("peer-live", Instant.now().toString(), List.of("TEXT_ANALYSIS"))
                ));
                awaitDelivery(heartbeatReceived, heartbeatFailure, "HEARTBEAT");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                transport.cancel(heartbeatConsumer);

                CountDownLatch jobResultReceived = new CountDownLatch(1);
                AtomicReference<Throwable> jobResultFailure = new AtomicReference<>();
                String resultConsumer = transport.subscribePeer(TransportRoute.JOB_RESULT, "peer-live", delivery -> {
                    assertDelivery(jobResultReceived, jobResultFailure,
                            () -> assertPeerJobResultDelivery(delivery));
                });
                transport.publishToPeer(
                        TransportRoute.JOB_RESULT,
                        "peer-live",
                        new OutboundTransportMessage(
                                TransportRoute.JOB_RESULT,
                                "COORDINATOR",
                                new JobResultMessage(
                                        "COORDINATOR",
                                        Instant.now().toString(),
                                        "job-live",
                                        "TEXT_ANALYSIS",
                                        true,
                                        List.of()
                                )
                        )
                );
                awaitDelivery(jobResultReceived, jobResultFailure, "peer JOB_RESULT");
                transport.cancel(resultConsumer);
            }
        } finally {
            cleanup(config, topology, "peer-live");
        }
    }

    private static void assertHeartbeatDelivery(InboundTransportMessage delivery) {
        assertEquals(TransportRoute.HEARTBEAT, delivery.route());
        assertEquals("peer-live", delivery.fromNodeId());
        PongMessage message = assertMessage(PongMessage.class, delivery);
        assertEquals("peer-live", message.getNodeId());
        assertEquals(List.of("TEXT_ANALYSIS"), message.getSupportedTaskTypes());
    }

    private static void assertPeerJobResultDelivery(InboundTransportMessage delivery) {
        assertEquals(TransportRoute.JOB_RESULT, delivery.route());
        assertEquals("COORDINATOR", delivery.fromNodeId());
        JobResultMessage message = assertMessage(JobResultMessage.class, delivery);
        assertEquals("job-live", message.getJobId());
        assertEquals("TEXT_ANALYSIS", message.getTaskType());
        assertTrue(message.isSuccessful());
    }

    private static <T extends Message> T assertMessage(Class<T> type, InboundTransportMessage delivery) {
        return assertInstanceOf(type, delivery.message());
    }

    private static void assertDelivery(CountDownLatch received,
                                       AtomicReference<Throwable> failure,
                                       CheckedAssertion assertion) {
        try {
            assertion.run();
        } catch (Throwable assertionError) {
            failure.set(assertionError);
        } finally {
            received.countDown();
        }
    }

    private static void awaitDelivery(CountDownLatch received,
                                      AtomicReference<Throwable> failure,
                                      String description) throws InterruptedException {
        assertTrue(received.await(10, TimeUnit.SECONDS), "Timed out waiting for " + description + " delivery");
        Throwable assertionError = failure.get();
        if (assertionError != null) {
            fail(assertionError);
        }
    }

    @FunctionalInterface
    private interface CheckedAssertion {
        void run() throws Exception;
    }

    private static void waitForQueueToDrain(RabbitMqTransportConfig config, String queueName) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (messageCount(config, queueName) == 0) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(0, messageCount(config, queueName), "Expected RabbitMQ queue to be drained: " + queueName);
    }

    private static long messageCount(RabbitMqTransportConfig config, String queueName) throws Exception {
        try (Connection connection = connectionFactory(config).newConnection("taskflow-rabbitmq-live-test-inspect");
             Channel channel = connection.createChannel()) {
            return channel.queueDeclarePassive(queueName).getMessageCount();
        }
    }

    private static RabbitMqTransportConfig liveConfig(String token) {
        RabbitMqTransportConfig base = RabbitMqTransportConfig.fromEnvironment();
        String name = "taskflow.live." + token;
        return new RabbitMqTransportConfig(
                base.host(),
                base.port(),
                base.username(),
                base.password(),
                base.virtualHost(),
                name + ".exchange",
                name,
                false,
                1,
                true,
                name + ".dlx",
                name + ".dlq",
                "dead-letter",
                false
        );
    }

    private static boolean liveTestEnabled() {
        if (Boolean.getBoolean(LIVE_TEST_PROPERTY)) {
            return true;
        }
        String enabled = System.getenv(LIVE_TEST_ENV);
        return "true".equalsIgnoreCase(enabled);
    }

    private static ConnectionFactory connectionFactory(RabbitMqTransportConfig config) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(false);
        return factory;
    }

    private static void cleanup(RabbitMqTransportConfig config,
                                RabbitMqTopology topology,
                                String peerId) throws Exception {
        for (TransportRoute route : TransportRoute.values()) {
            deleteQueue(config, topology.queueName(route));
        }
        deleteQueue(config, topology.peerQueueName(TransportRoute.JOB_RESULT, peerId));
        deleteQueue(config, config.deadLetterQueueName());
        deleteExchange(config, config.exchangeName());
        deleteExchange(config, config.deadLetterExchangeName());
    }

    private static void deleteQueue(RabbitMqTransportConfig config, String queueName) {
        try (Connection connection = connectionFactory(config).newConnection("taskflow-rabbitmq-live-test-cleanup");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(queueName);
        } catch (Exception ignored) {
        }
    }

    private static void deleteExchange(RabbitMqTransportConfig config, String exchangeName) {
        try (Connection connection = connectionFactory(config).newConnection("taskflow-rabbitmq-live-test-cleanup");
             Channel channel = connection.createChannel()) {
            channel.exchangeDelete(exchangeName);
        } catch (Exception ignored) {
        }
    }
}
