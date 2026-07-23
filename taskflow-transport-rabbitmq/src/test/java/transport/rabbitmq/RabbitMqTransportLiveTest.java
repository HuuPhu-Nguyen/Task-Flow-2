package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.Message;
import protocol.PongMessage;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RabbitMqTransportLiveTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";
    private static final String MANAGEMENT_URL_ENV = "TASKFLOW_RABBITMQ_MANAGEMENT_URL";
    private static final String MANAGEMENT_USERNAME_ENV = "TASKFLOW_RABBITMQ_MANAGEMENT_USERNAME";
    private static final String MANAGEMENT_PASSWORD_ENV = "TASKFLOW_RABBITMQ_MANAGEMENT_PASSWORD";
    private static final String TRANSPORT_CONNECTION_NAME = "taskflow-rabbitmq-transport";
    private static final Duration MANAGEMENT_REQUEST_TIMEOUT = Duration.ofSeconds(5);

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

    @Test
    void recoversConsumerAndPublisherAfterBrokerSideConnectionDropAgainstLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);
        RabbitMqManagement management = RabbitMqManagement.fromEnvironment(config);
        Assumptions.assumeTrue(management.isAvailable(),
                "RabbitMQ management API is required for broker-side connection-drop recovery coverage.");

        try {
            cleanup(config, topology, "peer-live");

            try (RabbitMqTransport transport = new RabbitMqTransport(config)) {
                transport.declareTopology();

                CountDownLatch firstDelivered = new CountDownLatch(1);
                CountDownLatch recoveredDelivery = new CountDownLatch(1);
                AtomicInteger deliveries = new AtomicInteger();
                AtomicReference<Throwable> failure = new AtomicReference<>();

                String consumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    try {
                        assertHeartbeatDelivery(delivery);
                        int deliveryNumber = deliveries.incrementAndGet();
                        if (deliveryNumber == 1) {
                            firstDelivered.countDown();
                            return;
                        }
                        if (deliveryNumber == 2) {
                            recoveredDelivery.countDown();
                            return;
                        }
                        failure.compareAndSet(null,
                                new AssertionError("Unexpected recovered delivery count: " + deliveryNumber));
                    } catch (Throwable assertionError) {
                        failure.set(assertionError);
                        firstDelivered.countDown();
                        recoveredDelivery.countDown();
                    }
                });

                publishHeartbeat(transport);
                awaitDelivery(firstDelivered, failure, "initial HEARTBEAT before broker-side connection drop");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));

                management.closeConnectionByClientProvidedName(TRANSPORT_CONNECTION_NAME, config.virtualHost());
                publishHeartbeatWithRecoveryRetry(transport);

                awaitDelivery(recoveredDelivery, failure, "HEARTBEAT after RabbitMQ client recovery");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                transport.cancel(consumer);
            }
        } finally {
            cleanup(config, topology, "peer-live");
        }
    }

    @Test
    void rejectsDeterministicHandlerFailureToDeadLetterQueueAgainstLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        try {
            cleanup(config, topology, "peer-live");

            try (RabbitMqTransport transport = new RabbitMqTransport(config)) {
                transport.declareTopology();

                CountDownLatch deliveryAttempted = new CountDownLatch(1);
                String consumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    deliveryAttempted.countDown();
                    throw new IllegalStateException("expected live-test handler failure");
                });
                transport.publish(new OutboundTransportMessage(
                        TransportRoute.HEARTBEAT,
                        "peer-live",
                        new PongMessage("peer-live", Instant.now().toString(), List.of("TEXT_ANALYSIS"))
                ));

                assertTrue(deliveryAttempted.await(10, TimeUnit.SECONDS),
                        "Timed out waiting for failed HEARTBEAT delivery");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                waitForQueueCount(config, config.deadLetterQueueName(), 1);
                transport.cancel(consumer);
            }
        } finally {
            cleanup(config, topology, "peer-live");
        }
    }

    @Test
    void inspectsAndRedrivesDeadLetteredDeterministicHandlerFailureAgainstLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        try {
            cleanup(config, topology, "peer-live");

            try (RabbitMqTransport transport = new RabbitMqTransport(config);
                 RabbitMqDlqClient dlqClient = new RabbitMqDlqClient(config)) {
                transport.declareTopology();
                dlqClient.declareTopology();

                CountDownLatch failedDelivery = new CountDownLatch(1);
                String failingConsumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    failedDelivery.countDown();
                    throw new IllegalStateException("expected live-test handler failure before redrive");
                });
                publishHeartbeat(transport);
                assertTrue(failedDelivery.await(10, TimeUnit.SECONDS),
                        "Timed out waiting for failed HEARTBEAT delivery");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                waitForQueueCount(config, config.deadLetterQueueName(), 1);
                transport.cancel(failingConsumer);

                List<RabbitMqDlqMessage> inspected = dlqClient.inspect(1);
                assertEquals(1, inspected.size());
                assertEquals(TransportRoute.HEARTBEAT, inspected.getFirst().inferredRoute());
                assertTrue(inspected.getFirst().redrivable());

                CountDownLatch redrivenDelivery = new CountDownLatch(1);
                AtomicReference<Throwable> redriveFailure = new AtomicReference<>();
                String healthyConsumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    assertDelivery(redrivenDelivery, redriveFailure,
                            () -> assertHeartbeatDelivery(delivery));
                });

                List<RabbitMqDlqDecisionResult> redriveResults = dlqClient.redrive(1);
                assertEquals(1, redriveResults.size());
                assertEquals(RabbitMqDlqDecisionResult.Status.REDRIVEN, redriveResults.getFirst().status());

                awaitDelivery(redrivenDelivery, redriveFailure, "redriven HEARTBEAT");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                waitForQueueToDrain(config, config.deadLetterQueueName());
                transport.cancel(healthyConsumer);
            }
        } finally {
            cleanup(config, topology, "peer-live");
        }
    }

    @Test
    void malformedDeliveryIsDeadLetteredNotRedrivenAndCanBeQuarantinedAgainstLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        try {
            cleanup(config, topology, "peer-live");

            try (RabbitMqTransport transport = new RabbitMqTransport(config);
                 RabbitMqDlqClient dlqClient = new RabbitMqDlqClient(config)) {
                transport.declareTopology();
                dlqClient.declareTopology();

                String consumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery ->
                        fail("Malformed delivery should be rejected before it reaches the handler"));
                publishRaw(config, TransportRoute.HEARTBEAT.routingKey(), "{not valid json".getBytes(StandardCharsets.UTF_8));
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                waitForQueueCount(config, config.deadLetterQueueName(), 1);
                transport.cancel(consumer);

                RabbitMqDlqDecisionResult redriveResult = dlqClient.redriveNext();
                assertEquals(RabbitMqDlqDecisionResult.Status.NOT_REDRIVABLE, redriveResult.status());
                assertNotNull(redriveResult.message());
                assertFalse(redriveResult.message().redrivable());
                waitForQueueCount(config, config.deadLetterQueueName(), 1);

                RabbitMqDlqDecisionResult quarantineResult = dlqClient.quarantineNext();
                assertEquals(RabbitMqDlqDecisionResult.Status.QUARANTINED, quarantineResult.status());
                waitForQueueToDrain(config, config.deadLetterQueueName());
                waitForQueueCount(config, topology.deadLetterQuarantineQueueName(), 1);
            }
        } finally {
            cleanup(config, topology, "peer-live");
        }
    }

    @Test
    void requeuesTransientHandlerFailuresAgainstLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        try {
            cleanup(config, topology, "peer-live");

            try (RabbitMqTransport transport = new RabbitMqTransport(config)) {
                transport.declareTopology();

                CountDownLatch redelivered = new CountDownLatch(1);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
                String consumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt == 1) {
                        throw new IOException("expected transient live-test requeue");
                    }
                    assertDelivery(redelivered, failure, () -> assertHeartbeatDelivery(delivery));
                });
                transport.publish(new OutboundTransportMessage(
                        TransportRoute.HEARTBEAT,
                        "peer-live",
                        new PongMessage("peer-live", Instant.now().toString(), List.of("TEXT_ANALYSIS"))
                ));

                awaitDelivery(redelivered, failure, "requeued HEARTBEAT redelivery");
                assertTrue(attempts.get() >= 2, "Expected at least one failed attempt and one redelivery");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                waitForQueueCount(config, config.deadLetterQueueName(), 0);
                transport.cancel(consumer);
            }
        } finally {
            cleanup(config, topology, "peer-live");
        }
    }

    @Test
    void prefetchLimitsUnacknowledgedDeliveriesAgainstLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        try {
            cleanup(config, topology, "peer-live");

            try (RabbitMqTransport transport = new RabbitMqTransport(config)) {
                transport.declareTopology();

                CountDownLatch firstDelivered = new CountDownLatch(1);
                CountDownLatch secondDelivered = new CountDownLatch(1);
                AtomicInteger deliveries = new AtomicInteger();
                AtomicReference<TransportAcknowledgement> firstAcknowledgement = new AtomicReference<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();

                String consumer = transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    try {
                        assertHeartbeatDelivery(delivery);
                        int deliveryNumber = deliveries.incrementAndGet();
                        if (deliveryNumber == 1) {
                            delivery.acknowledgement().defer();
                            firstAcknowledgement.set(delivery.acknowledgement());
                            firstDelivered.countDown();
                            return;
                        }
                        if (deliveryNumber == 2) {
                            secondDelivered.countDown();
                            return;
                        }
                        failure.compareAndSet(null,
                                new AssertionError("Unexpected delivery count: " + deliveryNumber));
                    } catch (Throwable assertionError) {
                        failure.set(assertionError);
                        firstDelivered.countDown();
                        secondDelivered.countDown();
                    }
                });

                publishHeartbeat(transport);
                publishHeartbeat(transport);

                awaitDelivery(firstDelivered, failure, "first prefetch-limited HEARTBEAT");
                assertEquals(1, deliveries.get(), "Expected only one unacknowledged delivery with prefetch=1");
                assertFalse(secondDelivered.await(500, TimeUnit.MILLISECONDS),
                        "Second delivery should wait until the first delivery is acknowledged");

                TransportAcknowledgement acknowledgement = firstAcknowledgement.get();
                assertTrue(acknowledgement != null, "Expected first delivery acknowledgement");
                acknowledgement.ack();

                awaitDelivery(secondDelivered, failure, "second HEARTBEAT after acknowledgement");
                waitForQueueToDrain(config, topology.queueName(TransportRoute.HEARTBEAT));
                transport.cancel(consumer);
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

    private static boolean publishHeartbeat(RabbitMqTransport transport) throws Exception {
        return transport.publish(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-live",
                new PongMessage("peer-live", Instant.now().toString(), List.of("TEXT_ANALYSIS"))
        ));
    }

    private static void publishHeartbeatWithRecoveryRetry(RabbitMqTransport transport) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (publishHeartbeat(transport)) {
                    return;
                }
                lastFailure = new AssertionError("RabbitMQ publish returned false during recovery");
            } catch (Throwable failure) {
                lastFailure = failure;
            }
            Thread.sleep(500);
        }
        AssertionError error = new AssertionError("RabbitMQ transport did not recover in time to publish again");
        if (lastFailure != null) {
            error.addSuppressed(lastFailure);
        }
        throw error;
    }

    private static void publishRaw(RabbitMqTransportConfig config,
                                   String routingKey,
                                   byte[] body) throws Exception {
        try (Connection connection = connectionFactory(config).newConnection("taskflow-rabbitmq-live-test-publish-raw");
             Channel channel = connection.createChannel()) {
            channel.basicPublish(
                    config.exchangeName(),
                    routingKey,
                    new com.rabbitmq.client.AMQP.BasicProperties.Builder()
                            .contentType("application/json")
                            .build(),
                    body
            );
        }
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
        waitForQueueCount(config, queueName, 0);
    }

    private static void waitForQueueCount(RabbitMqTransportConfig config,
                                          String queueName,
                                          long expectedCount) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (messageCount(config, queueName) == expectedCount) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(expectedCount, messageCount(config, queueName),
                "Unexpected RabbitMQ queue count: " + queueName);
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
                base.publisherConfirmTimeoutMillis(),
                true,
                name + ".dlx",
                name + ".dlq",
                "dead-letter"
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
        deleteQueue(config, topology.deadLetterQuarantineQueueName());
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

    private static final class RabbitMqManagement {
        private final HttpClient client;
        private final String baseUrl;
        private final String authorization;

        private RabbitMqManagement(String baseUrl, String username, String password) {
            this.client = HttpClient.newBuilder()
                    .connectTimeout(MANAGEMENT_REQUEST_TIMEOUT)
                    .build();
            this.baseUrl = trimTrailingSlash(baseUrl);
            String credentials = username + ":" + password;
            this.authorization = "Basic " + Base64.getEncoder()
                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        }

        private static RabbitMqManagement fromEnvironment(RabbitMqTransportConfig config) {
            String baseUrl = value(
                    MANAGEMENT_URL_ENV,
                    "http://" + config.host() + ":15672"
            );
            String username = value(MANAGEMENT_USERNAME_ENV, config.username());
            String password = value(MANAGEMENT_PASSWORD_ENV, config.password());
            return new RabbitMqManagement(baseUrl, username, password);
        }

        private boolean isAvailable() {
            try {
                HttpResponse<String> response = client.send(
                        request("/api/overview").GET().build(),
                        HttpResponse.BodyHandlers.ofString()
                );
                return response.statusCode() == 200;
            } catch (Exception ignored) {
                return false;
            }
        }

        private void closeConnectionByClientProvidedName(String clientProvidedName, String virtualHost) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                String connectionName = findConnectionName(clientProvidedName, virtualHost);
                if (connectionName != null) {
                    closeConnection(connectionName);
                    return;
                }
                Thread.sleep(100);
            }
            fail("RabbitMQ management API did not report transport connection: " + clientProvidedName);
        }

        private String findConnectionName(String clientProvidedName, String virtualHost) throws Exception {
            HttpResponse<String> response = client.send(
                    request("/api/connections").GET().build(),
                    HttpResponse.BodyHandlers.ofString()
            );
            if (response.statusCode() != 200) {
                throw new IllegalStateException("RabbitMQ management connection list failed with HTTP "
                        + response.statusCode());
            }

            JsonArray connections = JsonParser.parseString(response.body()).getAsJsonArray();
            for (JsonElement element : connections) {
                JsonObject connection = element.getAsJsonObject();
                if (!virtualHost.equals(stringField(connection, "vhost"))) {
                    continue;
                }
                JsonObject clientProperties = objectField(connection, "client_properties");
                if (clientProperties == null
                        || !clientProvidedName.equals(stringField(clientProperties, "connection_name"))) {
                    continue;
                }
                return stringField(connection, "name");
            }
            return null;
        }

        private void closeConnection(String connectionName) throws Exception {
            String encodedName = URLEncoder.encode(connectionName, StandardCharsets.UTF_8)
                    .replace("+", "%20");
            HttpResponse<String> response = client.send(
                    request("/api/connections/" + encodedName)
                            .header("X-Reason", "TaskFlow live recovery test")
                            .DELETE()
                            .build(),
                    HttpResponse.BodyHandlers.ofString()
            );
            if (response.statusCode() != 204) {
                throw new IllegalStateException("RabbitMQ management connection close failed with HTTP "
                        + response.statusCode());
            }
        }

        private HttpRequest.Builder request(String path) {
            return HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .timeout(MANAGEMENT_REQUEST_TIMEOUT)
                    .header("Authorization", authorization);
        }

        private static JsonObject objectField(JsonObject object, String field) {
            JsonElement value = object.get(field);
            return value == null || !value.isJsonObject() ? null : value.getAsJsonObject();
        }

        private static String stringField(JsonObject object, String field) {
            JsonElement value = object.get(field);
            return value == null || value.isJsonNull() ? null : value.getAsString();
        }

        private static String value(String envKey, String fallback) {
            String value = System.getenv(envKey);
            return value == null || value.isBlank() ? fallback : value;
        }

        private static String trimTrailingSlash(String value) {
            String trimmed = value.strip();
            while (trimmed.endsWith("/")) {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }
            return trimmed;
        }
    }
}
