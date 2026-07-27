package transport.rabbitmq;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Network;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import transport.BrokerTransport;
import transport.BrokerTransportContractTest;
import transport.DeliveryDisposition;
import transport.OutboundTransportMessage;
import transport.TransportRoute;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * RabbitMQ/Testcontainers binding for the reusable broker contract.
 */
class RabbitMqBrokerContractTest extends BrokerTransportContractTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";
    private static final DockerImageName RABBITMQ_IMAGE =
            DockerImageName.parse("rabbitmq:3.13-management");
    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");
    private static final int TOXIPROXY_AMQP_PORT = 8_666;
    private static final int TOXIPROXY_MANAGEMENT_PORT = 8_667;

    @Override
    protected boolean contractEnabled() {
        return Boolean.getBoolean(LIVE_TEST_PROPERTY)
                || "true".equalsIgnoreCase(System.getenv(LIVE_TEST_ENV));
    }

    @Override
    protected BrokerHarness startHarness() throws Exception {
        return new RabbitMqHarness();
    }

    private static final class RabbitMqHarness implements BrokerHarness {
        private static final Duration BROKER_START_TIMEOUT =
                Duration.ofSeconds(45);

        private final Network network;
        private final RabbitMQContainer broker;
        private final ToxiproxyContainer toxiproxy;
        private final AtomicInteger sessionSequence = new AtomicInteger();
        private final ManagementApi management;
        private volatile boolean brokerRunning;

        private RabbitMqHarness() throws Exception {
            network = Network.newNetwork();
            broker = new RabbitMQContainer(RABBITMQ_IMAGE)
                    .withAdminUser("taskflow")
                    .withAdminPassword("taskflow-contract")
                    .withNetwork(network)
                    .withNetworkAliases("rabbitmq");
            toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                    .withNetwork(network);
            broker.start();
            brokerRunning = true;
            toxiproxy.start();
            ToxiproxyClient toxiproxyClient = new ToxiproxyClient(
                    toxiproxy.getHost(),
                    toxiproxy.getControlPort()
            );
            toxiproxyClient.createProxy(
                    "rabbitmq-amqp",
                    "0.0.0.0:" + TOXIPROXY_AMQP_PORT,
                    "rabbitmq:5672"
            );
            toxiproxyClient.createProxy(
                    "rabbitmq-management",
                    "0.0.0.0:" + TOXIPROXY_MANAGEMENT_PORT,
                    "rabbitmq:15672"
            );
            management = new ManagementApi(
                    "http://"
                            + toxiproxy.getHost()
                            + ":"
                            + toxiproxy.getMappedPort(
                                    TOXIPROXY_MANAGEMENT_PORT
                            ),
                    broker.getAdminUsername(),
                    broker.getAdminPassword()
            );
        }

        @Override
        public BrokerSession openSession(String scenario,
                                         List<Long> retryDelays) {
            String token = scenario
                    .toLowerCase(Locale.ROOT)
                    .replaceAll("[^a-z0-9]+", ".");
            String prefix = "taskflow.contract."
                    + sessionSequence.incrementAndGet()
                    + "."
                    + token;
            RabbitMqTransportConfig config = new RabbitMqTransportConfig(
                    toxiproxy.getHost(),
                    toxiproxy.getMappedPort(TOXIPROXY_AMQP_PORT),
                    broker.getAdminUsername(),
                    broker.getAdminPassword(),
                    "/",
                    prefix + ".exchange",
                    prefix,
                    true,
                    1,
                    3_000L,
                    true,
                    prefix + ".dead-letter.exchange",
                    prefix + ".dead-letter",
                    "dead-letter",
                    retryDelays
            );
            return new RabbitMqSession(config, management);
        }

        @Override
        public void stopBroker() {
            if (!brokerRunning) {
                return;
            }
            broker.getDockerClient()
                    .stopContainerCmd(broker.getContainerId())
                    .withTimeout(20)
                    .exec();
            brokerRunning = false;
        }

        @Override
        public void startBroker() throws Exception {
            if (brokerRunning) {
                return;
            }
            broker.getDockerClient()
                    .startContainerCmd(broker.getContainerId())
                    .exec();
            brokerRunning = true;
            awaitBrokerReady(BROKER_START_TIMEOUT);
        }

        private void awaitBrokerReady(Duration timeout) throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            Throwable lastFailure = null;
            while (System.nanoTime() < deadline) {
                var state = broker.getCurrentContainerInfo().getState();
                if (!Boolean.TRUE.equals(state.getRunning())) {
                    fail(
                            "Managed RabbitMQ exited during restart: status="
                                    + state.getStatus()
                                    + " exitCode="
                                    + state.getExitCodeLong()
                                    + " error="
                                    + state.getError()
                    );
                }
                try {
                    if (management.available()) {
                        return;
                    }
                } catch (Throwable unavailable) {
                    lastFailure = unavailable;
                }
                awaitTick();
            }
            fail(
                    "Timed out waiting for managed RabbitMQ restart",
                    lastFailure
            );
        }

        @Override
        public void close() {
            try {
                broker.stop();
            } finally {
                brokerRunning = false;
                try {
                    toxiproxy.stop();
                } finally {
                    network.close();
                }
            }
        }
    }

    private static final class RabbitMqSession implements BrokerSession {
        private static final RabbitMqRecoveryPolicy RECOVERY_POLICY =
                new RabbitMqRecoveryPolicy(1_000, 100L, 500L, 2.0D);

        private final RabbitMqTransportConfig config;
        private final RabbitMqTopology topology;
        private final ManagementApi management;

        private RabbitMqSession(RabbitMqTransportConfig config,
                                ManagementApi management) {
            this.config = config;
            this.topology = new RabbitMqTopology(config);
            this.management = management;
        }

        @Override
        public BrokerTransport openTransport() throws Exception {
            return new RabbitMqTransport(config, RECOVERY_POLICY);
        }

        @Override
        public int maxDeliveryAttempts() {
            return config.maxDeliveryAttempts();
        }

        @Override
        public void awaitQueueState(TransportRoute route,
                                    long ready,
                                    long unacknowledged,
                                    Duration timeout) throws Exception {
            awaitCondition(
                    timeout,
                    () -> {
                        JsonObject queue = management.queue(
                                topology.queueName(route)
                        );
                        return queue != null
                                && longField(queue, "messages_ready") == ready
                                && longField(
                                        queue,
                                        "messages_unacknowledged"
                                ) == unacknowledged;
                    },
                    "queue " + topology.queueName(route)
                            + " ready=" + ready
                            + " unacknowledged=" + unacknowledged
            );
        }

        @Override
        public long quarantineCount() throws Exception {
            JsonObject queue = management.queue(
                    topology.deadLetterQuarantineQueueName()
            );
            return queue == null ? 0L : longField(queue, "messages");
        }

        @Override
        public QuarantinedDelivery awaitSingleQuarantined(Duration timeout)
                throws Exception {
            awaitCondition(
                    timeout,
                    () -> quarantineCount() == 1L,
                    "one final quarantine delivery"
            );
            try (RabbitMqDlqClient client = new RabbitMqDlqClient(config)) {
                List<RabbitMqDlqMessage> messages =
                        client.inspectQuarantine(2);
                assertEquals(1, messages.size());
                RabbitMqDlqMessage message = messages.getFirst();
                assertNotNull(message.failureDisposition());
                return new QuarantinedDelivery(
                        message.deliveryAttempt(),
                        message.failureReason(),
                        DeliveryDisposition.valueOf(
                                message.failureDisposition()
                        ),
                        message.inferredRoute()
                );
            }
        }

        @Override
        public long redeliveryCount(BrokerTransport transport) {
            RabbitMqTransport rabbit = Assertions.assertInstanceOf(
                    RabbitMqTransport.class,
                    transport
            );
            return rabbit.metricsSnapshot().redeliveriesTotal();
        }

        @Override
        public void awaitTransportRecovered(BrokerTransport transport,
                                            Duration timeout)
                throws Exception {
            RabbitMqTransport rabbit = Assertions.assertInstanceOf(
                    RabbitMqTransport.class,
                    transport
            );
            awaitCondition(
                    timeout,
                    rabbit::connectionUsable,
                    "RabbitMQ connection and topology recovery"
            );
        }

        @Override
        public boolean publishEventually(BrokerTransport transport,
                                         OutboundTransportMessage message,
                                         Duration timeout)
                throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            Throwable lastFailure = null;
            while (System.nanoTime() < deadline) {
                try {
                    if (transport.publish(message)) {
                        return true;
                    }
                } catch (Throwable publishFailure) {
                    lastFailure = publishFailure;
                }
                awaitTick();
            }
            if (lastFailure != null) {
                fail(
                        "Recovered transport did not confirm publication",
                        lastFailure
                );
            }
            return false;
        }

        @Override
        public void assertSharedTopologyDurable() throws Exception {
            assertDurableExchange(topology.exchangeName());
            for (String queueName : topology.queueNames().values()) {
                assertDurableQueue(queueName);
            }
            assertDurableExchange(topology.deadLetterExchangeName());
            assertDurableQueue(topology.deadLetterQueueName());
            assertDurableQueue(topology.deadLetterQuarantineQueueName());
            for (int stage = 1;
                 stage <= topology.retryStageCount();
                 stage++) {
                assertDurableExchange(topology.retryExchangeName(stage));
                assertDurableQueue(topology.retryQueueName(stage));
            }
        }

        @Override
        public void assertPeerEndpointEphemeral(TransportRoute route,
                                                String peerNodeId)
                throws Exception {
            JsonObject queue = requireQueue(
                    topology.peerQueueName(route, peerNodeId)
            );
            assertFalse(booleanField(queue, "durable"));
            assertTrue(booleanField(queue, "auto_delete"));
            assertTrue(booleanField(queue, "exclusive"));
        }

        private void assertDurableExchange(String exchangeName)
                throws Exception {
            JsonObject exchange = management.exchange(exchangeName);
            assertNotNull(
                    exchange,
                    "Missing expected exchange " + exchangeName
            );
            assertTrue(
                    booleanField(exchange, "durable"),
                    "Exchange must be durable: " + exchangeName
            );
            assertFalse(
                    booleanField(exchange, "auto_delete"),
                    "Shared exchange must not auto-delete: " + exchangeName
            );
        }

        private void assertDurableQueue(String queueName) throws Exception {
            JsonObject queue = requireQueue(queueName);
            assertTrue(
                    booleanField(queue, "durable"),
                    "Queue must be durable: " + queueName
            );
            assertFalse(
                    booleanField(queue, "auto_delete"),
                    "Shared queue must not auto-delete: " + queueName
            );
        }

        private JsonObject requireQueue(String queueName) throws Exception {
            JsonObject queue = management.queue(queueName);
            assertNotNull(queue, "Missing expected queue " + queueName);
            return queue;
        }

        @Override
        public void close() {
            // Each contract method owns a unique namespace. The managed
            // container removes all resources once after the suite.
        }
    }

    private static final class ManagementApi {
        private static final Duration REQUEST_TIMEOUT =
                Duration.ofSeconds(3);

        private final String baseUrl;
        private final String authorization;
        private final HttpClient client;

        private ManagementApi(String baseUrl,
                              String username,
                              String password) {
            this.baseUrl = trimTrailingSlash(baseUrl);
            this.authorization = "Basic " + Base64.getEncoder().encodeToString(
                    (username + ":" + password)
                            .getBytes(StandardCharsets.UTF_8)
            );
            this.client = HttpClient.newBuilder()
                    .connectTimeout(REQUEST_TIMEOUT)
                    .build();
        }

        private boolean available() throws Exception {
            HttpResponse<String> response = send("/api/overview");
            return response.statusCode() == 200;
        }

        private JsonObject queue(String queueName) throws Exception {
            return resource("/api/queues/%2F/" + encode(queueName));
        }

        private JsonObject exchange(String exchangeName) throws Exception {
            return resource("/api/exchanges/%2F/" + encode(exchangeName));
        }

        private JsonObject resource(String path) throws Exception {
            HttpResponse<String> response = send(path);
            if (response.statusCode() == 404) {
                return null;
            }
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "RabbitMQ management request failed with HTTP "
                                + response.statusCode()
                                + " for "
                                + path
                );
            }
            return JsonParser.parseString(response.body()).getAsJsonObject();
        }

        private HttpResponse<String> send(String path) throws Exception {
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create(baseUrl + path)
                    )
                    .timeout(REQUEST_TIMEOUT)
                    .header("Authorization", authorization)
                    .GET()
                    .build();
            return client.send(
                    request,
                    HttpResponse.BodyHandlers.ofString()
            );
        }

        private static String encode(String value) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8)
                    .replace("+", "%20");
        }

        private static String trimTrailingSlash(String value) {
            String trimmed = value.strip();
            while (trimmed.endsWith("/")) {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }
            return trimmed;
        }
    }

    private static void awaitCondition(Duration timeout,
                                       CheckedCondition condition,
                                       String description) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.evaluate()) {
                    return;
                }
            } catch (Throwable unavailable) {
                lastFailure = unavailable;
            }
            awaitTick();
        }
        if (lastFailure == null) {
            fail("Timed out waiting for " + description);
        } else {
            fail("Timed out waiting for " + description, lastFailure);
        }
    }

    private static void awaitTick() throws InterruptedException {
        new CountDownLatch(1).await(50L, TimeUnit.MILLISECONDS);
    }

    private static long longField(JsonObject object, String name) {
        return object.has(name) && !object.get(name).isJsonNull()
                ? object.get(name).getAsLong()
                : 0L;
    }

    private static boolean booleanField(JsonObject object, String name) {
        return object.has(name)
                && !object.get(name).isJsonNull()
                && object.get(name).getAsBoolean();
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }
}
