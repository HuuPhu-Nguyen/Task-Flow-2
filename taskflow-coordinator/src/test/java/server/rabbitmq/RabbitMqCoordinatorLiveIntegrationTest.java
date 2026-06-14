package server.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.scheduler.SchedulerConfig;
import server.scheduler.TaskScheduler;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;
import transport.rabbitmq.RabbitMqTopology;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RabbitMqCoordinatorLiveIntegrationTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";

    @Test
    void completesJobThroughLiveBrokerSchedulerAndPeerRoutes() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "it-" + UUID.randomUUID().toString().replace("-", "");
        String peerId = "peer-" + token;
        String jobId = "job-" + token;
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        Thread schedulerThread = null;
        try {
            cleanup(config, topology, peerId);

            try (RabbitMqTransport coordinatorTransport = new RabbitMqTransport(config);
                 RabbitMqTransport peerTransport = new RabbitMqTransport(config)) {
                coordinatorTransport.declareTopology();

                BlockingQueue<MessageEnvelope> schedulerMailbox = new LinkedBlockingQueue<>();
                InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
                TaskScheduler scheduler = new TaskScheduler(
                        schedulerMailbox,
                        registry,
                        null,
                        new RabbitMqSchedulerOutput(coordinatorTransport),
                        SchedulerConfig.defaults()
                );
                schedulerThread = new Thread(scheduler, "rabbitmq-coordinator-live-test-scheduler");
                schedulerThread.start();

                CountDownLatch heartbeatRegistered = new CountDownLatch(1);
                AtomicReference<Throwable> heartbeatFailure = new AtomicReference<>();
                coordinatorTransport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                    assertDelivery(heartbeatRegistered, heartbeatFailure,
                            () -> handleHeartbeat(registry, delivery, heartbeatRegistered));
                });
                coordinatorTransport.subscribe(TransportRoute.JOB_SUBMIT,
                        delivery -> enqueueForScheduler(schedulerMailbox, delivery));
                coordinatorTransport.subscribe(TransportRoute.TASK_RESULT,
                        delivery -> enqueueForScheduler(schedulerMailbox, delivery));

                CountDownLatch taskAssigned = new CountDownLatch(1);
                AtomicReference<TaskAssignMessage> assignment = new AtomicReference<>();
                AtomicReference<Throwable> assignmentFailure = new AtomicReference<>();
                peerTransport.subscribePeer(TransportRoute.TASK_ASSIGN, peerId, delivery -> {
                    assertDelivery(taskAssigned, assignmentFailure,
                            () -> captureAssignment(peerId, jobId, assignment, delivery));
                });

                CountDownLatch jobCompleted = new CountDownLatch(1);
                AtomicReference<JobResultMessage> jobResult = new AtomicReference<>();
                AtomicReference<Throwable> resultFailure = new AtomicReference<>();
                peerTransport.subscribePeer(TransportRoute.JOB_RESULT, peerId, delivery -> {
                    assertDelivery(jobCompleted, resultFailure,
                            () -> captureJobResult(jobId, jobResult, delivery));
                });

                peerTransport.publish(new OutboundTransportMessage(
                        TransportRoute.HEARTBEAT,
                        peerId,
                        new PongMessage(peerId, Instant.now().toString(), List.of(TestTaskPlugin.TASK_TYPE))
                ));
                awaitDelivery(heartbeatRegistered, heartbeatFailure, "peer heartbeat registration");

                peerTransport.publish(new OutboundTransportMessage(
                        TransportRoute.JOB_SUBMIT,
                        peerId,
                        new JobSubmitMessage(
                                peerId,
                                Instant.now().toString(),
                                jobId,
                                TestTaskPlugin.TASK_TYPE,
                                List.of("alpha"),
                                ""
                        )
                ));

                awaitDelivery(taskAssigned, assignmentFailure, "peer task assignment");
                TaskAssignMessage task = assignment.get();
                peerTransport.publish(new OutboundTransportMessage(
                        TransportRoute.TASK_RESULT,
                        peerId,
                        new TaskResultMessage(
                                peerId,
                                Instant.now().toString(),
                                task.getTaskId(),
                                task.getJobId(),
                                "processed-" + task.getPayload(),
                                true,
                                null
                        )
                ));

                awaitDelivery(jobCompleted, resultFailure, "peer job result");
                assertEquals(List.of("processed-alpha"), jobResult.get().getResultsByTaskId());
                assertQueueDrained(config, topology.queueName(TransportRoute.JOB_SUBMIT));
                assertQueueDrained(config, topology.queueName(TransportRoute.TASK_RESULT));
            }
        } finally {
            if (schedulerThread != null) {
                schedulerThread.interrupt();
                schedulerThread.join(2_000);
            }
            cleanup(config, topology, peerId);
        }
    }

    private static void enqueueForScheduler(BlockingQueue<MessageEnvelope> schedulerMailbox,
                                            InboundTransportMessage delivery) throws InterruptedException {
        if (delivery.acknowledgement() != null) {
            delivery.acknowledgement().defer();
        }
        schedulerMailbox.put(new MessageEnvelope(
                delivery.message(),
                delivery.fromNodeId(),
                delivery.acknowledgement()
        ));
    }

    private static void handleHeartbeat(InMemoryPeerRegistry registry,
                                        InboundTransportMessage delivery,
                                        CountDownLatch registered) {
        Message message = delivery.message();
        PongMessage heartbeat = assertInstanceOf(PongMessage.class, message);
        String peerNodeId = delivery.fromNodeId();
        if (peerNodeId == null || peerNodeId.isBlank()) {
            peerNodeId = heartbeat.getNodeId();
        }
        PeerInfo peer = registry.get(peerNodeId);
        if (peer == null) {
            registry.register(peerNodeId, new PeerInfo(
                    peerNodeId,
                    SchedulerConfig.defaults(),
                    heartbeat.getSupportedTaskTypes()
            ));
        } else {
            registry.updateHeartbeat(peerNodeId);
            peer.setSupportedTaskTypes(heartbeat.getSupportedTaskTypes());
        }
        registered.countDown();
    }

    private static void captureAssignment(String peerId,
                                          String jobId,
                                          AtomicReference<TaskAssignMessage> assignment,
                                          InboundTransportMessage delivery) {
        assertEquals(TransportRoute.TASK_ASSIGN, delivery.route());
        TaskAssignMessage message = assertInstanceOf(TaskAssignMessage.class, delivery.message());
        assertEquals(peerId, message.getNodeId());
        assertEquals(jobId, message.getJobId());
        assertEquals(TestTaskPlugin.TASK_TYPE, message.getTaskType());
        assertEquals("alpha", message.getPayload());
        assignment.set(message);
    }

    private static void captureJobResult(String jobId,
                                         AtomicReference<JobResultMessage> jobResult,
                                         InboundTransportMessage delivery) {
        assertEquals(TransportRoute.JOB_RESULT, delivery.route());
        assertEquals("COORDINATOR", delivery.fromNodeId());
        JobResultMessage message = assertInstanceOf(JobResultMessage.class, delivery.message());
        assertEquals(jobId, message.getJobId());
        assertEquals(TestTaskPlugin.TASK_TYPE, message.getTaskType());
        assertTrue(message.isSuccessful());
        jobResult.set(message);
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
        assertTrue(received.await(10, TimeUnit.SECONDS), "Timed out waiting for " + description);
        Throwable assertionError = failure.get();
        if (assertionError != null) {
            fail(assertionError);
        }
    }

    private static void assertQueueDrained(RabbitMqTransportConfig config, String queueName) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (messageCount(config, queueName) == 0) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(0, messageCount(config, queueName), "Expected RabbitMQ queue to drain: " + queueName);
    }

    private static long messageCount(RabbitMqTransportConfig config, String queueName) throws Exception {
        try (Connection connection = connectionFactory(config).newConnection("taskflow-rabbitmq-live-e2e-inspect");
             Channel channel = connection.createChannel()) {
            return channel.queueDeclarePassive(queueName).getMessageCount();
        }
    }

    @FunctionalInterface
    private interface CheckedAssertion {
        void run() throws Exception;
    }

    private static RabbitMqTransportConfig liveConfig(String token) {
        RabbitMqTransportConfig base = RabbitMqTransportConfig.fromEnvironment();
        String name = "taskflow.live.e2e." + token;
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
            deleteQueue(config, topology.peerQueueName(route, peerId));
        }
        deleteQueue(config, config.deadLetterQueueName());
        deleteExchange(config, config.exchangeName());
        deleteExchange(config, config.deadLetterExchangeName());
    }

    private static void deleteQueue(RabbitMqTransportConfig config, String queueName) {
        try (Connection connection = connectionFactory(config).newConnection("taskflow-rabbitmq-live-e2e-cleanup");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(queueName);
        } catch (Exception ignored) {
        }
    }

    private static void deleteExchange(RabbitMqTransportConfig config, String exchangeName) {
        try (Connection connection = connectionFactory(config).newConnection("taskflow-rabbitmq-live-e2e-cleanup");
             Channel channel = connection.createChannel()) {
            channel.exchangeDelete(exchangeName);
        } catch (Exception ignored) {
        }
    }
}
