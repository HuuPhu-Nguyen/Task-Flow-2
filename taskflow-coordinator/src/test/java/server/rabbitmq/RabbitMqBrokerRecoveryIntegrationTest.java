package server.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Network;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PeerDisconnectedMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.health.CoordinatorHealth;
import server.health.CoordinatorReadinessProbe;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.TaskScheduler;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRecoveryPolicy;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;
import transport.rabbitmq.RabbitMqTransportConnector;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RabbitMqBrokerRecoveryIntegrationTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";
    private static final DockerImageName RABBITMQ_IMAGE =
            DockerImageName.parse("rabbitmq:3.13-management");
    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");
    private static final int TOXIPROXY_AMQP_PORT = 8_666;

    @TempDir
    Path tempDir;

    @Test
    void unavailableStartupAndActiveBrokerRestartRecoverOutboxConsumersAndFencing() throws Exception {
        Assumptions.assumeTrue(
                liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV
                        + "=true to run managed RabbitMQ recovery tests."
        );

        String token = UUID.randomUUID().toString().replace("-", "");
        String peerId = "peer-recovery-" + token;
        String jobId = "job-recovery-" + token;
        Network network = Network.newNetwork();
        RabbitMQContainer broker = new RabbitMQContainer(RABBITMQ_IMAGE)
                .withAdminUser("taskflow")
                .withAdminPassword("taskflow-recovery")
                .withNetwork(network)
                .withNetworkAliases("rabbitmq");
        ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network);
        RabbitMqTransportConnector startupConnector = null;
        ExecutorService startupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "managed-rabbitmq-startup-connector");
            thread.setDaemon(true);
            return thread;
        });
        RabbitMqTransport coordinatorTransport = null;
        RabbitMqTransport peerTransport = null;
        DatabaseManager db = null;
        RabbitMqOutboxReplayer outboxReplayer = null;
        TaskScheduler scheduler = null;
        Thread schedulerThread = null;
        SchedulerMailbox.BrokerIngress brokerIngress = null;

        try {
            broker.start();
            toxiproxy.start();
            new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort())
                    .createProxy(
                            "rabbitmq-amqp",
                            "0.0.0.0:" + TOXIPROXY_AMQP_PORT,
                            "rabbitmq:5672"
                    );
            RabbitMqTransportConfig config = managedConfig(
                    toxiproxy.getHost(),
                    toxiproxy.getMappedPort(TOXIPROXY_AMQP_PORT),
                    broker.getAdminUsername(),
                    broker.getAdminPassword(),
                    token
            );
            RabbitMqRecoveryPolicy recoveryPolicy =
                    new RabbitMqRecoveryPolicy(750, 250L, 1_000L, 2.0D);

            stopBroker(broker);
            startupConnector = new RabbitMqTransportConnector(config, recoveryPolicy);
            RabbitMqTransportConnector connector = startupConnector;
            Future<RabbitMqTransport> startup =
                    startupExecutor.submit(connector::connect);

            awaitCondition(
                    () -> connector.attempts() >= 2,
                    5_000L,
                    "at least two bounded initial connection attempts"
            );
            assertFalse(startup.isDone(), "Coordinator startup exited while RabbitMQ was unavailable.");
            assertTrue(
                    connector.state() == RabbitMqTransportConnector.State.CONNECTING
                            || connector.state() == RabbitMqTransportConnector.State.WAITING_TO_RETRY,
                    "Unexpected startup recovery state: " + connector.state()
            );

            startBroker(broker, config);
            coordinatorTransport = startup.get(30, TimeUnit.SECONDS);
            assertEquals(RabbitMqTransportConnector.State.CONNECTED, connector.state());
            assertTrue(connector.attempts() >= 2);
            assertSame(
                    coordinatorTransport,
                    connector.releaseTransportOwnership(),
                    "Runtime shutdown must take ownership after startup recovery."
            );

            coordinatorTransport.declareTopology();
            peerTransport = new RabbitMqTransport(config, recoveryPolicy);
            db = new DatabaseManager(tempDir.resolve("broker-recovery.db").toString());
            DatabaseManager database = db;

            SchedulerConfig schedulerConfig = recoverySchedulerConfig();
            BlockingQueue<MessageEnvelope> schedulerMailbox = SchedulerMailbox.create(schedulerConfig);
            brokerIngress = SchedulerMailbox.brokerIngress(schedulerMailbox);
            InMemoryPeerRegistry registry = new InMemoryPeerRegistry(db);
            registry.register(peerId, new PeerInfo(
                    peerId,
                    schedulerConfig,
                    List.of(TestTaskPlugin.TASK_TYPE)
            ));

            RabbitMqSchedulerOutput schedulerOutput =
                    new RabbitMqSchedulerOutput(coordinatorTransport);
            scheduler = new TaskScheduler(
                    schedulerMailbox,
                    registry,
                    db,
                    schedulerOutput,
                    schedulerConfig
            );
            schedulerThread = new Thread(scheduler, "managed-broker-recovery-scheduler");

            SchedulerMailbox.BrokerIngress ingress = brokerIngress;
            coordinatorTransport.subscribe(
                    TransportRoute.JOB_SUBMIT,
                    delivery -> ingress.offer(delivery)
            );
            coordinatorTransport.subscribe(
                    TransportRoute.TASK_RESULT,
                    delivery -> ingress.offer(delivery)
            );

            BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
            BlockingQueue<JobResultMessage> jobResults = new LinkedBlockingQueue<>();
            AtomicReference<Throwable> peerDeliveryFailure = new AtomicReference<>();
            peerTransport.subscribePeer(
                    TransportRoute.TASK_ASSIGN,
                    peerId,
                    delivery -> captureAssignment(
                            delivery,
                            peerId,
                            jobId,
                            assignments,
                            peerDeliveryFailure
                    )
            );
            peerTransport.subscribePeer(
                    TransportRoute.JOB_RESULT,
                    peerId,
                    delivery -> captureJobResult(
                            delivery,
                            jobId,
                            jobResults,
                            peerDeliveryFailure
                    )
            );

            outboxReplayer = new RabbitMqOutboxReplayer(db, schedulerOutput, 10, 1_000L);
            CoordinatorHealth coordinatorHealth = new CoordinatorHealth();
            Thread runningSchedulerThread = schedulerThread;
            coordinatorHealth.activate(
                    runningSchedulerThread::isAlive,
                    new CoordinatorReadinessProbe(
                            db,
                            coordinatorTransport,
                            scheduler,
                            registry,
                            schedulerConfig
                    )
            );
            schedulerThread.start();
            outboxReplayer.start();
            awaitCondition(
                    () -> coordinatorHealth.readiness().ready(),
                    5_000L,
                    "initial coordinator readiness"
            );

            assertTrue(peerTransport.publish(new OutboundTransportMessage(
                    TransportRoute.JOB_SUBMIT,
                    peerId,
                    new JobSubmitMessage(
                            peerId,
                            Instant.now().toString(),
                            jobId,
                            TestTaskPlugin.TASK_TYPE,
                            List.of("alpha"),
                            "",
                            "token-" + jobId
                    )
            )));
            TaskAssignMessage first = awaitQueue(
                    assignments,
                    peerDeliveryFailure,
                    15_000L,
                    "pre-outage task assignment"
            );
            awaitCondition(
                    () -> database.loadPendingBrokerOutbox(10).isEmpty(),
                    5_000L,
                    "confirmed pre-outage assignment outbox mark"
            );
            assertEquals(1, first.getAttemptNumber());

            stopBroker(broker);
            awaitCondition(
                    () -> coordinatorHealth.liveness().live()
                            && !coordinatorHealth.readiness().ready()
                            && coordinatorHealth.readiness().reasons().contains(
                                    CoordinatorHealth.Reason.BROKER_NOT_USABLE
                            ),
                    10_000L,
                    "live-but-unready coordinator during broker outage"
            );
            schedulerMailbox.put(new MessageEnvelope(
                    new PeerDisconnectedMessage(
                            peerId,
                            Instant.now().toString(),
                            "broker_outage_worker_unavailable"
                    ),
                    peerId
            ));

            awaitCondition(
                    () -> {
                        List<JobStateStore.TaskAttemptRecord> attempts =
                                database.loadTaskAttempts(jobId);
                        return attempts.size() == 2
                                && attempts.get(1).outcome()
                                == JobStateStore.TaskAttemptOutcome.RUNNING
                                && !database.loadPendingBrokerOutbox(10).isEmpty();
                    },
                    10_000L,
                    "durable replacement assignment and pending outbox during outage"
            );

            List<JobStateStore.TaskAttemptRecord> outageAttempts = db.loadTaskAttempts(jobId);
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    outageAttempts.getFirst().outcome());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING,
                    outageAttempts.get(1).outcome());
            assertEquals(first.getAttemptNumber(), outageAttempts.getFirst().attemptNumber());
            assertEquals(first.getAssignmentId(), outageAttempts.getFirst().assignmentId());
            assertNotEquals(first.getAssignmentId(), outageAttempts.get(1).assignmentId());

            DatabaseManager.TaskRecord duringOutage = db.getTasksForJob(jobId).getFirst();
            assertEquals("ASSIGNED", duringOutage.status());
            assertEquals(2, duringOutage.attemptNumber());
            assertEquals(outageAttempts.get(1).assignmentId(), duringOutage.assignmentId());
            assertEquals("RUNNING", jobStatus(db, jobId));
            assertNull(db.loadCompletedJobResult(jobId).orElse(null));

            List<BrokerOutboxStore.OutboxRecord> pendingDuringOutage =
                    db.loadPendingBrokerOutbox(10);
            assertEquals(1, pendingDuringOutage.size());
            BrokerOutboxStore.OutboxRecord replacementOutbox = pendingDuringOutage.getFirst();
            assertEquals(TransportRoute.TASK_ASSIGN, replacementOutbox.message().route());
            TaskAssignMessage durableReplacement = assertInstanceOf(
                    TaskAssignMessage.class,
                    replacementOutbox.message().message()
            );
            assertEquals(2, durableReplacement.getAttemptNumber());
            assertEquals(duringOutage.assignmentId(), durableReplacement.getAssignmentId());
            assertTrue(replacementOutbox.attemptCount() >= 1);
            assertNull(jobResults.poll(250L, TimeUnit.MILLISECONDS));

            startBroker(broker, config);
            TaskAssignMessage current = awaitAssignmentGeneration(
                    assignments,
                    peerDeliveryFailure,
                    first,
                    2,
                    30_000L,
                    "outbox replay through recovered peer consumer"
            );
            assertEquals(2, current.getAttemptNumber());
            assertEquals(durableReplacement.getAssignmentId(), current.getAssignmentId());
            awaitCondition(
                    () -> database.loadPendingBrokerOutbox(10).isEmpty(),
                    10_000L,
                    "recovered outbox drain"
            );
            awaitCondition(
                    () -> coordinatorHealth.readiness().ready(),
                    10_000L,
                    "automatic coordinator readiness recovery"
            );

            long successesBeforeStale = scheduler.getMetricsSnapshot().successCount();
            assertTrue(peerTransport.publish(taskResult(peerId, first, "obsolete-result")));
            TaskScheduler runningScheduler = scheduler;
            awaitCondition(
                    () -> runningScheduler.getMetricsSnapshot().staleResultCount() == 1L,
                    10_000L,
                    "stale pre-outage result classification"
            );

            DatabaseManager.TaskRecord afterStale = db.getTasksForJob(jobId).getFirst();
            assertEquals("ASSIGNED", afterStale.status());
            assertEquals(current.getAttemptNumber(), afterStale.attemptNumber());
            assertEquals(current.getAssignmentId(), afterStale.assignmentId());
            assertEquals(successesBeforeStale, scheduler.getMetricsSnapshot().successCount());
            assertEquals("RUNNING", jobStatus(db, jobId));
            assertNull(db.loadCompletedJobResult(jobId).orElse(null));
            assertNull(jobResults.poll(250L, TimeUnit.MILLISECONDS));

            assertTrue(peerTransport.publish(taskResult(peerId, current, "current-result")));
            JobResultMessage finalResult = awaitQueue(
                    jobResults,
                    peerDeliveryFailure,
                    20_000L,
                    "final result through recovered requester consumer"
            );
            assertEquals(List.of("current-result"), finalResult.getResultsByTaskId());

            awaitCondition(
                    () -> "COMPLETED".equals(jobStatus(database, jobId))
                            && database.loadPendingBrokerOutbox(10).isEmpty(),
                    10_000L,
                    "durable completion and final-result outbox drain"
            );
            assertEquals("COMPLETED", db.getTasksForJob(jobId).getFirst().status());
            assertEquals(
                    List.of("current-result"),
                    db.loadCompletedJobResult(jobId).orElseThrow().resultsByTaskId()
            );
            List<JobStateStore.TaskAttemptRecord> finalAttempts = db.loadTaskAttempts(jobId);
            assertEquals(2, finalAttempts.size());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    finalAttempts.getFirst().outcome());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED,
                    finalAttempts.get(1).outcome());
            assertEquals(1L, finalAttempts.stream()
                    .filter(attempt -> attempt.outcome()
                            == JobStateStore.TaskAttemptOutcome.SUCCEEDED)
                    .count());
            assertEquals(1L, scheduler.getMetricsSnapshot().successCount());
            assertEquals(1L, scheduler.getMetricsSnapshot().staleResultCount());
            assertNull(jobResults.poll(1L, TimeUnit.SECONDS),
                    "Broker recovery produced a duplicate authoritative final result.");
        } finally {
            if (brokerIngress != null) {
                brokerIngress.stopIntake();
            }
            closeQuietly(outboxReplayer);
            if (scheduler != null) {
                scheduler.requestShutdownAfterDrain();
            }
            if (schedulerThread != null) {
                schedulerThread.join(5_000L);
                if (schedulerThread.isAlive()) {
                    schedulerThread.interrupt();
                    schedulerThread.join(1_000L);
                }
            }
            closeQuietly(peerTransport);
            closeQuietly(coordinatorTransport);
            closeQuietly(db);
            closeQuietly(startupConnector);
            startupExecutor.shutdownNow();
            startupExecutor.awaitTermination(2L, TimeUnit.SECONDS);
            closeQuietly(broker);
            closeQuietly(toxiproxy);
            closeQuietly(network);
        }
    }

    private static OutboundTransportMessage taskResult(String peerId,
                                                       TaskAssignMessage assignment,
                                                       String payload) {
        return new OutboundTransportMessage(
                TransportRoute.TASK_RESULT,
                peerId,
                new TaskResultMessage(
                        peerId,
                        Instant.now().toString(),
                        assignment.getTaskId(),
                        assignment.getJobId(),
                        assignment.getAttemptNumber(),
                        assignment.getAssignmentId(),
                        payload,
                        true,
                        null
                )
        );
    }

    private static void captureAssignment(InboundTransportMessage delivery,
                                          String peerId,
                                          String jobId,
                                          BlockingQueue<TaskAssignMessage> assignments,
                                          AtomicReference<Throwable> failure) {
        try {
            TaskAssignMessage assignment =
                    assertInstanceOf(TaskAssignMessage.class, delivery.message());
            assertEquals(TransportRoute.TASK_ASSIGN, delivery.route());
            assertEquals(peerId, assignment.getNodeId());
            assertEquals(jobId, assignment.getJobId());
            assertEquals(TestTaskPlugin.TASK_TYPE, assignment.getTaskType());
            assertEquals("alpha", assignment.getPayload());
            assignments.add(assignment);
        } catch (Throwable error) {
            failure.compareAndSet(null, error);
            throw error;
        }
    }

    private static void captureJobResult(InboundTransportMessage delivery,
                                         String jobId,
                                         BlockingQueue<JobResultMessage> results,
                                         AtomicReference<Throwable> failure) {
        try {
            JobResultMessage result =
                    assertInstanceOf(JobResultMessage.class, delivery.message());
            assertEquals(TransportRoute.JOB_RESULT, delivery.route());
            assertEquals(jobId, result.getJobId());
            assertTrue(result.isSuccessful());
            results.add(result);
        } catch (Throwable error) {
            failure.compareAndSet(null, error);
            throw error;
        }
    }

    private static <T> T awaitQueue(BlockingQueue<T> queue,
                                    AtomicReference<Throwable> failure,
                                    long timeoutMillis,
                                    String description) throws InterruptedException {
        T item = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        Throwable deliveryFailure = failure.get();
        if (deliveryFailure != null) {
            fail(deliveryFailure);
        }
        assertNotNull(item, "Timed out waiting for " + description);
        return item;
    }

    private static TaskAssignMessage awaitAssignmentGeneration(
            BlockingQueue<TaskAssignMessage> assignments,
            AtomicReference<Throwable> failure,
            TaskAssignMessage prior,
            int expectedAttempt,
            long timeoutMillis,
            String description
    ) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            long remainingNanos = deadline - System.nanoTime();
            TaskAssignMessage candidate = assignments.poll(remainingNanos, TimeUnit.NANOSECONDS);
            Throwable deliveryFailure = failure.get();
            if (deliveryFailure != null) {
                fail(deliveryFailure);
            }
            if (candidate == null) {
                break;
            }
            if (candidate.getAttemptNumber() == expectedAttempt) {
                return candidate;
            }
            assertEquals(prior.getAttemptNumber(), candidate.getAttemptNumber(),
                    "Unexpected assignment generation while awaiting recovery.");
            assertEquals(prior.getAssignmentId(), candidate.getAssignmentId(),
                    "At-least-once replay changed the prior assignment identity.");
        }
        fail("Timed out waiting for " + description);
        return null;
    }

    private static void awaitCondition(CheckedCondition condition,
                                       long timeoutMillis,
                                       String description) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.evaluate()) {
                return;
            }
            Thread.sleep(50L);
        }
        assertTrue(condition.evaluate(), "Timed out waiting for " + description);
    }

    private static String jobStatus(DatabaseManager db, String jobId) {
        return db.getJobHistory().stream()
                .filter(job -> jobId.equals(job.jobId()))
                .findFirst()
                .orElseThrow()
                .status();
    }

    private static SchedulerConfig recoverySchedulerConfig() {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        return new SchedulerConfig(
                60_000L,
                60_000L,
                3,
                100,
                20,
                60_000L,
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );
    }

    private static RabbitMqTransportConfig managedConfig(String host,
                                                         int port,
                                                         String username,
                                                         String password,
                                                         String token) {
        String name = "taskflow.managed.recovery." + token;
        return new RabbitMqTransportConfig(
                host,
                port,
                username,
                password,
                "/",
                name + ".exchange",
                name,
                true,
                1,
                1_000L,
                true,
                name + ".dlx",
                name + ".dlq",
                "dead-letter",
                List.of(100L, 250L)
        );
    }

    private static void stopBroker(RabbitMQContainer broker) {
        broker.getDockerClient()
                .stopContainerCmd(broker.getContainerId())
                .withTimeout(20)
                .exec();
    }

    private static void startBroker(RabbitMQContainer broker,
                                    RabbitMqTransportConfig config) throws Exception {
        broker.getDockerClient()
                .startContainerCmd(broker.getContainerId())
                .exec();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45L);
        Exception lastConnectionFailure = null;
        while (System.nanoTime() < deadline) {
            var state = broker.getCurrentContainerInfo().getState();
            if (!Boolean.TRUE.equals(state.getRunning())) {
                fail(
                        "Managed RabbitMQ container exited during restart: status="
                                + state.getStatus()
                                + " exitCode="
                                + state.getExitCodeLong()
                                + " error="
                                + state.getError()
                                + System.lineSeparator()
                                + tail(broker.getLogs(), 6_000)
                );
            }
            try {
                openReadinessConnection(config);
                return;
            } catch (Exception unavailable) {
                lastConnectionFailure = unavailable;
            }
            Thread.sleep(100L);
        }
        fail(
                "Timed out waiting for managed RabbitMQ broker restart; last connection error="
                        + (lastConnectionFailure == null
                        ? "none"
                        : lastConnectionFailure.getClass().getSimpleName()
                        + ": "
                        + lastConnectionFailure.getMessage())
                        + System.lineSeparator()
                        + tail(broker.getLogs(), 6_000)
        );
    }

    private static void openReadinessConnection(RabbitMqTransportConfig config) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(false);
        factory.setConnectionTimeout(250);
        factory.setHandshakeTimeout(500);
        try (Connection ignored = factory.newConnection("taskflow-managed-recovery-readiness")) {
        }
    }

    private static String tail(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(value.length() - maxLength);
    }

    private static boolean liveTestEnabled() {
        if (Boolean.getBoolean(LIVE_TEST_PROPERTY)) {
            return true;
        }
        return "true".equalsIgnoreCase(System.getenv(LIVE_TEST_ENV));
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }
}
