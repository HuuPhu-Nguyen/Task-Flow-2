package server.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;
import transport.rabbitmq.RabbitMqRuntimeDefaults;
import transport.rabbitmq.RabbitMqTopology;

import java.nio.file.Path;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RabbitMqCoordinatorLiveIntegrationTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";

    @TempDir
    Path tempDir;

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
                CountDownLatch jobResultSendCompleted = new CountDownLatch(1);
                AtomicReference<Throwable> jobResultSendFailure = new AtomicReference<>();
                TaskScheduler scheduler = new TaskScheduler(
                        schedulerMailbox,
                        registry,
                        null,
                        new ObservedSchedulerOutput(
                                new RabbitMqSchedulerOutput(coordinatorTransport),
                                jobResultSendCompleted,
                                jobResultSendFailure
                        ),
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
                        delivery -> enqueueForScheduler(schedulerMailbox, delivery, null, null));
                CountDownLatch taskResultAcknowledged = new CountDownLatch(1);
                AtomicReference<Throwable> taskResultAckFailure = new AtomicReference<>();
                coordinatorTransport.subscribe(TransportRoute.TASK_RESULT,
                        delivery -> enqueueForScheduler(
                                schedulerMailbox,
                                delivery,
                                taskResultAcknowledged,
                                taskResultAckFailure
                        ));

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
                                "",
                                "token-" + jobId
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
                awaitDelivery(jobResultSendCompleted, jobResultSendFailure, "scheduler job-result send completion");
                awaitDelivery(taskResultAcknowledged, taskResultAckFailure, "scheduler task-result acknowledgement");
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

    @Test
    void replaysSeededPendingOutboxRowsThroughLiveBroker() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "outbox-seed-" + UUID.randomUUID().toString().replace("-", "");
        String peerId = "peer-" + token;
        String jobId = "job-" + token;
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);

        try {
            cleanup(config, topology, peerId);

            try (DatabaseManager db = new DatabaseManager(tempDir.resolve(token + ".db").toString());
                 RabbitMqTransport coordinatorTransport = new RabbitMqTransport(config);
                 RabbitMqTransport peerTransport = new RabbitMqTransport(config)) {
                coordinatorTransport.declareTopology();
                RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(coordinatorTransport);

                BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
                AtomicReference<Throwable> assignmentFailure = new AtomicReference<>();
                peerTransport.subscribePeer(TransportRoute.TASK_ASSIGN, peerId, delivery -> {
                    try {
                        assignments.add(captureAssignment(peerId, jobId, delivery));
                    } catch (Throwable e) {
                        assignmentFailure.set(e);
                        throw e;
                    }
                });

                BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();
                AtomicReference<Throwable> resultFailure = new AtomicReference<>();
                peerTransport.subscribePeer(TransportRoute.JOB_RESULT, peerId, delivery -> {
                    try {
                        results.add(captureJobResult(jobId, delivery));
                    } catch (Throwable e) {
                        resultFailure.set(e);
                        throw e;
                    }
                });

                TaskAssignMessage assignment = new TaskAssignMessage(
                        "COORDINATOR",
                        Instant.now().toString(),
                        "task-" + jobId + "-0",
                        jobId,
                        TestTaskPlugin.TASK_TYPE,
                        "alpha",
                        ""
                );
                JobResultMessage result = new JobResultMessage(
                        "COORDINATOR",
                        Instant.now().toString(),
                        jobId,
                        TestTaskPlugin.TASK_TYPE,
                        true,
                        List.of("processed-alpha")
                );

                assertTrue(db.enqueueBrokerOutbox(output.taskAssignmentOutboxMessage(
                        new PeerInfo(peerId, SchedulerConfig.defaults(), List.of(TestTaskPlugin.TASK_TYPE)),
                        assignment
                )).isPresent());
                assertTrue(db.enqueueBrokerOutbox(output.jobResultOutboxMessage(peerId, result)).isPresent());
                assertEquals(2, db.loadPendingBrokerOutbox(10).size());

                RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(db, output, 10, 1_000L);
                assertEquals(2, replayer.replayOnce());

                TaskAssignMessage deliveredAssignment = awaitQueue(assignments, assignmentFailure, "replayed task assignment");
                JobResultMessage deliveredResult = awaitQueue(results, resultFailure, "replayed job result");
                assertEquals(assignment.getTaskId(), deliveredAssignment.getTaskId());
                assertEquals(List.of("processed-alpha"), deliveredResult.getResultsByTaskId());
                assertEquals(List.of(), db.loadPendingBrokerOutbox(10));
            }
        } finally {
            cleanup(config, topology, peerId);
        }
    }

    @Test
    void replayedTaskAssignmentDoesNotCreateDuplicateAcceptedResults() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "outbox-dup-" + UUID.randomUUID().toString().replace("-", "");
        String peerId = "peer-" + token;
        String jobId = "job-" + token;
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);
        Thread schedulerThread = null;

        try {
            cleanup(config, topology, peerId);

            try (DatabaseManager db = new DatabaseManager(tempDir.resolve(token + ".db").toString());
                 RabbitMqTransport coordinatorTransport = new RabbitMqTransport(config);
                 RabbitMqTransport peerTransport = new RabbitMqTransport(config)) {
                coordinatorTransport.declareTopology();

                BlockingQueue<MessageEnvelope> schedulerMailbox = new LinkedBlockingQueue<>();
                InMemoryPeerRegistry registry = new InMemoryPeerRegistry(db);
                registry.register(peerId, new PeerInfo(
                        peerId,
                        SchedulerConfig.defaults(),
                        List.of(TestTaskPlugin.TASK_TYPE)
                ));

                CrashAfterTaskAssignmentPublishOutput output =
                        new CrashAfterTaskAssignmentPublishOutput(coordinatorTransport);
                TaskScheduler scheduler = new TaskScheduler(
                        schedulerMailbox,
                        registry,
                        db,
                        output,
                        SchedulerConfig.defaults()
                );
                schedulerThread = new Thread(scheduler, "rabbitmq-outbox-duplicate-live-test-scheduler");
                schedulerThread.start();

                CountDownLatch taskResultsAcknowledged = new CountDownLatch(2);
                AtomicReference<Throwable> taskResultAckFailure = new AtomicReference<>();
                coordinatorTransport.subscribe(TransportRoute.TASK_RESULT,
                        delivery -> enqueueForScheduler(
                                schedulerMailbox,
                                delivery,
                                taskResultsAcknowledged,
                                taskResultAckFailure
                        ));

                BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
                AtomicReference<Throwable> assignmentFailure = new AtomicReference<>();
                peerTransport.subscribePeer(TransportRoute.TASK_ASSIGN, peerId, delivery -> {
                    try {
                        assignments.add(captureAssignment(peerId, jobId, delivery));
                    } catch (Throwable e) {
                        assignmentFailure.set(e);
                        throw e;
                    }
                });

                BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();
                AtomicReference<Throwable> resultFailure = new AtomicReference<>();
                peerTransport.subscribePeer(TransportRoute.JOB_RESULT, peerId, delivery -> {
                    try {
                        results.add(captureJobResult(jobId, delivery));
                    } catch (Throwable e) {
                        resultFailure.set(e);
                        throw e;
                    }
                });

                schedulerMailbox.put(new MessageEnvelope(
                        new JobSubmitMessage(
                                peerId,
                                Instant.now().toString(),
                                jobId,
                                TestTaskPlugin.TASK_TYPE,
                                List.of("alpha"),
                                "",
                                "token-" + jobId
                        ),
                        peerId
                ));

                TaskAssignMessage firstAssignment =
                        awaitQueue(assignments, assignmentFailure, "initial task assignment");
                output.awaitTaskAssignmentPublishAttempt();
                List<BrokerOutboxStore.OutboxRecord> pendingAfterSimulatedCrash =
                        db.loadPendingBrokerOutbox(10);
                assertEquals(1, pendingAfterSimulatedCrash.size());
                assertEquals(TransportRoute.TASK_ASSIGN, pendingAfterSimulatedCrash.getFirst().message().route());
                assertEquals(1, output.taskAssignmentPublishAttempts());

                RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(
                        db,
                        new RabbitMqSchedulerOutput(coordinatorTransport),
                        10,
                        1_000L
                );
                assertEquals(1, replayer.replayOnce());
                TaskAssignMessage replayedAssignment =
                        awaitQueue(assignments, assignmentFailure, "replayed duplicate task assignment");
                assertEquals(firstAssignment.getTaskId(), replayedAssignment.getTaskId());
                assertEquals(List.of(), db.loadPendingBrokerOutbox(10));

                TaskResultMessage taskResult = new TaskResultMessage(
                        peerId,
                        Instant.now().toString(),
                        firstAssignment.getTaskId(),
                        firstAssignment.getJobId(),
                        "processed-" + firstAssignment.getPayload(),
                        true,
                        null
                );
                peerTransport.publish(new OutboundTransportMessage(TransportRoute.TASK_RESULT, peerId, taskResult));
                peerTransport.publish(new OutboundTransportMessage(TransportRoute.TASK_RESULT, peerId, taskResult));

                JobResultMessage finalResult = awaitQueue(results, resultFailure, "final job result");
                awaitDelivery(taskResultsAcknowledged, taskResultAckFailure, "duplicate task-result acknowledgements");
                assertEquals(List.of("processed-alpha"), finalResult.getResultsByTaskId());
                assertNull(results.poll(1, TimeUnit.SECONDS), "Duplicate task result produced a second final job result.");
                assertEquals(List.of(), db.loadPendingBrokerOutbox(10));

                List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts(jobId);
                assertEquals(1, attempts.size());
                assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, attempts.getFirst().outcome());
                List<DatabaseManager.JobRecord> matchingJobs = db.getJobHistory().stream()
                        .filter(job -> jobId.equals(job.jobId()))
                        .toList();
                assertEquals(1, matchingJobs.size());
                assertEquals("COMPLETED", matchingJobs.getFirst().status());
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
                                            InboundTransportMessage delivery,
                                            CountDownLatch acknowledged,
                                            AtomicReference<Throwable> acknowledgementFailure)
            throws InterruptedException {
        if (delivery.acknowledgement() != null) {
            delivery.acknowledgement().defer();
        }
        TransportAcknowledgement acknowledgement = delivery.acknowledgement();
        if (acknowledgement != null && acknowledged != null) {
            acknowledgement = new ObservedAcknowledgement(
                    acknowledgement,
                    acknowledged,
                    acknowledgementFailure
            );
        }
        schedulerMailbox.put(new MessageEnvelope(
                delivery.message(),
                delivery.fromNodeId(),
                acknowledgement
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
        assignment.set(captureAssignment(peerId, jobId, delivery));
    }

    private static TaskAssignMessage captureAssignment(String peerId,
                                                       String jobId,
                                                       InboundTransportMessage delivery) {
        assertEquals(TransportRoute.TASK_ASSIGN, delivery.route());
        TaskAssignMessage message = assertInstanceOf(TaskAssignMessage.class, delivery.message());
        assertEquals(peerId, message.getNodeId());
        assertEquals(jobId, message.getJobId());
        assertEquals(TestTaskPlugin.TASK_TYPE, message.getTaskType());
        assertEquals("alpha", message.getPayload());
        return message;
    }

    private static void captureJobResult(String jobId,
                                         AtomicReference<JobResultMessage> jobResult,
                                         InboundTransportMessage delivery) {
        jobResult.set(captureJobResult(jobId, delivery));
    }

    private static JobResultMessage captureJobResult(String jobId,
                                                     InboundTransportMessage delivery) {
        assertEquals(TransportRoute.JOB_RESULT, delivery.route());
        assertEquals(RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID, delivery.fromNodeId());
        JobResultMessage message = assertInstanceOf(JobResultMessage.class, delivery.message());
        assertEquals("COORDINATOR", message.getNodeId());
        assertEquals(jobId, message.getJobId());
        assertEquals(TestTaskPlugin.TASK_TYPE, message.getTaskType());
        assertTrue(message.isSuccessful());
        return message;
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

    private static <T> T awaitQueue(BlockingQueue<T> queue,
                                    AtomicReference<Throwable> failure,
                                    String description) throws InterruptedException {
        T item = queue.poll(10, TimeUnit.SECONDS);
        assertTrue(item != null, "Timed out waiting for " + description);
        Throwable assertionError = failure.get();
        if (assertionError != null) {
            fail(assertionError);
        }
        return item;
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

    private static class ObservedSchedulerOutput implements SchedulerOutput {
        private final SchedulerOutput delegate;
        private final CountDownLatch jobResultSendCompleted;
        private final AtomicReference<Throwable> jobResultSendFailure;

        private ObservedSchedulerOutput(SchedulerOutput delegate,
                                        CountDownLatch jobResultSendCompleted,
                                        AtomicReference<Throwable> jobResultSendFailure) {
            this.delegate = delegate;
            this.jobResultSendCompleted = jobResultSendCompleted;
            this.jobResultSendFailure = jobResultSendFailure;
        }

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) throws Exception {
            delegate.sendTask(peer, message);
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) throws Exception {
            try {
                boolean sent = delegate.sendJobResult(requesterNodeId, message);
                if (!sent) {
                    jobResultSendFailure.set(new AssertionError("RabbitMQ job-result publish returned false."));
                }
                return sent;
            } catch (Exception e) {
                jobResultSendFailure.set(e);
                throw e;
            } finally {
                jobResultSendCompleted.countDown();
            }
        }
    }

    private static class ObservedAcknowledgement implements TransportAcknowledgement {
        private final TransportAcknowledgement delegate;
        private final CountDownLatch settled;
        private final AtomicReference<Throwable> settlementFailure;

        private ObservedAcknowledgement(TransportAcknowledgement delegate,
                                        CountDownLatch settled,
                                        AtomicReference<Throwable> settlementFailure) {
            this.delegate = delegate;
            this.settled = settled;
            this.settlementFailure = settlementFailure;
        }

        @Override
        public void ack() throws Exception {
            settle(delegate::ack);
        }

        @Override
        public void requeue() throws Exception {
            settle(delegate::requeue);
        }

        @Override
        public void reject() throws Exception {
            settle(delegate::reject);
        }

        @Override
        public void defer() {
            delegate.defer();
        }

        private void settle(CheckedAssertion operation) throws Exception {
            try {
                operation.run();
            } catch (Exception e) {
                settlementFailure.set(e);
                throw e;
            } finally {
                settled.countDown();
            }
        }
    }

    private static class CrashAfterTaskAssignmentPublishOutput extends RabbitMqSchedulerOutput {
        private final CountDownLatch taskAssignmentPublishAttempted = new CountDownLatch(1);
        private int taskAssignmentPublishAttempts;

        private CrashAfterTaskAssignmentPublishOutput(RabbitMqTransport transport) {
            super(transport);
        }

        @Override
        public boolean publishOutbox(BrokerOutboxStore.OutboxRecord record) throws Exception {
            boolean published = super.publishOutbox(record);
            if (record.message().route() == TransportRoute.TASK_ASSIGN) {
                taskAssignmentPublishAttempts++;
                taskAssignmentPublishAttempted.countDown();
                return false;
            }
            return published;
        }

        private void awaitTaskAssignmentPublishAttempt() throws InterruptedException {
            assertTrue(taskAssignmentPublishAttempted.await(10, TimeUnit.SECONDS),
                    "Timed out waiting for simulated task-assignment publish attempt.");
        }

        private int taskAssignmentPublishAttempts() {
            return taskAssignmentPublishAttempts;
        }
    }
}
