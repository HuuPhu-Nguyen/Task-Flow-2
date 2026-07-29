package server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;
import protocol.AdmissionRejection;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.model.MessageEnvelope;
import server.overload.OverloadExperimentConfig;
import server.overload.OverloadMetrics;
import server.rabbitmq.RabbitMqOutboxReplayer;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.scheduler.BrokerOutboxPublisher;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.SchedulerMetrics;
import server.scheduler.SchedulerOutput;
import server.scheduler.SchedulerOverloadSnapshot;
import server.scheduler.TaskScheduler;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;
import transport.rabbitmq.RabbitMqTopology;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in TF-0709 overload experiment. Its name deliberately avoids
 * Surefire's default patterns; invoke it through verify-overload.ps1.
 */
public class OverloadExperiment {
    private static final DockerImageName RABBITMQ_IMAGE =
            DockerImageName.parse("rabbitmq:3.13-management");
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_ID = "overload-requester";
    private static final String EXECUTOR_ID = "overload-executor";
    private static final long CONDITION_PARK_NANOS =
            TimeUnit.MILLISECONDS.toNanos(10L);

    @Test
    void runConfiguredOverloadExperiment() throws Exception {
        OverloadExperimentConfig config =
                OverloadExperimentConfig.fromSystemProperties();
        boolean reportGrade = Boolean.getBoolean("taskflow.overload.reportGrade");
        if (reportGrade) {
            config.requireReportGrade();
        }
        Path outputDirectory = config.outputDirectory().toAbsolutePath();
        if (Files.exists(outputDirectory)) {
            throw new IllegalStateException(
                    "Overload output directory already exists: " + outputDirectory
            );
        }
        Files.createDirectories(outputDirectory);
        writeConfiguration(outputDirectory, config, reportGrade);

        RabbitMQContainer broker = new RabbitMQContainer(RABBITMQ_IMAGE)
                .withAdminUser("taskflow")
                .withAdminPassword("taskflow-overload");
        broker.start();
        try {
            runExperiment(outputDirectory, config, reportGrade, broker);
        } finally {
            broker.stop();
        }
    }

    private static void runExperiment(
            Path outputDirectory,
            OverloadExperimentConfig config,
            boolean reportGrade,
            RabbitMQContainer broker
    ) throws Exception {
        String token = UUID.randomUUID().toString().replace("-", "");
        RabbitMqTransportConfig transportConfig = transportConfig(broker, token);
        RabbitMqTopology topology = new RabbitMqTopology(transportConfig);
        Path databasePath = outputDirectory.resolve("overload.db");
        SchedulerConfig schedulerConfig = schedulerConfig(config);
        BlockingQueue<MessageEnvelope> mailbox =
                SchedulerMailbox.create(schedulerConfig);
        SchedulerMailbox.BrokerIngress ingress =
                SchedulerMailbox.brokerIngress(mailbox);
        AtomicReference<Throwable> asynchronousFailure = new AtomicReference<>();
        BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
        ConcurrentLinkedQueue<String> assignmentEvidence =
                new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> responseEvidence =
                new ConcurrentLinkedQueue<>();
        AtomicLong typedRejections = new AtomicLong();
        Set<String> completedResponses = ConcurrentHashMap.newKeySet();
        AtomicBoolean executorRunning = new AtomicBoolean();
        CountDownLatch submissionLaneSaturated = new CountDownLatch(1);
        CountDownLatch resultReserveOccupied = new CountDownLatch(1);

        TaskScheduler scheduler = null;
        Thread schedulerThread = null;
        Thread executorThread = null;
        RabbitMqOutboxReplayer replayer = null;
        DatabaseManager database =
                new DatabaseManager(databasePath.toString());
        RabbitMqTransport coordinatorTransport =
                new RabbitMqTransport(transportConfig);
        RabbitMqTransport requesterTransport =
                new RabbitMqTransport(transportConfig);
        try {
            coordinatorTransport.declareTopology();
            requesterTransport.subscribePeer(
                    TransportRoute.TASK_ASSIGN,
                    EXECUTOR_ID,
                    delivery -> {
                        try {
                            TaskAssignMessage assignment =
                                    (TaskAssignMessage) delivery.message();
                            assignments.add(assignment);
                            assignmentEvidence.add(String.join(",",
                                    assignment.getJobId(),
                                    assignment.getTaskId(),
                                    Integer.toString(assignment.getAttemptNumber()),
                                    assignment.getAssignmentId()));
                        } catch (Throwable failure) {
                            asynchronousFailure.compareAndSet(null, failure);
                        }
                    }
            );
            requesterTransport.subscribePeer(
                    TransportRoute.JOB_RESULT,
                    REQUESTER_ID,
                    delivery -> {
                        try {
                            JobResultMessage response =
                                    (JobResultMessage) delivery.message();
                            AdmissionRejection rejection =
                                    response.getAdmissionRejection();
                            if (rejection != null) {
                                typedRejections.incrementAndGet();
                                responseEvidence.add(String.join(",",
                                        response.getJobId(),
                                        "REJECTED",
                                        rejection.limit().name(),
                                        Long.toString(rejection.configuredMaximum()),
                                        Long.toString(rejection.observedValue())));
                            } else if (response.isSuccessful()) {
                                completedResponses.add(response.getJobId());
                                responseEvidence.add(String.join(",",
                                        response.getJobId(),
                                        "COMPLETED",
                                        "NONE",
                                        "0",
                                        "0"));
                            }
                        } catch (Throwable failure) {
                            asynchronousFailure.compareAndSet(null, failure);
                        }
                    }
            );

            InMemoryPeerRegistry registry = new InMemoryPeerRegistry(database);
            registry.register(EXECUTOR_ID, new PeerInfo(
                    EXECUTOR_ID,
                    schedulerConfig,
                    List.of(TASK_TYPE)
            ));
            GatedOutboxOutput output = new GatedOutboxOutput(
                    new RabbitMqSchedulerOutput(coordinatorTransport)
            );
            scheduler = new TaskScheduler(
                    mailbox,
                    registry,
                    database,
                    output,
                    schedulerConfig
            );
            TaskScheduler liveScheduler = scheduler;
            coordinatorTransport.subscribe(
                    TransportRoute.JOB_SUBMIT,
                    1,
                    delivery -> {
                        ingress.offer(delivery);
                        liveScheduler.refreshMailboxPressure();
                        if (SchedulerMailbox.depthSnapshot(mailbox)
                                .submissionDepth() == config.mailboxCapacity()) {
                            submissionLaneSaturated.countDown();
                        }
                    }
            );
            coordinatorTransport.subscribe(
                    TransportRoute.TASK_RESULT,
                    delivery -> {
                        ingress.offer(delivery);
                        liveScheduler.refreshMailboxPressure();
                        if (SchedulerMailbox.depthSnapshot(mailbox)
                                .taskResultDepth() == 1) {
                            resultReserveOccupied.countDown();
                        }
                    }
            );
            schedulerThread = new Thread(
                    scheduler,
                    "tf0709-overload-scheduler"
            );
            schedulerThread.start();

            publishSubmission(requesterTransport, "initial-expiry");
            publishSubmission(requesterTransport, "initial-result");
            TaskAssignMessage expiryAssignment = awaitAssignment(
                    assignments,
                    asynchronousFailure,
                    config,
                    "initial expiry assignment"
            );
            TaskAssignMessage resultAssignment = awaitAssignment(
                    assignments,
                    asynchronousFailure,
                    config,
                    "initial result assignment"
            );
            assertEquals("job-initial-expiry", expiryAssignment.getJobId());
            assertEquals("job-initial-result", resultAssignment.getJobId());

            output.blockNextPublication();
            publishSubmission(requesterTransport, "gate");
            assertTrue(
                    output.awaitBlocked(config.completionTimeoutSeconds()),
                    "Scheduler did not enter the controlled outbox publication boundary."
            );
            for (int index = 0;
                 index < config.maxPendingOutboxRows();
                 index++) {
                JobResultMessage seeded = new JobResultMessage(
                        "COORDINATOR",
                        Instant.now().toString(),
                        "job-seeded-outbox-" + index,
                        TASK_TYPE,
                        false,
                        List.of(),
                        "Seeded durable overload pressure."
                );
                assertTrue(database.enqueueBrokerOutbox(
                        output.jobResultOutboxMessage(REQUESTER_ID, seeded)
                ).isPresent());
            }
            scheduler.refreshPendingOutboxPressure(
                    config.maxPendingOutboxRows(),
                    true
            );

            int initialBurst = config.mailboxCapacity() + 32;
            for (int index = 0; index < initialBurst; index++) {
                publishSubmission(requesterTransport, "flood-0-" + index);
            }
            publishResult(requesterTransport, resultAssignment);
            assertTrue(
                    submissionLaneSaturated.await(
                            config.completionTimeoutSeconds(),
                            TimeUnit.SECONDS
                    ),
                    "Configured submission lane did not saturate."
            );
            assertTrue(
                    resultReserveOccupied.await(
                            config.completionTimeoutSeconds(),
                            TimeUnit.SECONDS
                    ),
                    "Accepted result did not occupy the fixed reserve."
            );
            awaitCondition(
                    () -> SchedulerMailbox.depthSnapshot(mailbox).submissionDepth()
                            == config.mailboxCapacity(),
                    config,
                    "full submission lane"
            );
            SchedulerMailbox.DepthSnapshot saturatedMailbox =
                    new SchedulerMailbox.DepthSnapshot(
                            config.mailboxCapacity(),
                            config.mailboxCapacity(),
                            1,
                            1,
                            false
                    );
            SchedulerOverloadSnapshot saturatedSnapshot =
                    scheduler.getOverloadSnapshot();
            assertTrue(reasonActive(
                    saturatedSnapshot,
                    SchedulerOverloadSnapshot.Reason.SUBMISSION_MAILBOX_CAPACITY
            ));
            long brokerQueueHighWater = brokerQueueDepth(
                    transportConfig,
                    topology.queueName(TransportRoute.JOB_SUBMIT)
            );
            assertTrue(brokerQueueHighWater > 0L,
                    "Submission burst did not remain observably broker-owned.");

            long leaseWaitMillis = Math.max(
                    1L,
                    expiryAssignment.getLeaseExpiresAtEpochMillis()
                            - System.currentTimeMillis()
                            + 50L
            );
            assertTrue(leaseWaitMillis <= config.taskLeaseMillis() + 1_000L);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(leaseWaitMillis));
            output.releaseBlockedOpen();
            executorRunning.set(true);
            executorThread = new Thread(
                    () -> executeAssignments(
                            requesterTransport,
                            assignments,
                            executorRunning,
                            asynchronousFailure
                    ),
                    "tf0709-overload-executor"
            );
            executorThread.start();

            awaitCondition(
                    () -> {
                        SchedulerMetrics.Snapshot snapshot =
                                liveScheduler.getMetricsSnapshot();
                        return snapshot.taskResultsCommittedTotal() >= 1L
                                && snapshot.taskLeaseExpirationsTotal() >= 1L;
                    },
                    config,
                    "accepted result and lease expiry progress under submission pressure"
            );
            long classifiedWhenProgressObserved =
                    liveScheduler.getMetricsSnapshot().jobsAcceptedTotal()
                            + typedRejections.get();
            assertTrue(
                    classifiedWhenProgressObserved < 3L + initialBurst,
                    "Result/expiry progress was observed only after pressure drained."
            );

            List<Long> heapSamples = new ArrayList<>(config.waveCount());
            long submittedFlood = initialBurst;
            long outboxHighWater = pendingOutbox(database);
            long activeJobsHighWater =
                    liveScheduler.getMetricsSnapshot().activeJobs();
            for (int wave = 0; wave < config.waveCount(); wave++) {
                int alreadyPublished = wave == 0 ? initialBurst : 0;
                for (int index = alreadyPublished;
                     index < config.submissionsPerWave();
                     index++) {
                    publishSubmission(
                            requesterTransport,
                            "flood-" + wave + "-" + index
                    );
                    submittedFlood++;
                }
                long expectedTotal = 3L + submittedFlood;
                awaitCondition(
                        () -> liveScheduler.getMetricsSnapshot().jobsAcceptedTotal()
                                + typedRejections.get() == expectedTotal,
                        config,
                        "classification of overload wave " + (wave + 1)
                );
                assertNoAsyncFailure(asynchronousFailure);
                outboxHighWater = Math.max(outboxHighWater, pendingOutbox(database));
                activeJobsHighWater = Math.max(
                        activeJobsHighWater,
                        liveScheduler.getMetricsSnapshot().activeJobs()
                );
                heapSamples.add(retainedHeapBytes());
            }
            assertEquals(config.totalFloodSubmissions(), submittedFlood);
            long acceptedBeforeRecovery =
                    liveScheduler.getMetricsSnapshot().jobsAcceptedTotal();
            assertEquals(
                    3L + config.totalFloodSubmissions(),
                    acceptedBeforeRecovery + typedRejections.get(),
                    "Every submitted job must be durably accepted or explicitly rejected."
            );
            assertTrue(typedRejections.get() > 0L);
            assertTrue(outboxHighWater >= config.maxPendingOutboxRows());
            SchedulerOverloadSnapshot outboxSnapshot =
                    liveScheduler.getOverloadSnapshot();
            assertTrue(reasonActive(
                    outboxSnapshot,
                    SchedulerOverloadSnapshot.Reason.MAX_PENDING_OUTBOX_ROWS
            ));

            long plateauSpan = OverloadMetrics.plateauSpan(heapSamples);
            long plateauMaximum = OverloadMetrics.plateauMaximum(heapSamples);
            assertTrue(plateauSpan <= config.heapPlateauSpanBytes(),
                    "Retained heap did not plateau: " + heapSamples);
            assertTrue(plateauMaximum < config.heapCeilingBytes(),
                    "Retained heap exceeded the fixed experiment ceiling: " + heapSamples);

            replayer = new RabbitMqOutboxReplayer(
                    database,
                    output,
                    schedulerConfig.schedulerOutboxBatchSize()
            );
            replayer.start();

            long acceptedTarget = acceptedBeforeRecovery;
            awaitCondition(
                    () -> liveScheduler.getMetricsSnapshot().jobsCompletedTotal()
                            == acceptedTarget
                            && pendingOutbox(database) == 0L,
                    config,
                    "all accepted work and durable outbox replay"
            );
            assertEquals(acceptedTarget, completedResponses.size());

            publishSubmission(requesterTransport, "after-pressure");
            awaitCondition(
                    () -> liveScheduler.getMetricsSnapshot().jobsAcceptedTotal()
                            == acceptedTarget + 1L
                            && liveScheduler.getMetricsSnapshot().jobsCompletedTotal()
                            == acceptedTarget + 1L
                            && completedResponses.contains("job-after-pressure"),
                    config,
                    "fresh admission and completion after overload recovery"
            );
            awaitCondition(
                    () -> !liveScheduler.getOverloadSnapshot().overloaded()
                            && pendingOutbox(database) == 0L,
                    config,
                    "observable overload recovery"
            );
            assertFalse(reportGrade && ManagementFactory.getMemoryMXBean()
                    .getHeapMemoryUsage().getMax() <= 0L);
            assertEquals(0L, brokerQueueDepth(
                    transportConfig,
                    topology.queueName(TransportRoute.JOB_SUBMIT)
            ));
            assertEquals(0L, brokerQueueDepth(
                    transportConfig,
                    topology.queueName(TransportRoute.TASK_RESULT)
            ));
            assertNoAsyncFailure(asynchronousFailure);

            DatabaseAudit audit = auditDatabase(databasePath);
            assertEquals(acceptedTarget + 1L, audit.jobCount());
            assertEquals(acceptedTarget + 1L, audit.completedJobs());
            assertEquals(acceptedTarget + 1L, audit.taskCount());
            assertEquals(acceptedTarget + 1L, audit.completedTasks());
            assertEquals(0L, audit.pendingOutboxRows());
            assertTrue(audit.publishedOutboxRows() >= audit.taskCount());
            assertTrue(audit.maximumAttemptNumber() >= 2L);
            assertEquals("ok", audit.integrity());

            writeEvidence(
                    outputDirectory,
                    config,
                    reportGrade,
                    heapSamples,
                    plateauSpan,
                    plateauMaximum,
                    saturatedMailbox,
                    brokerQueueHighWater,
                    outboxHighWater,
                    activeJobsHighWater,
                    acceptedTarget + 1L,
                    typedRejections.get(),
                    liveScheduler.getMetricsSnapshot(),
                    audit,
                    assignmentEvidence,
                    responseEvidence
            );
        } finally {
            executorRunning.set(false);
            if (executorThread != null) {
                executorThread.interrupt();
                executorThread.join(2_000L);
            }
            if (replayer != null) {
                replayer.close();
            }
            ingress.stopIntake();
            if (scheduler != null) {
                scheduler.requestShutdownAfterDrain();
            }
            if (schedulerThread != null) {
                schedulerThread.join(10_000L);
                if (schedulerThread.isAlive()) {
                    schedulerThread.interrupt();
                    schedulerThread.join(2_000L);
                }
            }
            requesterTransport.close();
            coordinatorTransport.close();
            database.close();
        }
    }

    private static void executeAssignments(
            RabbitMqTransport requesterTransport,
            BlockingQueue<TaskAssignMessage> assignments,
            AtomicBoolean running,
            AtomicReference<Throwable> failure
    ) {
        while (running.get() && failure.get() == null) {
            try {
                TaskAssignMessage assignment = assignments.poll(1L, TimeUnit.SECONDS);
                if (assignment != null) {
                    publishResult(requesterTransport, assignment);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
                return;
            }
        }
    }

    private static void publishSubmission(
            RabbitMqTransport requester,
            String suffix
    ) throws Exception {
        String jobId = "job-" + suffix;
        assertTrue(requester.publish(new OutboundTransportMessage(
                TransportRoute.JOB_SUBMIT,
                REQUESTER_ID,
                new JobSubmitMessage(
                        REQUESTER_ID,
                        Instant.now().toString(),
                        jobId,
                        TASK_TYPE,
                        List.of("payload-" + suffix),
                        "",
                        "token-" + jobId
                )
        )), "Submission was not publisher-confirmed: " + jobId);
    }

    private static void publishResult(
            RabbitMqTransport requester,
            TaskAssignMessage assignment
    ) throws Exception {
        assertTrue(requester.publish(new OutboundTransportMessage(
                TransportRoute.TASK_RESULT,
                EXECUTOR_ID,
                new TaskResultMessage(
                        EXECUTOR_ID,
                        Instant.now().toString(),
                        assignment.getTaskId(),
                        assignment.getJobId(),
                        assignment.getAttemptNumber(),
                        assignment.getAssignmentId(),
                        "completed-" + assignment.getJobId(),
                        true,
                        ""
                )
        )), "Task result was not publisher-confirmed: " + assignment.getTaskId());
    }

    private static TaskAssignMessage awaitAssignment(
            BlockingQueue<TaskAssignMessage> assignments,
            AtomicReference<Throwable> failure,
            OverloadExperimentConfig config,
            String description
    ) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(config.completionTimeoutSeconds());
        while (System.nanoTime() < deadline) {
            assertNoAsyncFailure(failure);
            TaskAssignMessage assignment = assignments.poll(100L, TimeUnit.MILLISECONDS);
            if (assignment != null) {
                return assignment;
            }
        }
        throw new IllegalStateException("Timed out waiting for " + description + ".");
    }

    private static void awaitCondition(
            Condition condition,
            OverloadExperimentConfig config,
            String description
    ) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(config.completionTimeoutSeconds());
        while (System.nanoTime() < deadline) {
            if (condition.evaluate()) {
                return;
            }
            LockSupport.parkNanos(CONDITION_PARK_NANOS);
        }
        throw new IllegalStateException("Timed out waiting for " + description + ".");
    }

    private static void assertNoAsyncFailure(AtomicReference<Throwable> failure) {
        Throwable observed = failure.get();
        if (observed != null) {
            throw new AssertionError("Asynchronous overload fixture failed.", observed);
        }
    }

    private static boolean reasonActive(
            SchedulerOverloadSnapshot snapshot,
            SchedulerOverloadSnapshot.Reason reason
    ) {
        return snapshot.reasons().stream().anyMatch(
                pressure -> pressure.reason() == reason
        );
    }

    private static long pendingOutbox(DatabaseManager database) {
        BrokerOutboxStore.PendingOutboxCount count =
                database.countPendingBrokerOutbox();
        if (!count.counted()) {
            throw new IllegalStateException("Pending outbox observation failed.");
        }
        return count.count();
    }

    private static long retainedHeapBytes() {
        System.gc();
        System.gc();
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        return memory.getHeapMemoryUsage().getUsed();
    }

    private static long brokerQueueDepth(
            RabbitMqTransportConfig config,
            String queueName
    ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        try (Connection connection = factory.newConnection("tf0709-depth-probe");
             Channel channel = connection.createChannel()) {
            return channel.queueDeclarePassive(queueName).getMessageCount();
        }
    }

    private static SchedulerConfig schedulerConfig(
            OverloadExperimentConfig config
    ) {
        return SchedulerConfig.fromEnvironment(Map.ofEntries(
                Map.entry("TASKFLOW_TASK_TIMEOUT_MS", "60000"),
                Map.entry("TASKFLOW_TASK_LEASE_MS",
                        Long.toString(config.taskLeaseMillis())),
                Map.entry("TASKFLOW_MAX_TASK_RETRIES", "4"),
                Map.entry("TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY",
                        Integer.toString(config.mailboxCapacity())),
                Map.entry("TASKFLOW_MAX_ACTIVE_JOBS",
                        Integer.toString(config.activeJobLimit())),
                Map.entry("TASKFLOW_MAX_ACTIVE_TASKS",
                        Integer.toString(config.activeJobLimit())),
                Map.entry("TASKFLOW_MAX_PENDING_OUTBOX_ROWS",
                        Integer.toString(config.maxPendingOutboxRows())),
                Map.entry("TASKFLOW_SCHEDULER_MESSAGE_BATCH_SIZE", "1"),
                Map.entry("TASKFLOW_SCHEDULER_DEADLINE_BATCH_SIZE", "4"),
                Map.entry("TASKFLOW_SCHEDULER_DISPATCH_BATCH_SIZE", "8"),
                Map.entry("TASKFLOW_SCHEDULER_OUTBOX_BATCH_SIZE", "8"),
                Map.entry("TASKFLOW_METRICS_LOG_INTERVAL_MS", "1000")
        ));
    }

    private static RabbitMqTransportConfig transportConfig(
            RabbitMQContainer broker,
            String token
    ) {
        String prefix = "taskflow.overload." + token;
        return new RabbitMqTransportConfig(
                broker.getHost(),
                broker.getAmqpPort(),
                broker.getAdminUsername(),
                broker.getAdminPassword(),
                "/",
                prefix + ".exchange",
                prefix,
                true,
                32,
                5_000L,
                true,
                prefix + ".dlx",
                prefix + ".dlq",
                "dead-letter",
                List.of(25L, 50L, 100L)
        );
    }

    private static DatabaseAudit auditDatabase(Path databasePath)
            throws Exception {
        try (var connection = DriverManager.getConnection(
                "jdbc:sqlite:" + databasePath
        );
             Statement statement = connection.createStatement()) {
            String integrity;
            try (ResultSet result =
                         statement.executeQuery("PRAGMA integrity_check")) {
                integrity = result.next() ? result.getString(1) : "";
            }
            return new DatabaseAudit(
                    queryLong(statement, "SELECT COUNT(*) FROM jobs"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM jobs WHERE status='COMPLETED'"),
                    queryLong(statement, "SELECT COUNT(*) FROM tasks"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM tasks WHERE status='COMPLETED'"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM broker_outbox "
                                    + "WHERE published_at IS NULL"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM broker_outbox "
                                    + "WHERE published_at IS NOT NULL"),
                    queryLong(statement,
                            "SELECT COALESCE(MAX(attempt_number),0) FROM tasks"),
                    integrity
            );
        }
    }

    private static long queryLong(Statement statement, String sql)
            throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getLong(1) : 0L;
        }
    }

    private static void writeConfiguration(
            Path output,
            OverloadExperimentConfig config,
            boolean reportGrade
    ) throws Exception {
        writeLines(output.resolve("configuration.properties"), List.of(
                "reportGrade=" + reportGrade,
                "waves=" + config.waveCount(),
                "submissionsPerWave=" + config.submissionsPerWave(),
                "totalFloodSubmissions=" + config.totalFloodSubmissions(),
                "mailboxCapacity=" + config.mailboxCapacity(),
                "activeJobLimit=" + config.activeJobLimit(),
                "maxPendingOutboxRows=" + config.maxPendingOutboxRows(),
                "taskLeaseMillis=" + config.taskLeaseMillis(),
                "completionTimeoutSeconds=" + config.completionTimeoutSeconds(),
                "heapPlateauSpanBytes=" + config.heapPlateauSpanBytes(),
                "heapCeilingBytes=" + config.heapCeilingBytes()
        ));
    }

    private static void writeEvidence(
            Path output,
            OverloadExperimentConfig config,
            boolean reportGrade,
            List<Long> heapSamples,
            long plateauSpan,
            long plateauMaximum,
            SchedulerMailbox.DepthSnapshot saturatedMailbox,
            long brokerQueueHighWater,
            long outboxHighWater,
            long activeJobsHighWater,
            long acceptedJobs,
            long typedRejections,
            SchedulerMetrics.Snapshot metrics,
            DatabaseAudit audit,
            ConcurrentLinkedQueue<String> assignments,
            ConcurrentLinkedQueue<String> responses
    ) throws Exception {
        long maxHeap = ManagementFactory.getMemoryMXBean()
                .getHeapMemoryUsage().getMax();
        writeLines(output.resolve("metrics.properties"), List.of(
                "reportGrade=" + reportGrade,
                "submittedJobs=" + (3L + config.totalFloodSubmissions() + 1L),
                "acceptedJobs=" + acceptedJobs,
                "typedRejections=" + typedRejections,
                "jobsCompleted=" + metrics.jobsCompletedTotal(),
                "taskResultsCommitted=" + metrics.taskResultsCommittedTotal(),
                "leaseExpirations=" + metrics.taskLeaseExpirationsTotal(),
                "mailboxSubmissionCapacity=" + saturatedMailbox.submissionCapacity(),
                "mailboxSubmissionHighWater=" + saturatedMailbox.submissionDepth(),
                "mailboxResultCapacity=" + saturatedMailbox.taskResultCapacity(),
                "mailboxResultHighWater=" + saturatedMailbox.taskResultDepth(),
                "brokerSubmissionQueueHighWater=" + brokerQueueHighWater,
                "outboxAdmissionThreshold=" + config.maxPendingOutboxRows(),
                "outboxPendingHighWater=" + outboxHighWater,
                "activeJobsHighWater=" + activeJobsHighWater,
                "heapMaximumBytes=" + maxHeap,
                "heapPlateauSpanBytes=" + plateauSpan,
                "heapPlateauMaximumBytes=" + plateauMaximum,
                "restartCount=0",
                "freshJobAcceptedAfterRecovery=true"
        ));
        List<String> heapLines = new ArrayList<>();
        heapLines.add("wave,retained_heap_bytes");
        for (int index = 0; index < heapSamples.size(); index++) {
            heapLines.add((index + 1) + "," + heapSamples.get(index));
        }
        writeLines(output.resolve("heap-samples.csv"), heapLines);
        List<String> assignmentLines = new ArrayList<>();
        assignmentLines.add("job_id,task_id,attempt_number,assignment_id");
        assignmentLines.addAll(assignments);
        writeLines(output.resolve("assignments.csv"), assignmentLines);
        List<String> responseLines = new ArrayList<>();
        responseLines.add("job_id,outcome,limit,configured_maximum,observed_value");
        responseLines.addAll(responses);
        writeLines(output.resolve("responses.csv"), responseLines);
        writeLines(output.resolve("audit.properties"), List.of(
                "databaseSchemaVersion=14",
                "databaseIntegrity=" + audit.integrity(),
                "durableJobs=" + audit.jobCount(),
                "durableCompletedJobs=" + audit.completedJobs(),
                "durableTasks=" + audit.taskCount(),
                "durableCompletedTasks=" + audit.completedTasks(),
                "pendingOutboxRows=" + audit.pendingOutboxRows(),
                "publishedOutboxRows=" + audit.publishedOutboxRows(),
                "maximumAttemptNumber=" + audit.maximumAttemptNumber(),
                "brokerSubmissionQueueFinal=0",
                "brokerResultQueueFinal=0"
        ));
    }

    private static void writeLines(Path path, List<String> lines)
            throws Exception {
        Files.write(
                path,
                lines,
                StandardCharsets.UTF_8
        );
    }

    @FunctionalInterface
    private interface Condition {
        boolean evaluate() throws Exception;
    }

    private record DatabaseAudit(
            long jobCount,
            long completedJobs,
            long taskCount,
            long completedTasks,
            long pendingOutboxRows,
            long publishedOutboxRows,
            long maximumAttemptNumber,
            String integrity
    ) {
    }

    private static final class GatedOutboxOutput
            implements SchedulerOutput, BrokerOutboxPublisher {
        private enum Mode {
            OPEN,
            BLOCK_NEXT
        }

        private final RabbitMqSchedulerOutput delegate;
        private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.OPEN);
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private GatedOutboxOutput(RabbitMqSchedulerOutput delegate) {
            this.delegate = delegate;
        }

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message)
                throws Exception {
            delegate.sendTask(peer, message);
        }

        @Override
        public boolean sendJobResult(
                String requesterNodeId,
                JobResultMessage message
        ) throws Exception {
            return delegate.sendJobResult(requesterNodeId, message);
        }

        @Override
        public BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(
                PeerInfo peer,
                TaskAssignMessage message
        ) {
            return delegate.taskAssignmentOutboxMessage(peer, message);
        }

        @Override
        public BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(
                String requesterNodeId,
                JobResultMessage message
        ) {
            return delegate.jobResultOutboxMessage(requesterNodeId, message);
        }

        @Override
        public boolean publishOutbox(BrokerOutboxStore.OutboxRecord record)
                throws Exception {
            Mode observed = mode.get();
            if (observed == Mode.BLOCK_NEXT
                    && mode.compareAndSet(Mode.BLOCK_NEXT, Mode.OPEN)) {
                blocked.countDown();
                if (!release.await(600L, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "Timed out at the controlled outbox boundary."
                    );
                }
                return delegate.publishOutbox(record);
            }
            return delegate.publishOutbox(record);
        }

        private void blockNextPublication() {
            assertTrue(mode.compareAndSet(Mode.OPEN, Mode.BLOCK_NEXT));
        }

        private boolean awaitBlocked(long timeoutSeconds)
                throws InterruptedException {
            return blocked.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        private void releaseBlockedOpen() {
            release.countDown();
        }

    }
}
