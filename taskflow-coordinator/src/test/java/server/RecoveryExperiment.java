package server;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStores;
import objectstore.TaskFlowObjectKeys;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.monitor.PeerLivenessMonitor;
import server.objectstore.RecoveryOrphanProbe;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.rabbitmq.RecoveryOutboxProbe;
import server.recovery.RecoveryExperimentConfig;
import server.recovery.RecoveryMetrics;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.AssignmentIdGenerator;
import server.runtime.SystemTaskFlowClock;
import server.runtime.TaskFlowClock;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRecoveryPolicy;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in TF-0708 recovery experiment. Its name deliberately avoids Surefire's
 * default test patterns; invoke it through {@code verify-recovery.ps1}.
 */
public class RecoveryExperiment {
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_ID = "recovery-requester";
    private static final String EXECUTOR_ID = "recovery-executor";
    private static final String COORDINATOR_ID = "COORDINATOR_recovery";
    private static final String MINIO_IMAGE =
            "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    private static final DockerImageName RABBITMQ_IMAGE =
            DockerImageName.parse("rabbitmq:3.13-management");
    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");
    private static final int TOXIPROXY_AMQP_PORT = 8_666;
    private static final RabbitMqRecoveryPolicy RECOVERY_POLICY =
            new RabbitMqRecoveryPolicy(1_000, 100L, 1_000L, 2.0D);
    private static final long CONDITION_PARK_NANOS =
            TimeUnit.MILLISECONDS.toNanos(25L);

    @Test
    void runConfiguredRecoveryExperiment() throws Exception {
        RecoveryExperimentConfig config =
                RecoveryExperimentConfig.fromSystemProperties();
        boolean reportGrade = Boolean.getBoolean(
                "taskflow.recovery.reportGrade"
        );
        if (reportGrade) {
            config.requireReportGrade();
        }
        Path outputDirectory = config.outputDirectory().toAbsolutePath();
        if (Files.exists(outputDirectory)) {
            throw new IllegalStateException(
                    "Recovery output directory already exists: "
                            + outputDirectory
            );
        }
        Files.createDirectories(outputDirectory);
        writeConfiguration(outputDirectory, config, reportGrade);

        WorkerFailureMeasurement workerFailure =
                measureWorkerFailureDetection(config);
        LeaseMeasurement lease = measureLeaseReassignment(
                outputDirectory,
                config
        );
        PersistedRecoveryMeasurement coordinatorRestart =
                measurePersistedRecovery(
                        outputDirectory,
                        config,
                        "coordinator-restart",
                        config.coordinatorRestartTaskCount()
                );
        PersistedRecoveryMeasurement persistedSmall =
                measurePersistedRecovery(
                        outputDirectory,
                        config,
                        "persisted-" + config.smallPersistedTaskCount(),
                        config.smallPersistedTaskCount()
                );
        PersistedRecoveryMeasurement persistedLarge =
                measurePersistedRecovery(
                        outputDirectory,
                        config,
                        "persisted-" + config.largePersistedTaskCount(),
                        config.largePersistedTaskCount()
                );
        BrokerMeasurements broker = measureBrokerRecoveryAndReplay(
                outputDirectory,
                config
        );
        OrphanMeasurement orphan = measureOrphanCleanup(
                outputDirectory,
                config
        );

        writeMetrics(
                outputDirectory,
                reportGrade,
                workerFailure,
                lease,
                coordinatorRestart,
                persistedSmall,
                persistedLarge,
                broker,
                orphan
        );
        writeAudit(
                outputDirectory,
                lease,
                coordinatorRestart,
                persistedSmall,
                persistedLarge,
                broker,
                orphan
        );
    }

    private static WorkerFailureMeasurement measureWorkerFailureDetection(
            RecoveryExperimentConfig config
    ) throws Exception {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo(
                EXECUTOR_ID,
                SchedulerConfig.defaults(),
                List.of(TASK_TYPE)
        );
        registry.register(EXECUTOR_ID, peer);
        peer.updateHeartbeatReceivedNow();
        long failureStartedAt = System.nanoTime();
        AtomicLong detectedAt = new AtomicLong();
        CountDownLatch detected = new CountDownLatch(1);
        PeerLivenessMonitor monitor = new PeerLivenessMonitor(
                registry,
                config.workerFailureTimeoutMillis(),
                timedOut -> {
                    if (EXECUTOR_ID.equals(timedOut.getNodeId())) {
                        detectedAt.compareAndSet(0L, System.nanoTime());
                        detected.countDown();
                    }
                }
        );
        monitor.start();
        try {
            assertTrue(
                    detected.await(
                            config.workerFailureTimeoutMillis() + 10_000L,
                            TimeUnit.MILLISECONDS
                    ),
                    "Timed out waiting for production worker failure detection."
            );
            assertTrue(registry.get(EXECUTOR_ID) == null);
            long duration = detectedAt.get() - failureStartedAt;
            assertTrue(duration > 0L);
            return new WorkerFailureMeasurement(duration);
        } finally {
            monitor.shutdown();
        }
    }

    private static LeaseMeasurement measureLeaseReassignment(
            Path outputDirectory,
            RecoveryExperimentConfig experimentConfig
    ) throws Exception {
        Path databasePath = outputDirectory.resolve("lease-reassignment.db");
        SchedulerConfig config = schedulerConfig(
                experimentConfig.taskLeaseMillis()
        );
        BlockingQueue<MessageEnvelope> mailbox =
                SchedulerMailbox.create(config);
        CapturingSchedulerOutput output = new CapturingSchedulerOutput();
        DeterministicIds ids = new DeterministicIds(
                "00000000-0000-0000-0000-000000000708",
                "00000000-0000-0000-0000-000000000709"
        );
        try (DatabaseManager database =
                     new DatabaseManager(databasePath.toString())) {
            InMemoryPeerRegistry registry =
                    new InMemoryPeerRegistry(database);
            registry.register(
                    EXECUTOR_ID,
                    new PeerInfo(EXECUTOR_ID, config, List.of(TASK_TYPE))
            );
            TaskScheduler scheduler = new TaskScheduler(
                    mailbox,
                    registry,
                    database,
                    output,
                    config,
                    SystemTaskFlowClock.INSTANCE,
                    ids,
                    COORDINATOR_ID
            );
            Thread schedulerThread = new Thread(
                    scheduler,
                    "recovery-lease-scheduler"
            );
            schedulerThread.start();
            try {
                mailbox.put(new MessageEnvelope(
                        new JobSubmitMessage(
                                REQUESTER_ID,
                                Instant.now().toString(),
                                "job-recovery-lease",
                                TASK_TYPE,
                                List.of("payload"),
                                "",
                                "token-job-recovery-lease"
                        ),
                        REQUESTER_ID
                ));
                CapturedAssignment first = output.awaitAssignment(
                        experimentConfig.completionTimeoutSeconds()
                );
                CapturedAssignment second = output.awaitAssignment(
                        experimentConfig.completionTimeoutSeconds()
                );
                assertEquals(1, first.message().getAttemptNumber());
                assertEquals(2, second.message().getAttemptNumber());
                assertNotEquals(
                        first.message().getAssignmentId(),
                        second.message().getAssignmentId()
                );
                assertEquals(
                        first.message().getTaskId(),
                        second.message().getTaskId()
                );
                long reassignmentDelayMillis = second.capturedAtEpochMillis()
                        - first.message().getLeaseExpiresAtEpochMillis();
                assertTrue(
                        reassignmentDelayMillis >= 0L,
                        "Replacement assignment preceded its durable lease deadline."
                );

                mailbox.put(new MessageEnvelope(
                        successfulResult(second.message(), "recovered-result"),
                        EXECUTOR_ID
                ));
                JobResultMessage terminal = output.awaitResult(
                        experimentConfig.completionTimeoutSeconds()
                );
                assertTrue(terminal.isSuccessful());
                assertEquals(
                        List.of("recovered-result"),
                        terminal.getResultsByTaskId()
                );
                List<JobStateStore.TaskAttemptRecord> attempts =
                        database.loadTaskAttempts("job-recovery-lease");
                assertEquals(2, attempts.size());
                assertEquals(
                        JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                        attempts.getFirst().outcome()
                );
                assertEquals("lease_expired", attempts.getFirst().failureReason());
                assertEquals(
                        JobStateStore.TaskAttemptOutcome.SUCCEEDED,
                        attempts.get(1).outcome()
                );
                writeLines(
                        outputDirectory.resolve("lease-assignments.csv"),
                        List.of(
                                "attemptNumber,assignmentId,"
                                        + "leaseExpiresAtEpochMillis,"
                                        + "capturedAtEpochMillis",
                                assignmentCsv(first),
                                assignmentCsv(second)
                        )
                );
                return new LeaseMeasurement(
                        TimeUnit.MILLISECONDS.toNanos(reassignmentDelayMillis),
                        first.message().getLeaseExpiresAtEpochMillis(),
                        second.capturedAtEpochMillis(),
                        attempts.size()
                );
            } finally {
                scheduler.requestShutdownAfterDrain();
                schedulerThread.join(5_000L);
                assertFalse(
                        schedulerThread.isAlive(),
                        "Recovery lease scheduler did not stop."
                );
            }
        }
    }

    private static PersistedRecoveryMeasurement measurePersistedRecovery(
            Path outputDirectory,
            RecoveryExperimentConfig config,
            String label,
            int taskCount
    ) throws Exception {
        Path databasePath = outputDirectory.resolve(label + ".db");
        int jobCount = config.jobCount(taskCount);
        long seedStartedAt = System.nanoTime();
        try (DatabaseManager database =
                     new DatabaseManager(databasePath.toString())) {
            int nextTask = 0;
            for (int jobIndex = 0; jobIndex < jobCount; jobIndex++) {
                String jobId = label + "-job-" + jobIndex;
                int tasksInJob = config.tasksInJob(taskCount, jobIndex);
                List<JobStateStore.TaskStartupState> tasks =
                        new ArrayList<>(tasksInJob);
                for (int offset = 0; offset < tasksInJob; offset++) {
                    tasks.add(new JobStateStore.TaskStartupState(
                            "task-" + jobId + "-" + offset,
                            "payload-" + nextTask
                    ));
                    nextTask++;
                }
                JobStateStore.JobSubmissionDecision decision =
                        database.commitJobSubmission(
                                jobId,
                                TASK_TYPE,
                                REQUESTER_ID,
                                RequesterTokens.hashToken("token-" + jobId),
                                "",
                                "request-hash-" + jobId,
                                "",
                                tasks
                        );
                assertEquals(
                        JobStateStore.JobSubmissionOutcome.COMMITTED,
                        decision.outcome()
                );
            }
            assertEquals(taskCount, nextTask);
        }
        long seedDuration = System.nanoTime() - seedStartedAt;

        long recoveryStartedAt = System.nanoTime();
        CoordinatorStartupRecovery.RecoveryResult recovery;
        try (DatabaseManager reopened =
                     new DatabaseManager(databasePath.toString())) {
            recovery = CoordinatorStartupRecovery.recoverPersistedJobs(
                    reopened,
                    SystemTaskFlowClock.INSTANCE,
                    deterministicIds(taskCount)
            );
            long recoveryDuration = System.nanoTime() - recoveryStartedAt;
            assertTrue(recovery.successful());
            assertEquals(0, recovery.failedJobs());
            assertEquals(jobCount, recovery.resumedJobs().size());
            long recoveredTasks = recovery.resumedJobs().stream()
                    .mapToLong(job -> job.getTasks().size())
                    .sum();
            assertEquals(taskCount, recoveredTasks);

            DatabaseAudit audit = auditDatabase(databasePath);
            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, audit.schemaVersion());
            assertEquals(jobCount, audit.runningJobs());
            assertEquals(taskCount, audit.pendingTasks());
            assertEquals(taskCount, audit.totalTasks());
            assertEquals("ok", audit.integrity());
            PersistedRecoveryMeasurement measurement =
                    new PersistedRecoveryMeasurement(
                            label,
                            taskCount,
                            jobCount,
                            seedDuration,
                            recoveryDuration,
                            audit
                    );
            writeLines(
                    outputDirectory.resolve(label + ".properties"),
                    measurement.properties()
            );
            return measurement;
        }
    }

    private static BrokerMeasurements measureBrokerRecoveryAndReplay(
            Path outputDirectory,
            RecoveryExperimentConfig config
    ) throws Exception {
        Network network = Network.newNetwork();
        RabbitMQContainer broker = new RabbitMQContainer(RABBITMQ_IMAGE)
                .withAdminUser("taskflow")
                .withAdminPassword("taskflow-recovery")
                .withNetwork(network)
                .withNetworkAliases("rabbitmq");
        ToxiproxyContainer toxiproxy =
                new ToxiproxyContainer(TOXIPROXY_IMAGE)
                        .withNetwork(network);
        RabbitMqTransport transport = null;
        Exception primaryFailure = null;
        try {
            broker.start();
            toxiproxy.start();
            new ToxiproxyClient(
                    toxiproxy.getHost(),
                    toxiproxy.getControlPort()
            ).createProxy(
                    "recovery-rabbitmq-amqp",
                    "0.0.0.0:" + TOXIPROXY_AMQP_PORT,
                    "rabbitmq:5672"
            );
            String token = UUID.randomUUID()
                    .toString()
                    .replace("-", "");
            RabbitMqTransportConfig transportConfig = transportConfig(
                    toxiproxy.getHost(),
                    toxiproxy.getMappedPort(TOXIPROXY_AMQP_PORT),
                    broker.getAdminUsername(),
                    broker.getAdminPassword(),
                    token
            );
            transport = new RabbitMqTransport(
                    transportConfig,
                    RECOVERY_POLICY
            );
            transport.declareTopology();
            Set<String> deliveredJobIds = ConcurrentHashMap.newKeySet();
            AtomicLong rawDeliveries = new AtomicLong();
            AtomicReference<Throwable> deliveryFailure =
                    new AtomicReference<>();
            transport.subscribePeer(
                    TransportRoute.JOB_RESULT,
                    REQUESTER_ID,
                    delivery -> captureOutboxDelivery(
                            delivery,
                            deliveredJobIds,
                            rawDeliveries,
                            deliveryFailure
                    )
            );
            RabbitMqSchedulerOutput publisher =
                    new RabbitMqSchedulerOutput(transport);
            RabbitMqTransport activeTransport = transport;

            Path steadyDatabasePath =
                    outputDirectory.resolve("outbox-replay.db");
            RecoveryOutboxProbe.ReplayResult steadyReplay;
            try (DatabaseManager database =
                         new DatabaseManager(steadyDatabasePath.toString())) {
                seedOutbox(
                        database,
                        "steady",
                        config.outboxMessageCount()
                );
                steadyReplay = RecoveryOutboxProbe.drain(
                        database,
                        publisher,
                        config.batchSize(),
                        config.completionTimeoutSeconds(),
                        () -> {
                            throwIfDeliveryFailed(deliveryFailure);
                            return countPrefix(
                                    deliveredJobIds,
                                    "recovery-outbox-steady-"
                            ) == config.outboxMessageCount();
                        }
                );
                assertEquals(
                        config.outboxMessageCount(),
                        steadyReplay.publishedRows()
                );
                assertEquals(
                        0L,
                        database.countPendingBrokerOutbox().count()
                );
            }
            DatabaseAudit steadyAudit = auditDatabase(steadyDatabasePath);
            assertEquals("ok", steadyAudit.integrity());

            int restartMessages = config.batchSize();
            Path restartDatabasePath =
                    outputDirectory.resolve("rabbitmq-restart.db");
            RecoveryOutboxProbe.ReplayResult restartReplay;
            long brokerRecoveryDuration;
            try (DatabaseManager database =
                         new DatabaseManager(restartDatabasePath.toString())) {
                seedOutbox(database, "restart", restartMessages);
                stopBroker(broker);
                awaitCondition(
                        () -> !activeTransport.connectionUsable(),
                        10_000L,
                        "RabbitMQ transport outage observation"
                );
                long recoveryStartedAt = System.nanoTime();
                startBroker(broker, transportConfig);
                restartReplay = RecoveryOutboxProbe.drain(
                        database,
                        publisher,
                        config.batchSize(),
                        config.completionTimeoutSeconds(),
                        () -> {
                            throwIfDeliveryFailed(deliveryFailure);
                            return countPrefix(
                                    deliveredJobIds,
                                    "recovery-outbox-restart-"
                            ) == restartMessages;
                        }
                );
                brokerRecoveryDuration =
                        System.nanoTime() - recoveryStartedAt;
                assertEquals(restartMessages, restartReplay.publishedRows());
                assertEquals(
                        0L,
                        database.countPendingBrokerOutbox().count()
                );
            }
            DatabaseAudit restartAudit = auditDatabase(restartDatabasePath);
            assertEquals("ok", restartAudit.integrity());
            throwIfDeliveryFailed(deliveryFailure);
            assertEquals(
                    config.outboxMessageCount() + restartMessages,
                    deliveredJobIds.size()
            );
            assertEquals(deliveredJobIds.size(), rawDeliveries.get());
            writeLines(
                    outputDirectory.resolve("outbox-deliveries.txt"),
                    deliveredJobIds.stream().sorted().toList()
            );
            return new BrokerMeasurements(
                    brokerRecoveryDuration,
                    restartMessages,
                    restartReplay,
                    config.outboxMessageCount(),
                    steadyReplay,
                    rawDeliveries.get(),
                    deliveredJobIds.size(),
                    steadyAudit,
                    restartAudit
            );
        } catch (Exception failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            Throwable cleanupFailure = null;
            if (transport != null) {
                try {
                    transport.close();
                } catch (Throwable failure) {
                    cleanupFailure = merge(cleanupFailure, failure);
                }
            }
            try {
                broker.stop();
            } catch (Throwable failure) {
                cleanupFailure = merge(cleanupFailure, failure);
            }
            try {
                toxiproxy.stop();
            } catch (Throwable failure) {
                cleanupFailure = merge(cleanupFailure, failure);
            }
            try {
                network.close();
            } catch (Throwable failure) {
                cleanupFailure = merge(cleanupFailure, failure);
            }
            if (cleanupFailure != null) {
                if (primaryFailure != null) {
                    primaryFailure.addSuppressed(cleanupFailure);
                } else if (cleanupFailure instanceof Exception exception) {
                    throw exception;
                } else if (cleanupFailure instanceof Error error) {
                    throw error;
                } else {
                    throw new IllegalStateException(
                            "Unexpected broker cleanup failure.",
                            cleanupFailure
                    );
                }
            }
        }
    }

    private static OrphanMeasurement measureOrphanCleanup(
            Path outputDirectory,
            RecoveryExperimentConfig config
    ) throws Exception {
        MinIOContainer minio = new MinIOContainer(MINIO_IMAGE);
        String endpoint = null;
        String bucket = "taskflow-recovery-"
                + UUID.randomUUID().toString().replace("-", "");
        Path databasePath = outputDirectory.resolve("orphan-cleanup.db");
        try {
            minio.start();
            endpoint = minioEndpoint(minio);
            try (MinioClient client = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(minio.getUserName(), minio.getPassword())
                    .build()) {
                client.makeBucket(
                        MakeBucketArgs.builder().bucket(bucket).build()
                );
            }
            System.setProperty("taskflow.minioEndpoint", endpoint);
            System.setProperty("taskflow.minioAccessKey", minio.getUserName());
            System.setProperty("taskflow.minioSecretKey", minio.getPassword());
            System.setProperty("taskflow.minioBucket", bucket);

            byte[] content = "recovery-orphan".getBytes(
                    StandardCharsets.UTF_8
            );
            String digest = sha256(content);
            List<String> objectKeys =
                    new ArrayList<>(config.orphanObjectCount());
            try (DatabaseManager database =
                         new DatabaseManager(databasePath.toString());
                 ObjectStore store = ObjectStores.open()) {
                for (int index = 0;
                     index < config.orphanObjectCount();
                     index++) {
                    String assignmentId = UUID.nameUUIDFromBytes(
                            ("recovery-orphan-" + index).getBytes(
                                    StandardCharsets.UTF_8
                            )
                    ).toString();
                    String key = TaskFlowObjectKeys.attemptOutputKey(
                            "orphan-job-" + index,
                            "orphan-task-" + index,
                            1,
                            assignmentId
                    );
                    objectKeys.add(key);
                    store.putIfAbsent(
                            new ObjectReference(
                                    key,
                                    content.length,
                                    digest,
                                    "application/octet-stream"
                            ),
                            new ByteArrayInputStream(content)
                    );
                }
                long futureEpochMillis = System.currentTimeMillis()
                        + TimeUnit.MINUTES.toMillis(1L);
                RecoveryOrphanProbe.CleanupResult result =
                        RecoveryOrphanProbe.clean(
                                store,
                                database,
                                fixedClock(futureEpochMillis),
                                config.orphanObjectCount(),
                                config.batchSize()
                        );
                DatabaseAudit audit = auditDatabase(databasePath);
                assertEquals("ok", audit.integrity());
                writeLines(
                        outputDirectory.resolve("orphan-object-keys.txt"),
                        objectKeys
                );
                return new OrphanMeasurement(result, audit);
            }
        } finally {
            clearMinioProperties();
            minio.stop();
        }
    }

    private static void captureOutboxDelivery(
            InboundTransportMessage delivery,
            Set<String> deliveredJobIds,
            AtomicLong rawDeliveries,
            AtomicReference<Throwable> failure
    ) {
        try {
            if (!(delivery.message() instanceof JobResultMessage result)
                    || !result.getJobId().startsWith("recovery-outbox-")) {
                delivery.acknowledgement().settle(
                        DeliveryDisposition.REJECT_INVALID,
                        "recovery_outbox_message_invalid"
                );
                return;
            }
            rawDeliveries.incrementAndGet();
            deliveredJobIds.add(result.getJobId());
            delivery.acknowledgement().settle(
                    DeliveryDisposition.ACK_SUCCESS,
                    "recovery_outbox_received"
            );
        } catch (Throwable throwable) {
            failure.compareAndSet(null, throwable);
            if (throwable instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (throwable instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(
                    "Recovery outbox delivery callback failed.",
                    throwable
            );
        }
    }

    private static void seedOutbox(
            DatabaseManager database,
            String kind,
            int count
    ) {
        for (int index = 0; index < count; index++) {
            String jobId = "recovery-outbox-" + kind + "-" + index;
            BrokerOutboxStore.OutboxRecord record =
                    database.enqueueBrokerOutbox(
                            new BrokerOutboxStore.OutboxMessage(
                                    TransportRoute.JOB_RESULT,
                                    REQUESTER_ID,
                                    COORDINATOR_ID,
                                    new JobResultMessage(
                                            COORDINATOR_ID,
                                            Instant.now().toString(),
                                            jobId,
                                            TASK_TYPE,
                                            true,
                                            List.of("result-" + index)
                                    )
                            )
                    ).orElseThrow();
            assertTrue(record.outboxId() > 0L);
        }
        assertEquals(count, database.countPendingBrokerOutbox().count());
    }

    private static long countPrefix(Set<String> values, String prefix) {
        return values.stream().filter(value -> value.startsWith(prefix)).count();
    }

    private static void throwIfDeliveryFailed(
            AtomicReference<Throwable> failure
    ) {
        Throwable observed = failure.get();
        if (observed != null) {
            throw new IllegalStateException(
                    "Recovery broker delivery failed.",
                    observed
            );
        }
    }

    private static TaskResultMessage successfulResult(
            TaskAssignMessage assignment,
            String payload
    ) {
        return new TaskResultMessage(
                EXECUTOR_ID,
                Instant.now().toString(),
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                payload,
                true,
                ""
        );
    }

    private static SchedulerConfig schedulerConfig(long leaseMillis) {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        return new SchedulerConfig(
                Math.max(60_000L, leaseMillis * 10L),
                leaseMillis,
                3,
                1_000,
                defaults.maxActiveJobs(),
                defaults.maxActiveTasks(),
                defaults.maxPendingOutboxRows(),
                defaults.jobResultMaxDeliveryAttempts(),
                defaults.schedulerMessageBatchSize(),
                defaults.schedulerDeadlineBatchSize(),
                defaults.schedulerDispatchBatchSize(),
                defaults.schedulerMaxAssignmentsPerJobPerRound(),
                defaults.schedulerOutboxBatchSize(),
                defaults.metricsLogIntervalMillis(),
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );
    }

    private static AssignmentIdGenerator deterministicIds(int taskCount) {
        AtomicLong next = new AtomicLong();
        return () -> new UUID(
                taskCount,
                next.incrementAndGet()
        ).toString();
    }

    private static RabbitMqTransportConfig transportConfig(
            String host,
            int port,
            String username,
            String password,
            String token
    ) {
        String name = "taskflow.recovery." + token;
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
                2_000L,
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

    private static void startBroker(
            RabbitMQContainer broker,
            RabbitMqTransportConfig config
    ) throws Exception {
        broker.getDockerClient()
                .startContainerCmd(broker.getContainerId())
                .exec();
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(45L);
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                openReadinessConnection(config);
                return;
            } catch (Exception unavailable) {
                lastFailure = unavailable;
                LockSupport.parkNanos(
                        TimeUnit.MILLISECONDS.toNanos(100L)
                );
            }
        }
        throw new IllegalStateException(
                "RabbitMQ did not become ready after restart.",
                lastFailure
        );
    }

    private static void openReadinessConnection(
            RabbitMqTransportConfig config
    ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(false);
        factory.setConnectionTimeout(250);
        factory.setHandshakeTimeout(500);
        try (Connection ignored = factory.newConnection(
                "taskflow-recovery-readiness"
        )) {
        }
    }

    private static void awaitCondition(
            CheckedCondition condition,
            long timeoutMillis,
            String description
    ) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.evaluate()) {
                return;
            }
            LockSupport.parkNanos(CONDITION_PARK_NANOS);
        }
        throw new IllegalStateException(
                "Timed out waiting for " + description + "."
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
                    queryLong(statement,
                            "SELECT version FROM schema_version WHERE id=1"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM jobs WHERE status='RUNNING'"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM tasks WHERE status='PENDING'"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM tasks"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM broker_outbox "
                                    + "WHERE published_at IS NULL"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM broker_outbox "
                                    + "WHERE published_at IS NOT NULL"),
                    queryLong(statement,
                            "SELECT COUNT(*) FROM "
                                    + "orphan_output_gc_failures"),
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

    private static String minioEndpoint(MinIOContainer minio) {
        return "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
    }

    private static String sha256(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte value : digest) {
            hex.append(String.format(Locale.ROOT, "%02x", value & 0xff));
        }
        return hex.toString();
    }

    private static TaskFlowClock fixedClock(long epochMillis) {
        return new TaskFlowClock() {
            @Override
            public Instant now() {
                return Instant.ofEpochMilli(epochMillis);
            }

            @Override
            public long nowEpochMillis() {
                return epochMillis;
            }
        };
    }

    private static void clearMinioProperties() {
        System.clearProperty("taskflow.minioEndpoint");
        System.clearProperty("taskflow.minioAccessKey");
        System.clearProperty("taskflow.minioSecretKey");
        System.clearProperty("taskflow.minioBucket");
    }

    private static void writeConfiguration(
            Path outputDirectory,
            RecoveryExperimentConfig config,
            boolean reportGrade
    ) throws Exception {
        writeLines(
                outputDirectory.resolve("configuration.properties"),
                List.of(
                        "formatVersion=1",
                        "result=PASS",
                        "reportGrade=" + reportGrade,
                        "coordinatorRestartTaskCount="
                                + config.coordinatorRestartTaskCount(),
                        "smallPersistedTaskCount="
                                + config.smallPersistedTaskCount(),
                        "largePersistedTaskCount="
                                + config.largePersistedTaskCount(),
                        "tasksPerJob=" + config.tasksPerJob(),
                        "outboxMessageCount="
                                + config.outboxMessageCount(),
                        "orphanObjectCount="
                                + config.orphanObjectCount(),
                        "workerFailureTimeoutMillis="
                                + config.workerFailureTimeoutMillis(),
                        "taskLeaseMillis=" + config.taskLeaseMillis(),
                        "batchSize=" + config.batchSize(),
                        "completionTimeoutSeconds="
                                + config.completionTimeoutSeconds(),
                        "rabbitMqImage=" + RABBITMQ_IMAGE,
                        "minioImage=" + MINIO_IMAGE
                )
        );
    }

    private static void writeMetrics(
            Path outputDirectory,
            boolean reportGrade,
            WorkerFailureMeasurement worker,
            LeaseMeasurement lease,
            PersistedRecoveryMeasurement coordinator,
            PersistedRecoveryMeasurement small,
            PersistedRecoveryMeasurement large,
            BrokerMeasurements broker,
            OrphanMeasurement orphan
    ) throws Exception {
        writeLines(
                outputDirectory.resolve("metrics.properties"),
                List.of(
                        "formatVersion=1",
                        "result=PASS",
                        "reportGrade=" + reportGrade,
                        "workerFailureDetectionMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                worker.durationNanos()
                        )),
                        "leaseExpiryToReassignmentMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                lease.durationNanos()
                        )),
                        "coordinatorRestartRecoveryMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                coordinator.recoveryDurationNanos()
                        )),
                        "persisted10000RecoveryMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                small.recoveryDurationNanos()
                        )),
                        "persisted100000RecoveryMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                large.recoveryDurationNanos()
                        )),
                        "rabbitMqRestartRecoveryMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                broker.restartDurationNanos()
                        )),
                        "outboxReplayRows="
                                + broker.steadyRows(),
                        "outboxReplayMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                broker.steadyReplay().durationNanos()
                        )),
                        "outboxReplayRowsPerSecond="
                                + decimal(RecoveryMetrics.ratePerSecond(
                                broker.steadyRows(),
                                broker.steadyReplay().durationNanos()
                        )),
                        "objectOrphansDeleted="
                                + orphan.cleanup().deletedObjects(),
                        "objectOrphanCleanupMillis="
                                + decimal(RecoveryMetrics.nanosToMillis(
                                orphan.cleanup().durationNanos()
                        )),
                        "objectOrphanCleanupRowsPerSecond="
                                + decimal(RecoveryMetrics.ratePerSecond(
                                orphan.cleanup().deletedObjects(),
                                orphan.cleanup().durationNanos()
                        ))
                )
        );
    }

    private static void writeAudit(
            Path outputDirectory,
            LeaseMeasurement lease,
            PersistedRecoveryMeasurement coordinator,
            PersistedRecoveryMeasurement small,
            PersistedRecoveryMeasurement large,
            BrokerMeasurements broker,
            OrphanMeasurement orphan
    ) throws Exception {
        writeLines(
                outputDirectory.resolve("audit.properties"),
                List.of(
                        "formatVersion=1",
                        "coordinatorRestartRecoveredTasks="
                                + coordinator.taskCount(),
                        "coordinatorRestartRecoveredJobs="
                                + coordinator.jobCount(),
                        "persistedSmallRecoveredTasks=" + small.taskCount(),
                        "persistedSmallRecoveredJobs=" + small.jobCount(),
                        "persistedLargeRecoveredTasks=" + large.taskCount(),
                        "persistedLargeRecoveredJobs=" + large.jobCount(),
                        "leaseAttemptRows=" + lease.attemptRows(),
                        "leaseExpiredAtEpochMillis="
                                + lease.expiredAtEpochMillis(),
                        "leaseReassignedAtEpochMillis="
                                + lease.reassignedAtEpochMillis(),
                        "outboxRestartRows=" + broker.restartRows(),
                        "outboxSteadyRows=" + broker.steadyRows(),
                        "outboxRawDeliveries=" + broker.rawDeliveries(),
                        "outboxUniqueDeliveries="
                                + broker.uniqueDeliveries(),
                        "outboxSteadyPending="
                                + broker.steadyAudit().pendingOutbox(),
                        "outboxSteadySent="
                                + broker.steadyAudit().sentOutbox(),
                        "outboxRestartPending="
                                + broker.restartAudit().pendingOutbox(),
                        "outboxRestartSent="
                                + broker.restartAudit().sentOutbox(),
                        "orphanGcBatches=" + orphan.cleanup().batches(),
                        "orphanGcMaximumExaminedInBatch="
                                + orphan.cleanup().maximumExaminedInBatch(),
                        "orphanGcRetryRows="
                                + orphan.audit().orphanRetryRows(),
                        "coordinatorIntegrity="
                                + coordinator.audit().integrity(),
                        "persistedSmallIntegrity="
                                + small.audit().integrity(),
                        "persistedLargeIntegrity="
                                + large.audit().integrity(),
                        "outboxSteadyIntegrity="
                                + broker.steadyAudit().integrity(),
                        "outboxRestartIntegrity="
                                + broker.restartAudit().integrity(),
                        "orphanIntegrity=" + orphan.audit().integrity()
                )
        );
    }

    private static String assignmentCsv(CapturedAssignment assignment) {
        return assignment.message().getAttemptNumber()
                + ","
                + assignment.message().getAssignmentId()
                + ","
                + assignment.message().getLeaseExpiresAtEpochMillis()
                + ","
                + assignment.capturedAtEpochMillis();
    }

    private static String decimal(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static Throwable merge(
            Throwable existing,
            Throwable additional
    ) {
        if (existing == null) {
            return additional;
        }
        existing.addSuppressed(additional);
        return existing;
    }

    private static void writeLines(Path path, List<String> lines)
            throws Exception {
        Files.write(
                path,
                lines,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
        );
    }

    private static final class CapturingSchedulerOutput
            implements SchedulerOutput {
        private final BlockingQueue<CapturedAssignment> assignments =
                new LinkedBlockingQueue<>();
        private final BlockingQueue<JobResultMessage> results =
                new LinkedBlockingQueue<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            assertEquals(EXECUTOR_ID, peer.getNodeId());
            assignments.add(new CapturedAssignment(
                    message,
                    System.currentTimeMillis()
            ));
        }

        @Override
        public boolean sendJobResult(
                String requesterNodeId,
                JobResultMessage message
        ) {
            assertEquals(REQUESTER_ID, requesterNodeId);
            results.add(message);
            return true;
        }

        private CapturedAssignment awaitAssignment(long timeoutSeconds)
                throws Exception {
            CapturedAssignment assignment = assignments.poll(
                    timeoutSeconds,
                    TimeUnit.SECONDS
            );
            assertNotNull(
                    assignment,
                    "Timed out waiting for recovery assignment."
            );
            return assignment;
        }

        private JobResultMessage awaitResult(long timeoutSeconds)
                throws Exception {
            JobResultMessage result = results.poll(
                    timeoutSeconds,
                    TimeUnit.SECONDS
            );
            assertNotNull(
                    result,
                    "Timed out waiting for recovery terminal result."
            );
            return result;
        }
    }

    private static final class DeterministicIds
            implements AssignmentIdGenerator {
        private final Deque<String> ids;

        private DeterministicIds(String... ids) {
            this.ids = new ArrayDeque<>(List.of(ids));
        }

        @Override
        public synchronized String nextAssignmentId() {
            if (ids.isEmpty()) {
                throw new IllegalStateException(
                        "No deterministic recovery assignment ID remains."
                );
            }
            return ids.removeFirst();
        }
    }

    private record WorkerFailureMeasurement(long durationNanos) {
    }

    private record CapturedAssignment(
            TaskAssignMessage message,
            long capturedAtEpochMillis
    ) {
    }

    private record LeaseMeasurement(
            long durationNanos,
            long expiredAtEpochMillis,
            long reassignedAtEpochMillis,
            int attemptRows
    ) {
    }

    private record PersistedRecoveryMeasurement(
            String label,
            int taskCount,
            int jobCount,
            long seedDurationNanos,
            long recoveryDurationNanos,
            DatabaseAudit audit
    ) {
        private List<String> properties() {
            return List.of(
                    "formatVersion=1",
                    "label=" + label,
                    "taskCount=" + taskCount,
                    "jobCount=" + jobCount,
                    "seedMillis=" + decimal(
                            RecoveryMetrics.nanosToMillis(
                                    seedDurationNanos
                            )
                    ),
                    "recoveryMillis=" + decimal(
                            RecoveryMetrics.nanosToMillis(
                                    recoveryDurationNanos
                            )
                    ),
                    "schemaVersion=" + audit.schemaVersion(),
                    "runningJobs=" + audit.runningJobs(),
                    "pendingTasks=" + audit.pendingTasks(),
                    "totalTasks=" + audit.totalTasks(),
                    "integrity=" + audit.integrity()
            );
        }
    }

    private record BrokerMeasurements(
            long restartDurationNanos,
            int restartRows,
            RecoveryOutboxProbe.ReplayResult restartReplay,
            int steadyRows,
            RecoveryOutboxProbe.ReplayResult steadyReplay,
            long rawDeliveries,
            long uniqueDeliveries,
            DatabaseAudit steadyAudit,
            DatabaseAudit restartAudit
    ) {
    }

    private record OrphanMeasurement(
            RecoveryOrphanProbe.CleanupResult cleanup,
            DatabaseAudit audit
    ) {
    }

    private record DatabaseAudit(
            long schemaVersion,
            long runningJobs,
            long pendingTasks,
            long totalTasks,
            long pendingOutbox,
            long sentOutbox,
            long orphanRetryRows,
            String integrity
    ) {
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }
}
