package server;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.SchedulerMetrics;
import server.scheduler.TaskScheduler;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRuntimeDefaults;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The integrated, operator-facing TF-0804 reviewer narrative.
 *
 * <p>The PowerShell wrapper opts into this test and asserts its exact
 * {@code TF0804 TRACE} contract. RabbitMQ and MinIO are real containers;
 * SQLite, the scheduler, the outbox publisher, broker settlement, and startup
 * recovery are production components. The two executor endpoints deliberately
 * keep result publication under test control so the failure order does not
 * depend on wall-clock timing.</p>
 */
class ReviewerDemoTest {
    private static final String ENABLED_PROPERTY = "taskflow.reviewer.demo";
    private static final String RABBITMQ_IMAGE = "rabbitmq:3.13-management";
    private static final String MINIO_IMAGE =
            "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    private static final String MINIO_BUCKET = "taskflow-reviewer-demo";
    private static final String REQUESTER_ID = "reviewer-requester";
    private static final String WORKER_A = "reviewer-worker-a";
    private static final String WORKER_B = "reviewer-worker-b";
    private static final String JOB_ID = "job-reviewer-demo";
    private static final String TASK_ID = "task-job-reviewer-demo-0";
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_TOKEN = "token-job-reviewer-demo";
    private static final String ASSIGNMENT_X =
            "00000000-0000-0000-0000-000000000801";
    private static final String ASSIGNMENT_Y =
            "00000000-0000-0000-0000-000000000802";
    private static final String UNUSED_RECOVERY_ASSIGNMENT =
            "00000000-0000-0000-0000-000000000803";
    private static final long STARTED_AT = 1_767_225_600_000L;
    private static final long LEASE_MILLIS = 1_000L;
    private static final long DEMO_LIMIT_NANOS = Duration.ofMinutes(5).toNanos();
    private static final long DELIVERY_TIMEOUT_SECONDS = 15L;

    @TempDir
    Path tempDir;

    @Test
    void runsFiveMinuteFailureRecoveryNarrative() throws Exception {
        Assumptions.assumeTrue(
                Boolean.getBoolean(ENABLED_PROPERTY),
                "Set -D" + ENABLED_PROPERTY + "=true to run the reviewer demo."
        );

        RabbitMQContainer rabbit = new RabbitMQContainer(RABBITMQ_IMAGE)
                .withAdminUser("taskflow")
                .withAdminPassword("taskflow-reviewer");
        MinIOContainer minio = new MinIOContainer(MINIO_IMAGE);
        RabbitMqTransport coordinatorTransport = null;
        RabbitMqTransport replacementCoordinatorTransport = null;
        RabbitMqTransport workerATransport = null;
        RabbitMqTransport workerBTransport = null;
        RabbitMqTransport requesterTransport = null;
        TaskScheduler firstScheduler = null;
        TaskScheduler replacementScheduler = null;
        Thread firstSchedulerThread = null;
        Thread replacementSchedulerThread = null;
        Logger schedulerLogger =
                (Logger) LoggerFactory.getLogger(TaskScheduler.class);
        AssignmentPublishAppender assignmentPublishAppender =
                new AssignmentPublishAppender();
        assignmentPublishAppender.start();
        schedulerLogger.addAppender(assignmentPublishAppender);

        try {
            rabbit.start();
            minio.start();
            initializeMinio(minio);
            long demoStartedNanos = System.nanoTime();

            Path databasePath = tempDir.resolve("tf0804-reviewer-demo.db");
            MutableClock clock = new MutableClock(STARTED_AT);
            SchedulerConfig schedulerConfig = SchedulerConfig.fromEnvironment(Map.of(
                    "TASKFLOW_TASK_TIMEOUT_MS", "60000",
                    "TASKFLOW_TASK_LEASE_MS", Long.toString(LEASE_MILLIS),
                    "TASKFLOW_MAX_TASK_RETRIES", "2"
            ));
            RabbitMqTransportConfig rabbitConfig = rabbitConfig(rabbit);
            AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
            BlockingQueue<String> registeredWorkers = new LinkedBlockingQueue<>();
            BlockingQueue<TaskAssignMessage> workerAAssignments =
                    new LinkedBlockingQueue<>();
            BlockingQueue<TaskAssignMessage> workerBAssignments =
                    new LinkedBlockingQueue<>();
            BlockingQueue<JobResultMessage> requesterResults =
                    new LinkedBlockingQueue<>();
            BlockingQueue<ResultSettlement> resultSettlements =
                    new LinkedBlockingQueue<>();
            BlockingQueue<RequestSettlement> requestSettlements =
                    new LinkedBlockingQueue<>();
            SchedulerMetrics.Snapshot terminalMetrics;

            coordinatorTransport = new RabbitMqTransport(rabbitConfig);
            workerATransport = new RabbitMqTransport(rabbitConfig);
            workerBTransport = new RabbitMqTransport(rabbitConfig);
            requesterTransport = new RabbitMqTransport(rabbitConfig);
            coordinatorTransport.declareTopology();

            DatabaseManager database =
                    new DatabaseManager(databasePath.toString());
            try {
                BlockingQueue<MessageEnvelope> mailbox =
                        SchedulerMailbox.create(schedulerConfig);
                InMemoryPeerRegistry registry =
                        new InMemoryPeerRegistry(database);
                subscribeCoordinatorIngress(
                        coordinatorTransport,
                        mailbox,
                        registry,
                        schedulerConfig,
                        registeredWorkers,
                        resultSettlements,
                        asyncFailure
                );
                subscribeAssignment(
                        workerATransport,
                        WORKER_A,
                        workerAAssignments,
                        asyncFailure
                );
                subscribeAssignment(
                        workerBTransport,
                        WORKER_B,
                        workerBAssignments,
                        asyncFailure
                );
                subscribeJobResults(
                        requesterTransport,
                        requesterResults,
                        asyncFailure
                );

                firstScheduler = new TaskScheduler(
                        mailbox,
                        registry,
                        database,
                        new RabbitMqSchedulerOutput(coordinatorTransport),
                        schedulerConfig,
                        clock,
                        new DeterministicIds(ASSIGNMENT_X, ASSIGNMENT_Y),
                        "COORDINATOR_tf0804_demo_1"
                );
                firstSchedulerThread = new Thread(
                        firstScheduler,
                        "tf0804-reviewer-demo-coordinator-1"
                );
                firstSchedulerThread.start();

                publishHeartbeat(workerATransport, WORKER_A, 1L);
                assertEquals(
                        WORKER_A,
                        await(registeredWorkers, asyncFailure, "worker A registration")
                );
                publishHeartbeat(workerBTransport, WORKER_B, 1L);
                assertEquals(
                        WORKER_B,
                        await(registeredWorkers, asyncFailure, "worker B registration")
                );
                assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION,
                        database.getSchemaVersion());
                trace(
                        1,
                        "STACK_READY coordinator_instance=COORDINATOR_tf0804_demo_1 "
                                + "rabbitmq=UP sqlite_schema="
                                + DatabaseManager.CURRENT_SCHEMA_VERSION
                                + " minio=UP workers=" + WORKER_A + "," + WORKER_B
                );

                requesterTransport.publish(new OutboundTransportMessage(
                        TransportRoute.JOB_SUBMIT,
                        REQUESTER_ID,
                        submission(clock)
                ));
                TaskAssignMessage first = await(
                        workerAAssignments,
                        asyncFailure,
                        "assignment X on worker A"
                );
                assertEquals(
                        ASSIGNMENT_X,
                        await(
                                assignmentPublishAppender.publishedAssignments(),
                                asyncFailure,
                                "assignment X published-outbox mark"
                        )
                );
                assertAssignment(
                        first,
                        WORKER_A,
                        1,
                        ASSIGNMENT_X,
                        STARTED_AT + LEASE_MILLIS
                );
                assertEquals("RUNNING", job(database).status());
                assertEquals(1, database.getTasksForJob(JOB_ID).size());
                trace(
                        2,
                        "SUBMITTED job_id=" + JOB_ID + " task_id=" + TASK_ID
                                + " accepted=true"
                );
                trace(
                        3,
                        "ASSIGNED worker_id=" + WORKER_A
                                + " attempt_number=1 assignment_id=" + ASSIGNMENT_X
                                + " lease_expires_at_epoch_ms="
                                + first.getLeaseExpiresAtEpochMillis()
                );

                workerATransport.close();
                registry.remove(WORKER_A);
                assertNull(registry.get(WORKER_A));
                trace(
                        4,
                        "WORKER_PAUSED worker_id=" + WORKER_A
                                + " transport=closed registry_status=DISCONNECTED"
                );

                clock.advanceMillis(LEASE_MILLIS);
                firstScheduler.requestSchedulingRecheck();
                TaskAssignMessage current = await(
                        workerBAssignments,
                        asyncFailure,
                        "assignment Y on worker B"
                );
                assertEquals(
                        ASSIGNMENT_Y,
                        await(
                                assignmentPublishAppender.publishedAssignments(),
                                asyncFailure,
                                "assignment Y published-outbox mark"
                        )
                );
                assertAssignment(
                        current,
                        WORKER_B,
                        2,
                        ASSIGNMENT_Y,
                        STARTED_AT + (2L * LEASE_MILLIS)
                );
                List<JobStateStore.TaskAttemptRecord> attemptsAfterExpiry =
                        database.loadTaskAttempts(JOB_ID);
                assertEquals(2, attemptsAfterExpiry.size());
                JobStateStore.TaskAttemptRecord expired =
                        attemptsAfterExpiry.getFirst();
                assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                        expired.outcome());
                assertEquals("lease_expired", expired.failureReason());
                assertEquals(STARTED_AT + LEASE_MILLIS, expired.finishedAt());
                trace(
                        5,
                        "LEASE_EXPIRED attempt_number=1 assignment_id="
                                + ASSIGNMENT_X + " at_epoch_ms="
                                + clock.nowEpochMillis()
                                + " outcome=RETRY_SCHEDULED"
                );
                trace(
                        6,
                        "REASSIGNED worker_id=" + WORKER_B
                                + " attempt_number=2 assignment_id=" + ASSIGNMENT_Y
                                + " lease_expires_at_epoch_ms="
                                + current.getLeaseExpiresAtEpochMillis()
                );

                try (RabbitMqTransport oldWorkerResultInjector =
                             new RabbitMqTransport(rabbitConfig)) {
                    oldWorkerResultInjector.publish(new OutboundTransportMessage(
                            TransportRoute.TASK_RESULT,
                            WORKER_A,
                            successfulResult(clock, WORKER_A, first, "obsolete-result")
                    ));
                }
                ResultSettlement stale = await(
                        resultSettlements,
                        asyncFailure,
                        "stale assignment X settlement"
                );
                assertSettlement(
                        stale,
                        first,
                        DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                        "duplicate_or_stale_domain_event"
                );
                assertCurrentAssignment(database, current, WORKER_B);
                assertEquals("RUNNING", job(database).status());
                assertNull(requesterResults.poll());
                trace(
                        7,
                        "STALE_REJECTED attempt_number=1 assignment_id="
                                + ASSIGNMENT_X
                                + " disposition=ACK_DUPLICATE_OR_STALE "
                                + "authoritative_assignment_id=" + ASSIGNMENT_Y
                );

                workerBTransport.publish(new OutboundTransportMessage(
                        TransportRoute.TASK_RESULT,
                        WORKER_B,
                        successfulResult(clock, WORKER_B, current, "current-result")
                ));
                ResultSettlement committed = await(
                        resultSettlements,
                        asyncFailure,
                        "current assignment Y settlement"
                );
                assertSettlement(
                        committed,
                        current,
                        DeliveryDisposition.ACK_SUCCESS,
                        "handled"
                );
                JobResultMessage completed = await(
                        requesterResults,
                        asyncFailure,
                        "terminal job result"
                );
                assertEquals(List.of("current-result"),
                        completed.getResultsByTaskId());
                assertEquals("COMPLETED", job(database).status());
                assertEquals(
                        List.of("current-result"),
                        database.loadCompletedJobResult(JOB_ID)
                                .orElseThrow()
                                .resultsByTaskId()
                );
                assertEquals(1L, database.loadTaskAttempts(JOB_ID).stream()
                        .filter(attempt -> attempt.outcome()
                                == JobStateStore.TaskAttemptOutcome.SUCCEEDED)
                        .count());
                assertEquals(List.of(), database.loadPendingBrokerOutbox(10));
                terminalMetrics = firstScheduler.getMetricsSnapshot();
                assertEquals(2L, terminalMetrics.assignmentGenerationsTotal());
                assertEquals(1L, terminalMetrics.taskLeaseExpirationsTotal());
                assertEquals(1L, terminalMetrics.taskResultsStaleTotal());
                assertEquals(1L, terminalMetrics.taskResultsCommittedTotal());
                assertEquals(1L, terminalMetrics.jobsCompletedTotal());
                trace(
                        8,
                        "CURRENT_COMMITTED attempt_number=2 assignment_id="
                                + ASSIGNMENT_Y
                                + " disposition=ACK_SUCCESS result=current-result"
                );
                trace(
                        9,
                        "COMPLETED job_id=" + JOB_ID
                                + " authoritative_results=1 final_result=current-result"
                );

                firstScheduler.requestShutdownAfterDrain();
                awaitStopped(firstSchedulerThread, "first coordinator");
            } finally {
                requestStop(firstScheduler);
                joinQuietly(firstSchedulerThread);
                closeQuietly(coordinatorTransport);
                database.close();
            }
            coordinatorTransport = null;

            replacementCoordinatorTransport =
                    new RabbitMqTransport(rabbitConfig);
            replacementCoordinatorTransport.declareTopology();
            DatabaseManager recoveredDatabase =
                    new DatabaseManager(databasePath.toString());
            try {
                BlockingQueue<MessageEnvelope> recoveredMailbox =
                        SchedulerMailbox.create(schedulerConfig);
                InMemoryPeerRegistry recoveredRegistry =
                        new InMemoryPeerRegistry(recoveredDatabase);
                subscribeCoordinatorRequestIngress(
                        replacementCoordinatorTransport,
                        recoveredMailbox,
                        requestSettlements,
                        asyncFailure
                );
                CoordinatorStartupRecovery.RecoveryResult recovery =
                        CoordinatorStartupRecovery.recoverPersistedJobs(
                                recoveredDatabase,
                                clock,
                                new DeterministicIds(UNUSED_RECOVERY_ASSIGNMENT)
                        );
                assertTrue(recovery.successful());
                assertEquals(List.of(), recovery.resumedJobs());
                replacementScheduler = new TaskScheduler(
                        recoveredMailbox,
                        recoveredRegistry,
                        recoveredDatabase,
                        new RabbitMqSchedulerOutput(
                                replacementCoordinatorTransport
                        ),
                        schedulerConfig,
                        clock,
                        new DeterministicIds(UNUSED_RECOVERY_ASSIGNMENT),
                        "COORDINATOR_tf0804_demo_2"
                );
                replacementScheduler.restoreJobs(
                        recovery.resumedJobs(),
                        recovery.requesterTokenHashes(),
                        recovery.requesterIdentityKeys()
                );
                replacementSchedulerThread = new Thread(
                        replacementScheduler,
                        "tf0804-reviewer-demo-coordinator-2"
                );
                replacementSchedulerThread.start();

                assertEquals("COMPLETED", job(recoveredDatabase).status());
                trace(
                        10,
                        "COORDINATOR_RESTARTED "
                                + "coordinator_instance=COORDINATOR_tf0804_demo_2 "
                                + "recovered_running_jobs=0 "
                                + "persisted_job_status=COMPLETED"
                );

                requesterTransport.publish(new OutboundTransportMessage(
                        TransportRoute.JOB_SUBMIT,
                        REQUESTER_ID,
                        new JobResultRequestMessage(
                                REQUESTER_ID,
                                clock.now().toString(),
                                JOB_ID,
                                REQUESTER_TOKEN
                        )
                ));
                JobResultMessage persisted = await(
                        requesterResults,
                        asyncFailure,
                        "persisted result after coordinator restart"
                );
                assertEquals(List.of("current-result"),
                        persisted.getResultsByTaskId());
                RequestSettlement requestSettlement = await(
                        requestSettlements,
                        asyncFailure,
                        "persisted-result request settlement"
                );
                assertEquals(DeliveryDisposition.ACK_SUCCESS,
                        requestSettlement.disposition());
                assertEquals("handled", requestSettlement.reasonCode());
                trace(
                        11,
                        "PERSISTED_RESULT_RETRIEVED job_id=" + JOB_ID
                                + " delivery=JOB_RESULT result=current-result"
                );

                assertEquals(List.of(),
                        recoveredDatabase.loadPendingBrokerOutbox(10));
                OutboxAudit outbox = auditOutbox(databasePath);
                assertEquals(3L, outbox.totalRows());
                assertEquals(3L, outbox.publishedRows());
                assertEquals(0L, outbox.pendingRows());
                trace(
                        12,
                        "OBSERVED metrics_assignments="
                                + terminalMetrics.assignmentGenerationsTotal()
                                + " metrics_lease_expirations="
                                + terminalMetrics.taskLeaseExpirationsTotal()
                                + " metrics_stale="
                                + terminalMetrics.taskResultsStaleTotal()
                                + " metrics_committed="
                                + terminalMetrics.taskResultsCommittedTotal()
                                + " metrics_jobs_completed="
                                + terminalMetrics.jobsCompletedTotal()
                                + " outbox_published=" + outbox.publishedRows()
                                + " outbox_pending=" + outbox.pendingRows()
                                + " minio_bucket=" + MINIO_BUCKET
                );

                replacementScheduler.requestShutdownAfterDrain();
                awaitStopped(replacementSchedulerThread,
                        "replacement coordinator");
            } finally {
                requestStop(replacementScheduler);
                joinQuietly(replacementSchedulerThread);
                closeQuietly(replacementCoordinatorTransport);
                recoveredDatabase.close();
            }
            replacementCoordinatorTransport = null;

            long durationNanos = System.nanoTime() - demoStartedNanos;
            assertTrue(
                    durationNanos < DEMO_LIMIT_NANOS,
                    "Reviewer demo exceeded five minutes after dependencies "
                            + "were ready: "
                            + TimeUnit.NANOSECONDS.toMillis(durationNanos)
                            + " ms"
            );
            System.out.println(
                    "TF0804 RESULT PASS trace_steps=12 duration_ms="
                            + TimeUnit.NANOSECONDS.toMillis(durationNanos)
            );
        } finally {
            requestStop(replacementScheduler);
            requestStop(firstScheduler);
            joinQuietly(replacementSchedulerThread);
            joinQuietly(firstSchedulerThread);
            closeQuietly(replacementCoordinatorTransport);
            closeQuietly(coordinatorTransport);
            closeQuietly(requesterTransport);
            closeQuietly(workerBTransport);
            closeQuietly(workerATransport);
            schedulerLogger.detachAppender(assignmentPublishAppender);
            assignmentPublishAppender.stop();
            minio.stop();
            rabbit.stop();
        }
    }

    private static void initializeMinio(MinIOContainer minio) throws Exception {
        String endpoint = "http://" + minio.getHost() + ":"
                + minio.getMappedPort(9000);
        try (MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(minio.getUserName(), minio.getPassword())
                .build()) {
            client.makeBucket(
                    MakeBucketArgs.builder().bucket(MINIO_BUCKET).build()
            );
            assertTrue(client.bucketExists(
                    BucketExistsArgs.builder().bucket(MINIO_BUCKET).build()
            ));
        }
    }

    private static RabbitMqTransportConfig rabbitConfig(
            RabbitMQContainer rabbit
    ) {
        String prefix = "taskflow.reviewer";
        return new RabbitMqTransportConfig(
                rabbit.getHost(),
                rabbit.getAmqpPort(),
                rabbit.getAdminUsername(),
                rabbit.getAdminPassword(),
                "/",
                prefix + ".exchange",
                prefix,
                true,
                1,
                5_000L,
                true,
                prefix + ".dlx",
                prefix + ".dlq",
                "dead-letter",
                List.of(100L, 250L)
        );
    }

    private static void subscribeCoordinatorIngress(
            RabbitMqTransport transport,
            BlockingQueue<MessageEnvelope> mailbox,
            InMemoryPeerRegistry registry,
            SchedulerConfig schedulerConfig,
            BlockingQueue<String> registeredWorkers,
            BlockingQueue<ResultSettlement> resultSettlements,
            AtomicReference<Throwable> asyncFailure
    ) throws Exception {
        transport.subscribe(TransportRoute.HEARTBEAT, delivery ->
                captureAsync(asyncFailure, () -> {
                    PongMessage heartbeat = assertInstanceOf(
                            PongMessage.class,
                            delivery.message()
                    );
                    String workerId = delivery.fromNodeId();
                    if (workerId == null || workerId.isBlank()) {
                        workerId = heartbeat.getNodeId();
                    }
                    if (registry.get(workerId) == null) {
                        registry.register(
                                workerId,
                                new PeerInfo(
                                        workerId,
                                        schedulerConfig,
                                        List.of()
                                )
                        );
                    }
                    registry.updateHeartbeat(workerId, heartbeat);
                    registeredWorkers.add(workerId);
                }));
        transport.subscribe(TransportRoute.JOB_SUBMIT, delivery ->
                enqueue(mailbox, delivery, asyncFailure));
        transport.subscribe(TransportRoute.TASK_RESULT, delivery ->
                enqueueTaskResult(
                        mailbox,
                        delivery,
                        resultSettlements,
                        asyncFailure
                ));
    }

    private static void subscribeCoordinatorRequestIngress(
            RabbitMqTransport transport,
            BlockingQueue<MessageEnvelope> mailbox,
            BlockingQueue<RequestSettlement> requestSettlements,
            AtomicReference<Throwable> asyncFailure
    ) throws Exception {
        transport.subscribe(TransportRoute.JOB_SUBMIT, delivery ->
                enqueueRequest(
                        mailbox,
                        delivery,
                        requestSettlements,
                        asyncFailure
                ));
    }

    private static void subscribeAssignment(
            RabbitMqTransport transport,
            String workerId,
            BlockingQueue<TaskAssignMessage> assignments,
            AtomicReference<Throwable> asyncFailure
    ) throws Exception {
        transport.subscribePeer(
                TransportRoute.TASK_ASSIGN,
                workerId,
                delivery -> captureAsync(asyncFailure, () -> {
                    assertEquals(TransportRoute.TASK_ASSIGN, delivery.route());
                    TaskAssignMessage assignment = assertInstanceOf(
                            TaskAssignMessage.class,
                            delivery.message()
                    );
                    assertEquals(workerId, assignment.getNodeId());
                    assertEquals(JOB_ID, assignment.getJobId());
                    assertEquals(TASK_TYPE, assignment.getTaskType());
                    assertEquals("reviewer-payload", assignment.getPayload());
                    assignments.add(assignment);
                })
        );
    }

    private static void subscribeJobResults(
            RabbitMqTransport transport,
            BlockingQueue<JobResultMessage> results,
            AtomicReference<Throwable> asyncFailure
    ) throws Exception {
        transport.subscribePeer(
                TransportRoute.JOB_RESULT,
                REQUESTER_ID,
                delivery -> captureAsync(asyncFailure, () -> {
                    assertEquals(TransportRoute.JOB_RESULT, delivery.route());
                    assertEquals(
                            RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID,
                            delivery.fromNodeId()
                    );
                    JobResultMessage result = assertInstanceOf(
                            JobResultMessage.class,
                            delivery.message()
                    );
                    assertEquals(JOB_ID, result.getJobId());
                    assertEquals(TASK_TYPE, result.getTaskType());
                    assertTrue(result.isSuccessful());
                    results.add(result);
                })
        );
    }

    private static void enqueue(
            BlockingQueue<MessageEnvelope> mailbox,
            InboundTransportMessage delivery,
            AtomicReference<Throwable> asyncFailure
    ) throws Exception {
        try {
            TransportAcknowledgement acknowledgement =
                    delivery.acknowledgement();
            if (acknowledgement != null) {
                acknowledgement.defer();
            }
            if (!SchedulerMailbox.offer(
                    mailbox,
                    new MessageEnvelope(
                            delivery.message(),
                            delivery.fromNodeId(),
                            acknowledgement
                    )
            )) {
                throw new AssertionError(
                        "Reviewer demo scheduler mailbox saturated."
                );
            }
        } catch (Exception | Error failure) {
            asyncFailure.compareAndSet(null, failure);
            throw failure;
        }
    }

    private static void enqueueTaskResult(
            BlockingQueue<MessageEnvelope> mailbox,
            InboundTransportMessage delivery,
            BlockingQueue<ResultSettlement> settlements,
            AtomicReference<Throwable> asyncFailure
    ) throws Exception {
        try {
            TaskResultMessage result = assertInstanceOf(
                    TaskResultMessage.class,
                    delivery.message()
            );
            TransportAcknowledgement acknowledgement =
                    delivery.acknowledgement();
            assertNotNull(acknowledgement);
            acknowledgement.defer();
            if (!SchedulerMailbox.offer(
                    mailbox,
                    new MessageEnvelope(
                            result,
                            delivery.fromNodeId(),
                            new ObservedAcknowledgement(
                                    acknowledgement,
                                    result,
                                    settlements
                            )
                    )
            )) {
                throw new AssertionError(
                        "Reviewer demo task-result reserve saturated."
                );
            }
        } catch (Exception | Error failure) {
            asyncFailure.compareAndSet(null, failure);
            throw failure;
        }
    }

    private static void enqueueRequest(
            BlockingQueue<MessageEnvelope> mailbox,
            InboundTransportMessage delivery,
            BlockingQueue<RequestSettlement> settlements,
            AtomicReference<Throwable> asyncFailure
    ) throws Exception {
        try {
            assertInstanceOf(
                    JobResultRequestMessage.class,
                    delivery.message()
            );
            TransportAcknowledgement acknowledgement =
                    delivery.acknowledgement();
            assertNotNull(acknowledgement);
            acknowledgement.defer();
            if (!SchedulerMailbox.offer(
                    mailbox,
                    new MessageEnvelope(
                            delivery.message(),
                            delivery.fromNodeId(),
                            new ObservedRequestAcknowledgement(
                                    acknowledgement,
                                    settlements
                            )
                    )
            )) {
                throw new AssertionError(
                        "Reviewer demo result-request mailbox saturated."
                );
            }
        } catch (Exception | Error failure) {
            asyncFailure.compareAndSet(null, failure);
            throw failure;
        }
    }

    private static void publishHeartbeat(
            RabbitMqTransport transport,
            String workerId,
            long sequence
    ) throws Exception {
        String executorInstanceId = WORKER_A.equals(workerId)
                ? "00000000-0000-0000-0000-000000000811"
                : "00000000-0000-0000-0000-000000000812";
        transport.publish(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                workerId,
                new PongMessage(
                        workerId,
                        Instant.ofEpochMilli(STARTED_AT).toString(),
                        List.of(TASK_TYPE),
                        executorInstanceId,
                        sequence,
                        1,
                        1,
                        Map.of(TASK_TYPE, 1)
                )
        ));
    }

    private static JobSubmitMessage submission(TaskFlowClock clock) {
        return new JobSubmitMessage(
                REQUESTER_ID,
                clock.now().toString(),
                JOB_ID,
                TASK_TYPE,
                List.of("reviewer-payload"),
                "",
                REQUESTER_TOKEN
        );
    }

    private static TaskResultMessage successfulResult(
            TaskFlowClock clock,
            String workerId,
            TaskAssignMessage assignment,
            String result
    ) {
        return new TaskResultMessage(
                workerId,
                clock.now().toString(),
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                result,
                true,
                null
        );
    }

    private static void assertAssignment(
            TaskAssignMessage assignment,
            String workerId,
            int attemptNumber,
            String assignmentId,
            long leaseExpiresAt
    ) {
        assertEquals(workerId, assignment.getNodeId());
        assertEquals(JOB_ID, assignment.getJobId());
        assertEquals(TASK_ID, assignment.getTaskId());
        assertEquals(attemptNumber, assignment.getAttemptNumber());
        assertEquals(assignmentId, assignment.getAssignmentId());
        assertEquals(leaseExpiresAt,
                assignment.getLeaseExpiresAtEpochMillis());
    }

    private static void assertSettlement(
            ResultSettlement settlement,
            TaskAssignMessage assignment,
            DeliveryDisposition disposition,
            String reasonCode
    ) {
        assertEquals(assignment.getAttemptNumber(),
                settlement.attemptNumber());
        assertEquals(assignment.getAssignmentId(),
                settlement.assignmentId());
        assertEquals(disposition, settlement.disposition());
        assertEquals(reasonCode, settlement.reasonCode());
    }

    private static void assertCurrentAssignment(
            DatabaseManager database,
            TaskAssignMessage assignment,
            String workerId
    ) {
        DatabaseManager.TaskRecord task =
                database.getTasksForJob(JOB_ID).getFirst();
        assertEquals("ASSIGNED", task.status());
        assertEquals(workerId, task.assignedPeerId());
        assertEquals(assignment.getAttemptNumber(), task.attemptNumber());
        assertEquals(assignment.getAssignmentId(), task.assignmentId());
        assertEquals(assignment.getLeaseExpiresAtEpochMillis(),
                task.leaseExpiresAt());
    }

    private static DatabaseManager.JobRecord job(DatabaseManager database) {
        return database.getJobHistory().stream()
                .filter(candidate -> JOB_ID.equals(candidate.jobId()))
                .findFirst()
                .orElseThrow();
    }

    private static OutboxAudit auditOutbox(Path databasePath)
            throws Exception {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:sqlite:" + databasePath.toAbsolutePath()
        ); Statement statement = connection.createStatement()) {
            long total = queryLong(
                    statement,
                    "SELECT COUNT(*) FROM broker_outbox"
            );
            long published = queryLong(
                    statement,
                    "SELECT COUNT(*) FROM broker_outbox "
                            + "WHERE published_at IS NOT NULL"
            );
            return new OutboxAudit(total, published, total - published);
        }
    }

    private static long queryLong(Statement statement, String sql)
            throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private static <T> T await(
            BlockingQueue<T> queue,
            AtomicReference<Throwable> asyncFailure,
            String description
    ) throws Exception {
        T value = queue.poll(
                DELIVERY_TIMEOUT_SECONDS,
                TimeUnit.SECONDS
        );
        Throwable failure = asyncFailure.get();
        if (failure != null) {
            fail(failure);
        }
        assertNotNull(value, "Timed out waiting for " + description);
        return value;
    }

    private static void captureAsync(
            AtomicReference<Throwable> failure,
            CheckedAction action
    ) {
        try {
            action.run();
        } catch (Throwable callbackFailure) {
            failure.compareAndSet(null, callbackFailure);
        }
    }

    private static void awaitStopped(Thread thread, String description)
            throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(5L));
        assertFalse(thread.isAlive(), description + " did not stop.");
    }

    private static void requestStop(TaskScheduler scheduler) {
        if (scheduler != null) {
            scheduler.requestShutdownAfterDrain();
        }
    }

    private static void joinQuietly(Thread thread) {
        if (thread == null) {
            return;
        }
        try {
            thread.join(TimeUnit.SECONDS.toMillis(2L));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Preserve the primary assertion failure while cleanup continues.
        }
    }

    private static void trace(int step, String message) {
        System.out.println("TF0804 TRACE " + step + " " + message);
    }

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws Exception;
    }

    private static final class MutableClock implements TaskFlowClock {
        private long epochMillis;

        private MutableClock(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        @Override
        public synchronized Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public synchronized long nowEpochMillis() {
            return epochMillis;
        }

        private synchronized void advanceMillis(long millis) {
            epochMillis += millis;
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
                        "No deterministic reviewer assignment ID remains."
                );
            }
            return ids.removeFirst();
        }
    }

    private static final class ObservedAcknowledgement
            implements TransportAcknowledgement {
        private final TransportAcknowledgement delegate;
        private final TaskResultMessage result;
        private final BlockingQueue<ResultSettlement> settlements;

        private ObservedAcknowledgement(
                TransportAcknowledgement delegate,
                TaskResultMessage result,
                BlockingQueue<ResultSettlement> settlements
        ) {
            this.delegate = delegate;
            this.result = result;
            this.settlements = settlements;
        }

        @Override
        public void settle(
                DeliveryDisposition disposition,
                String reasonCode
        ) throws Exception {
            delegate.settle(disposition, reasonCode);
            settlements.add(new ResultSettlement(
                    result.getAttemptNumber(),
                    result.getAssignmentId(),
                    disposition,
                    reasonCode
            ));
        }

        @Override
        public void ack() throws Exception {
            delegate.ack();
            settlements.add(new ResultSettlement(
                    result.getAttemptNumber(),
                    result.getAssignmentId(),
                    DeliveryDisposition.ACK_SUCCESS,
                    "legacy_ack"
            ));
        }

        @Override
        public void requeue() throws Exception {
            delegate.requeue();
            settlements.add(new ResultSettlement(
                    result.getAttemptNumber(),
                    result.getAssignmentId(),
                    DeliveryDisposition.RETRY_TRANSIENT,
                    "legacy_requeue"
            ));
        }

        @Override
        public void reject() throws Exception {
            delegate.reject();
            settlements.add(new ResultSettlement(
                    result.getAttemptNumber(),
                    result.getAssignmentId(),
                    DeliveryDisposition.REJECT_INVALID,
                    "legacy_reject"
            ));
        }

        @Override
        public void defer() {
            delegate.defer();
        }
    }

    private record ResultSettlement(
            int attemptNumber,
            String assignmentId,
            DeliveryDisposition disposition,
            String reasonCode
    ) {
    }

    private record RequestSettlement(
            DeliveryDisposition disposition,
            String reasonCode
    ) {
    }

    private record OutboxAudit(
            long totalRows,
            long publishedRows,
            long pendingRows
    ) {
    }

    private static final class AssignmentPublishAppender
            extends AppenderBase<ILoggingEvent> {
        private final BlockingQueue<String> publishedAssignments =
                new LinkedBlockingQueue<>();

        @Override
        protected void append(ILoggingEvent event) {
            String message = event.getFormattedMessage();
            if (!message.contains("event=task_assignment_created")
                    || !message.contains("outbox_published=true")) {
                return;
            }
            if (message.contains("assignment_id=" + ASSIGNMENT_X)) {
                publishedAssignments.add(ASSIGNMENT_X);
            } else if (message.contains("assignment_id=" + ASSIGNMENT_Y)) {
                publishedAssignments.add(ASSIGNMENT_Y);
            }
        }

        private BlockingQueue<String> publishedAssignments() {
            return publishedAssignments;
        }
    }

    private static final class ObservedRequestAcknowledgement
            implements TransportAcknowledgement {
        private final TransportAcknowledgement delegate;
        private final BlockingQueue<RequestSettlement> settlements;

        private ObservedRequestAcknowledgement(
                TransportAcknowledgement delegate,
                BlockingQueue<RequestSettlement> settlements
        ) {
            this.delegate = delegate;
            this.settlements = settlements;
        }

        @Override
        public void settle(
                DeliveryDisposition disposition,
                String reasonCode
        ) throws Exception {
            delegate.settle(disposition, reasonCode);
            settlements.add(new RequestSettlement(disposition, reasonCode));
        }

        @Override
        public void ack() throws Exception {
            delegate.ack();
            settlements.add(new RequestSettlement(
                    DeliveryDisposition.ACK_SUCCESS,
                    "legacy_ack"
            ));
        }

        @Override
        public void requeue() throws Exception {
            delegate.requeue();
            settlements.add(new RequestSettlement(
                    DeliveryDisposition.RETRY_TRANSIENT,
                    "legacy_requeue"
            ));
        }

        @Override
        public void reject() throws Exception {
            delegate.reject();
            settlements.add(new RequestSettlement(
                    DeliveryDisposition.REJECT_INVALID,
                    "legacy_reject"
            ));
        }

        @Override
        public void defer() {
            delegate.defer();
        }
    }
}
