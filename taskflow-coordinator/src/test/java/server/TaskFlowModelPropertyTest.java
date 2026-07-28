package server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PeerDisconnectedMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.model.MessageEnvelope;
import server.rabbitmq.RabbitMqOutboxReplayer;
import server.registry.CapacityMetricsSnapshot;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.BrokerOutboxPublisher;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.DeliveryDisposition;
import transport.TransportAcknowledgement;
import transport.TransportRoute;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bounded model-based proof for one durable multi-task job.
 *
 * <p>The reference model deliberately contains no scheduler, persistence, or
 * transport implementation code. Generated events are applied independently
 * to that model and to the real scheduler backed by a temporary SQLite
 * database. Every event is followed by a comparison against durable state.</p>
 */
class TaskFlowModelPropertyTest {
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_ID = "model-requester";
    private static final int TASK_COUNT = 3;
    private static final int GENERATED_STEPS_PER_SEED = 32;
    private static final long[] CI_SEEDS = {
            3_520_704_001L,
            3_520_704_017L,
            3_520_704_033L,
            3_520_704_049L,
            3_520_704_065L,
            3_520_704_081L,
            3_520_704_097L,
            3_520_704_113L
    };

    @TempDir
    Path tempDir;

    @Test
    void generatedSequencesPreserveDurableSchedulerProperties() throws Exception {
        for (int index = 0; index < CI_SEEDS.length; index++) {
            long seed = CI_SEEDS[index];
            runWithSeed(seed, index, true);
        }
    }

    @Test
    void duplicateEventsDoNotDuplicateAuthoritativeTransitions() throws Exception {
        runWithSeed(3_520_704_129L, CI_SEEDS.length, false);
    }

    private void runWithSeed(long seed, int index, boolean generated) throws Exception {
        ModelHarness harness = null;
        try {
            harness = new ModelHarness(
                    tempDir.resolve("model-" + index + ".db"),
                    "model-job-" + index,
                    seed
            );
            try (ModelHarness closeable = harness) {
                if (generated) {
                    closeable.runGeneratedSequence();
                } else {
                    closeable.runDuplicateScenario();
                }
            }
        } catch (Throwable failure) {
            throw new AssertionError(
                    "TaskFlow model property failed; seed=" + seed
                            + " (0x" + Long.toHexString(seed) + ")\n"
                            + (harness == null
                            ? "event trace unavailable: fixture construction failed"
                            : harness.formattedTrace()),
                    failure
            );
        }
    }

    private enum Event {
        ASSIGN,
        DUPLICATE_ASSIGNMENT,
        SUCCESS_RESULT,
        DUPLICATE_RESULT,
        STALE_RESULT,
        FAILURE_RESULT,
        LEASE_EXPIRY,
        WORKER_DISCONNECT,
        RESTART_RELOAD,
        OUTBOX_REPLAY
    }

    private static final class ModelHarness implements AutoCloseable {
        private static final int MAILBOX_CAPACITY = 32;
        private static final int PUBLICATION_CAPACITY = 256;
        private static final int MAX_PUBLICATIONS_PER_EVENT = 128;
        private static final long AWAIT_SECONDS = 3L;

        private final Path databasePath;
        private final String jobId;
        private final SplittableRandom random;
        private final MutableClock clock = new MutableClock(1_000_000L);
        private final DeterministicAssignmentIds assignmentIds =
                new DeterministicAssignmentIds();
        private final RecordingOutboxPublisher output =
                new RecordingOutboxPublisher(PUBLICATION_CAPACITY);
        private final ReferenceModel model;
        private final List<String> trace = new ArrayList<>();
        private final Map<String, Integer> previousActualAttempts =
                new LinkedHashMap<>();
        private final Map<String, String> previousActualStatuses =
                new LinkedHashMap<>();
        private String previousActualJobStatus;
        private String connectedExecutorId = "model-executor-1";
        private int nextExecutorNumber = 2;
        private int traceSequence;
        private DatabaseManager database;
        private InMemoryPeerRegistry registry;
        private BlockingQueue<MessageEnvelope> mailbox;
        private TaskScheduler scheduler;
        private Thread schedulerThread;
        private TaskResultMessage lastSuccessfulResult;

        private ModelHarness(Path databasePath, String jobId, long seed)
                throws Exception {
            this.databasePath = databasePath;
            this.jobId = jobId;
            this.random = new SplittableRandom(seed);
            this.model = new ReferenceModel(jobId, TASK_COUNT);
            this.database = new DatabaseManager(databasePath.toString());
            try {
                startScheduler(null);
                submitJob();
            } catch (Exception failure) {
                closeAfterConstructionFailure(failure);
                throw failure;
            } catch (Error failure) {
                closeAfterConstructionFailure(failure);
                throw failure;
            }
        }

        private void runGeneratedSequence() throws Exception {
            runRequiredPrefix();
            Event[] events = Event.values();
            for (int step = 0; step < GENERATED_STEPS_PER_SEED; step++) {
                apply(events[random.nextInt(events.length)]);
            }
            finishAfterFailuresStop();
        }

        private void runDuplicateScenario() throws Exception {
            apply(Event.OUTBOX_REPLAY);
            apply(Event.DUPLICATE_ASSIGNMENT);
            apply(Event.FAILURE_RESULT);
            apply(Event.OUTBOX_REPLAY);
            apply(Event.STALE_RESULT);
            apply(Event.SUCCESS_RESULT);
            apply(Event.OUTBOX_REPLAY);
            apply(Event.DUPLICATE_RESULT);
            finishAfterFailuresStop();
        }

        private void runRequiredPrefix() throws Exception {
            apply(Event.OUTBOX_REPLAY);
            apply(Event.DUPLICATE_ASSIGNMENT);
            apply(Event.FAILURE_RESULT);
            apply(Event.OUTBOX_REPLAY);
            apply(Event.STALE_RESULT);
            apply(Event.LEASE_EXPIRY);
            apply(Event.OUTBOX_REPLAY);
            apply(Event.WORKER_DISCONNECT);
            apply(Event.OUTBOX_REPLAY);
            apply(Event.RESTART_RELOAD);
            apply(Event.SUCCESS_RESULT);
            apply(Event.OUTBOX_REPLAY);
            apply(Event.DUPLICATE_RESULT);
            apply(Event.ASSIGN);
        }

        private void apply(Event event) throws Exception {
            trace(event.name(), "begin");
            switch (event) {
                case ASSIGN -> requestAssignment();
                case DUPLICATE_ASSIGNMENT -> duplicateAssignment();
                case SUCCESS_RESULT -> successfulResult();
                case DUPLICATE_RESULT -> duplicateResult();
                case STALE_RESULT -> staleResult();
                case FAILURE_RESULT -> failedResult();
                case LEASE_EXPIRY -> expireLease();
                case WORKER_DISCONNECT -> disconnectWorker();
                case RESTART_RELOAD -> restartAndReload();
                case OUTBOX_REPLAY -> replayOutbox();
            }
            drainPublications();
            assertProperties();
        }

        private void submitJob() throws Exception {
            List<Object> payloads = List.of("payload-0", "payload-1", "payload-2");
            JobSubmitMessage submit = new JobSubmitMessage(
                    REQUESTER_ID,
                    clock.now().toString(),
                    jobId,
                    TASK_TYPE,
                    payloads,
                    "",
                    "token-" + jobId
            );
            DeliveryDisposition disposition = sendAndAwait(submit, REQUESTER_ID);
            assertEquals(DeliveryDisposition.ACK_SUCCESS, disposition);
            awaitNewAssignment();
            trace("SUBMIT", "accepted task_count=" + TASK_COUNT);
            assertProperties();
        }

        private void requestAssignment() throws Exception {
            if (model.terminal()) {
                trace(Event.ASSIGN.name(), "no-op terminal job");
                return;
            }
            scheduler.requestSchedulingRecheck();
            if (model.currentAssignment() == null) {
                awaitNewAssignment();
                return;
            }
            trace(
                    Event.ASSIGN.name(),
                    "no-op current=" + model.currentAssignment().assignmentId()
            );
        }

        private void duplicateAssignment() throws Exception {
            ReferenceModel.Assignment current = model.currentAssignment();
            if (current == null) {
                trace(Event.DUPLICATE_ASSIGNMENT.name(), "no-op no assignment");
                return;
            }
            BrokerOutboxStore.OutboxRecord record =
                    output.assignmentRecord(current.assignmentId());
            assertNotNull(record, "Current durable assignment is missing its outbox record");
            output.publishOutbox(record);
            drainPublications();
            trace(
                    Event.DUPLICATE_ASSIGNMENT.name(),
                    "replayed assignment_id=" + current.assignmentId()
            );
        }

        private void successfulResult() throws Exception {
            ReferenceModel.Assignment current = model.currentAssignment();
            if (current == null || !output.wasSuccessfullyPublished(current.assignmentId())) {
                trace(Event.SUCCESS_RESULT.name(), "no-op current assignment not delivered");
                return;
            }

            String payload = "result-" + current.taskId() + "-" + current.attemptNumber();
            TaskResultMessage result = successfulResult(current, payload);
            ReferenceModel.ResultDisposition expected = model.applySuccessfulResult(current);
            assertEquals(ReferenceModel.ResultDisposition.COMMITTED, expected);

            DeliveryDisposition disposition = sendAndAwait(result, current.workerId());
            assertEquals(DeliveryDisposition.ACK_SUCCESS, disposition);
            lastSuccessfulResult = result;
            drainPublications();
            if (!model.terminal()) {
                awaitNewAssignment();
            }
            trace(
                    Event.SUCCESS_RESULT.name(),
                    "committed task_id=" + current.taskId()
                            + " assignment_id=" + current.assignmentId()
            );
        }

        private void duplicateResult() throws Exception {
            if (lastSuccessfulResult == null) {
                trace(Event.DUPLICATE_RESULT.name(), "no-op no successful result");
                return;
            }
            ReferenceModel.Assignment assignment =
                    model.assignment(lastSuccessfulResult.getAssignmentId());
            assertNotNull(
                    assignment,
                    "Successful result is missing its reference-model assignment"
            );
            ReferenceModel.ResultDisposition expected =
                    model.applySuccessfulResult(assignment);
            assertEquals(ReferenceModel.ResultDisposition.DUPLICATE, expected);
            DeliveryDisposition disposition =
                    sendAndAwait(lastSuccessfulResult, assignment.workerId());
            assertEquals(DeliveryDisposition.ACK_DUPLICATE_OR_STALE, disposition);
            trace(
                    Event.DUPLICATE_RESULT.name(),
                    "ignored assignment_id=" + assignment.assignmentId()
            );
        }

        private void staleResult() throws Exception {
            ReferenceModel.Assignment stale = model.randomStaleAssignment(random);
            if (stale == null) {
                trace(Event.STALE_RESULT.name(), "no-op no obsolete assignment");
                return;
            }
            ReferenceModel.ResultDisposition expected =
                    model.applySuccessfulResult(stale);
            assertEquals(ReferenceModel.ResultDisposition.STALE, expected);
            DeliveryDisposition disposition = sendAndAwait(
                    successfulResult(stale, "stale-" + stale.assignmentId()),
                    stale.workerId()
            );
            assertEquals(DeliveryDisposition.ACK_DUPLICATE_OR_STALE, disposition);
            trace(
                    Event.STALE_RESULT.name(),
                    "fenced assignment_id=" + stale.assignmentId()
            );
        }

        private void failedResult() throws Exception {
            ReferenceModel.Assignment current = model.currentAssignment();
            if (current == null || !output.wasSuccessfullyPublished(current.assignmentId())) {
                trace(Event.FAILURE_RESULT.name(), "no-op current assignment not delivered");
                return;
            }
            model.applyRetryableFailure(current, "failure_result");
            TaskResultMessage failed = new TaskResultMessage(
                    current.workerId(),
                    clock.now().toString(),
                    current.taskId(),
                    jobId,
                    current.attemptNumber(),
                    current.assignmentId(),
                    null,
                    false,
                    "generated retryable failure"
            );
            DeliveryDisposition disposition = sendAndAwait(failed, current.workerId());
            assertEquals(DeliveryDisposition.ACK_SUCCESS, disposition);
            awaitNewAssignment();
            trace(
                    Event.FAILURE_RESULT.name(),
                    "retried assignment_id=" + current.assignmentId()
            );
        }

        private void expireLease() throws Exception {
            ReferenceModel.Assignment current = model.currentAssignment();
            if (current == null) {
                trace(Event.LEASE_EXPIRY.name(), "no-op no assignment");
                return;
            }
            clock.setEpochMillis(current.leaseExpiresAt());
            model.applyRetryableFailure(current, "lease_expired");
            PeerDisconnectedMessage wakeup = new PeerDisconnectedMessage(
                    "model-clock-wakeup",
                    clock.now().toString(),
                    "test_clock_advanced"
            );
            assertEquals(
                    DeliveryDisposition.ACK_SUCCESS,
                    sendAndAwait(wakeup, "model-clock-wakeup")
            );
            awaitNewAssignment();
            trace(
                    Event.LEASE_EXPIRY.name(),
                    "expired assignment_id=" + current.assignmentId()
            );
        }

        private void disconnectWorker() throws Exception {
            ReferenceModel.Assignment current = model.currentAssignment();
            if (current == null) {
                trace(Event.WORKER_DISCONNECT.name(), "no-op no assignment");
                return;
            }
            String disconnected = current.workerId();
            registry.remove(disconnected);
            connectedExecutorId = "model-executor-" + nextExecutorNumber++;
            registerExecutor(registry, connectedExecutorId);
            model.applyRetryableFailure(current, "worker_disconnected");
            PeerDisconnectedMessage message = new PeerDisconnectedMessage(
                    disconnected,
                    clock.now().toString(),
                    "generated_disconnect"
            );
            assertEquals(
                    DeliveryDisposition.ACK_SUCCESS,
                    sendAndAwait(message, disconnected)
            );
            awaitNewAssignment();
            trace(
                    Event.WORKER_DISCONNECT.name(),
                    "released worker_id=" + disconnected
                            + " replacement=" + connectedExecutorId
            );
        }

        private void restartAndReload() throws Exception {
            stopScheduler();
            database.close();
            database = new DatabaseManager(databasePath.toString());

            CoordinatorStartupRecovery.RecoveryResult recovered =
                    CoordinatorStartupRecovery.recoverPersistedJobs(
                            database,
                            clock,
                            assignmentIds
                    );
            assertRecoveredProjection(recovered);
            startScheduler(recovered);
            if (!model.terminal() && model.currentAssignment() == null) {
                awaitNewAssignment();
            }
            trace(
                    Event.RESTART_RELOAD.name(),
                    "resumed_jobs=" + recovered.resumedJobs().size()
            );
        }

        private void replayOutbox() throws Exception {
            int pendingBefore = database.loadPendingBrokerOutbox(256).size();
            try (RabbitMqOutboxReplayer replayer =
                         new RabbitMqOutboxReplayer(database, output, 256)) {
                replayer.start();
            }
            drainPublications();
            int pendingAfter = database.loadPendingBrokerOutbox(256).size();
            trace(
                    Event.OUTBOX_REPLAY.name(),
                    "pending_before=" + pendingBefore + " pending_after=" + pendingAfter
            );
        }

        private void finishAfterFailuresStop() throws Exception {
            int completions = 0;
            while (!model.terminal()) {
                ReferenceModel.Assignment current = model.currentAssignment();
                if (current == null) {
                    requestAssignment();
                    current = model.currentAssignment();
                }
                assertNotNull(current, "Nonterminal model had no assignable task");
                if (!output.wasSuccessfullyPublished(current.assignmentId())) {
                    replayOutbox();
                }
                successfulResult();
                assertProperties();
                completions++;
                assertTrue(
                        completions <= TASK_COUNT,
                        "Finishing a three-task model required too many completions"
                );
            }
            duplicateResult();
            staleResult();
            replayOutbox();
            restartAndReload();
            assertProperties();
            trace("FINISH", "job terminal after failures stopped");
        }

        private void awaitNewAssignment() throws Exception {
            for (int observed = 0; observed < MAX_PUBLICATIONS_PER_EVENT; observed++) {
                RecordingOutboxPublisher.Publication publication =
                        output.awaitPublication(AWAIT_SECONDS, TimeUnit.SECONDS);
                assertNotNull(
                        publication,
                        "Timed out waiting for the next durable assignment publication"
                );
                boolean wasUnassigned = model.currentAssignment() == null;
                processPublication(publication);
                if (wasUnassigned && model.currentAssignment() != null) {
                    return;
                }
            }
            throw new AssertionError("Publication bound reached without a new assignment");
        }

        private void drainPublications() {
            int processed = 0;
            RecordingOutboxPublisher.Publication publication;
            while ((publication = output.pollPublication()) != null) {
                processPublication(publication);
                processed++;
                assertTrue(
                        processed <= MAX_PUBLICATIONS_PER_EVENT,
                        "One generated event exceeded the publication-processing bound"
                );
            }
        }

        private void processPublication(
                RecordingOutboxPublisher.Publication publication
        ) {
            if (publication.record().message().message()
                    instanceof TaskAssignMessage assignment) {
                ReferenceModel.AssignmentDisposition disposition =
                        model.observeAssignment(assignment);
                trace(
                        disposition == ReferenceModel.AssignmentDisposition.NEW
                                ? Event.ASSIGN.name()
                                : Event.DUPLICATE_ASSIGNMENT.name(),
                        "assignment_id=" + assignment.getAssignmentId()
                                + " attempt=" + assignment.getAttemptNumber()
                                + " published=" + publication.successful()
                                + " disposition=" + disposition
                );
            } else if (publication.record().message().message()
                    instanceof JobResultMessage result) {
                trace(
                        Event.OUTBOX_REPLAY.name(),
                        "job_result_outbox_id=" + publication.record().outboxId()
                                + " successful=" + publication.successful()
                                + " terminal_success=" + result.isSuccessful()
                );
            }
        }

        private DeliveryDisposition sendAndAwait(
                protocol.Message message,
                String fromNodeId
        ) throws Exception {
            Settlement acknowledgement = new Settlement();
            boolean offered = mailbox.offer(
                    new MessageEnvelope(message, fromNodeId, acknowledgement),
                    AWAIT_SECONDS,
                    TimeUnit.SECONDS
            );
            assertTrue(offered, "Bounded scheduler mailbox did not accept test event");
            return acknowledgement.await(AWAIT_SECONDS, TimeUnit.SECONDS);
        }

        private void startScheduler(
                CoordinatorStartupRecovery.RecoveryResult recovered
        ) {
            mailbox = new ArrayBlockingQueue<>(MAILBOX_CAPACITY);
            registry = new InMemoryPeerRegistry();
            registerExecutor(registry, connectedExecutorId);
            scheduler = new TaskScheduler(
                    mailbox,
                    registry,
                    database,
                    output,
                    modelConfig(),
                    clock,
                    assignmentIds,
                    "COORDINATOR_model"
            );
            if (recovered != null) {
                scheduler.restoreJobs(
                        recovered.resumedJobs(),
                        recovered.requesterTokenHashes(),
                        recovered.requesterIdentityKeys()
                );
            }
            schedulerThread = new Thread(
                    scheduler,
                    "taskflow-model-scheduler-" + jobId
            );
            schedulerThread.start();
        }

        private void stopScheduler() throws Exception {
            if (schedulerThread == null) {
                return;
            }
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
            assertFalse(
                    schedulerThread.isAlive(),
                    "Model scheduler did not stop within the bounded join"
            );
            schedulerThread = null;
        }

        private void assertProperties() {
            DatabaseManager.JobRecord actualJob = database.getJobHistory().stream()
                    .filter(candidate -> jobId.equals(candidate.jobId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Durable job is missing"));
            Map<String, DatabaseManager.TaskRecord> actualTasks =
                    database.getTasksForJob(jobId).stream()
                            .collect(java.util.stream.Collectors.toMap(
                                    DatabaseManager.TaskRecord::taskId,
                                    task -> task,
                                    (left, right) -> left,
                                    LinkedHashMap::new
                            ));
            assertEquals(TASK_COUNT, actualTasks.size());

            assertTerminalMonotonic(actualJob.status(), actualTasks);
            assertEquals(model.jobStatus(), actualJob.status());
            for (ReferenceModel.Task expected : model.tasks()) {
                DatabaseManager.TaskRecord actual = actualTasks.get(expected.taskId());
                assertNotNull(actual, "Durable task is missing: " + expected.taskId());
                assertEquals(expected.status().name(), actual.status());
                assertEquals(expected.retryCount(), actual.retryCount());
                assertEquals(expected.attemptNumber(), actual.attemptNumber());
                if (expected.status() == ReferenceModel.TaskStatus.ASSIGNED) {
                    ReferenceModel.Assignment assignment = expected.currentAssignment();
                    assertNotNull(assignment);
                    assertEquals(assignment.assignmentId(), actual.assignmentId());
                    assertEquals(assignment.workerId(), actual.assignedPeerId());
                    assertEquals(assignment.leaseExpiresAt(), actual.leaseExpiresAt());
                }
            }

            List<JobStateStore.TaskAttemptRecord> attempts =
                    database.loadTaskAttempts(jobId);
            assertAttemptAndAuthorityProperties(actualTasks, attempts);
            assertCapacityProperties(actualTasks);
            assertOutboundIntentProperty(attempts, actualJob.status());
        }

        private void assertTerminalMonotonic(
                String actualJobStatus,
                Map<String, DatabaseManager.TaskRecord> actualTasks
        ) {
            if (isTerminal(previousActualJobStatus)) {
                assertEquals(
                        previousActualJobStatus,
                        actualJobStatus,
                        "Terminal durable job state regressed"
                );
            }
            previousActualJobStatus = actualJobStatus;

            for (DatabaseManager.TaskRecord task : actualTasks.values()) {
                String previousStatus = previousActualStatuses.put(
                        task.taskId(),
                        task.status()
                );
                if (isTerminal(previousStatus)) {
                    assertEquals(
                            previousStatus,
                            task.status(),
                            "Terminal durable task state regressed for " + task.taskId()
                    );
                }
                int previousAttempt = previousActualAttempts.getOrDefault(
                        task.taskId(),
                        0
                );
                assertTrue(
                        task.attemptNumber() >= previousAttempt,
                        "Durable attempt number decreased for " + task.taskId()
                );
                previousActualAttempts.put(task.taskId(), task.attemptNumber());
            }
        }

        private void assertAttemptAndAuthorityProperties(
                Map<String, DatabaseManager.TaskRecord> actualTasks,
                List<JobStateStore.TaskAttemptRecord> attempts
        ) {
            Set<String> assignmentIds = new LinkedHashSet<>();
            for (JobStateStore.TaskAttemptRecord attempt : attempts) {
                assertTrue(attempt.attemptNumber() > 0);
                assertTrue(
                        assignmentIds.add(attempt.assignmentId()),
                        "Assignment ID was reused: " + attempt.assignmentId()
                );
            }
            for (ReferenceModel.Task expected : model.tasks()) {
                List<JobStateStore.TaskAttemptRecord> taskAttempts = attempts.stream()
                        .filter(attempt -> expected.taskId().equals(attempt.taskId()))
                        .sorted(Comparator.comparingInt(
                                JobStateStore.TaskAttemptRecord::attemptNumber
                        ))
                        .toList();
                long authoritativeResults = taskAttempts.stream()
                        .filter(attempt -> attempt.outcome()
                                == JobStateStore.TaskAttemptOutcome.SUCCEEDED)
                        .count();
                assertTrue(
                        authoritativeResults <= 1L,
                        "Task has two authoritative result commits: " + expected.taskId()
                );
                assertEquals(expected.authoritativeResults(), authoritativeResults);
                for (int index = 1; index < taskAttempts.size(); index++) {
                    assertTrue(
                            taskAttempts.get(index).attemptNumber()
                                    > taskAttempts.get(index - 1).attemptNumber(),
                            "Attempt generations are not strictly increasing"
                    );
                }
                DatabaseManager.TaskRecord actualTask =
                        actualTasks.get(expected.taskId());
                int maximumAttempt = taskAttempts.stream()
                        .mapToInt(JobStateStore.TaskAttemptRecord::attemptNumber)
                        .max()
                        .orElse(0);
                assertEquals(actualTask.attemptNumber(), maximumAttempt);
            }
        }

        private void assertCapacityProperties(
                Map<String, DatabaseManager.TaskRecord> actualTasks
        ) {
            long assignedTasks = actualTasks.values().stream()
                    .filter(task -> "ASSIGNED".equals(task.status()))
                    .count();
            CapacityMetricsSnapshot capacity = registry.capacityMetricsSnapshot();
            assertTrue(registry.capacityProjectionValid());
            assertEquals(assignedTasks, capacity.activeReservations());
            assertEquals(assignedTasks, capacity.reservedCapacityUnits());
            assertEquals(model.assignedCount(), capacity.activeReservations());
            assertTrue(capacity.activeReservations() >= 0L);
            assertTrue(capacity.reservedCapacityUnits() >= 0L);
            int activePeerTasks = registry.getAllPeers().stream()
                    .mapToInt(peer -> {
                        assertTrue(
                                peer.getActiveTasks() >= 0,
                                "Executor active-task count became negative"
                        );
                        return peer.getActiveTasks();
                    })
                    .sum();
            assertEquals(assignedTasks, activePeerTasks);
        }

        private void assertOutboundIntentProperty(
                List<JobStateStore.TaskAttemptRecord> attempts,
                String actualJobStatus
        ) {
            Set<Long> pendingIds = database.loadPendingBrokerOutbox(256).stream()
                    .map(BrokerOutboxStore.OutboxRecord::outboxId)
                    .collect(java.util.stream.Collectors.toSet());
            for (BrokerOutboxStore.OutboxRecord record : output.records()) {
                assertTrue(
                        output.wasSuccessfullyPublished(record.outboxId())
                                || pendingIds.contains(record.outboxId()),
                        "Committed outbox row is neither published nor pending: "
                                + record.outboxId()
                );
            }
            for (JobStateStore.TaskAttemptRecord attempt : attempts) {
                BrokerOutboxStore.OutboxRecord assignment =
                        output.assignmentRecord(attempt.assignmentId());
                assertNotNull(
                        assignment,
                        "Durable assignment has no observed outbound intent: "
                                + attempt.assignmentId()
                );
            }
            if (isTerminal(actualJobStatus)) {
                assertTrue(
                        output.records().stream().anyMatch(record ->
                                record.message().message() instanceof JobResultMessage result
                                        && jobId.equals(result.getJobId())),
                        "Terminal job has no observed final-result outbound intent"
                );
            }
        }

        private void assertRecoveredProjection(
                CoordinatorStartupRecovery.RecoveryResult recovered
        ) {
            assertTrue(recovered.successful());
            assertEquals(0, recovered.failedJobs());
            if (model.terminal()) {
                assertEquals(List.of(), recovered.resumedJobs());
                assertTrue(database.loadCompletedJobResult(jobId).isPresent());
                return;
            }
            assertEquals(1, recovered.resumedJobs().size());
            EmbarrassinglyParallelJob<?, ?> job = recovered.resumedJobs().getFirst();
            assertEquals(jobId, job.getJobId());
            for (ReferenceModel.Task expected : model.tasks()) {
                TaskUnit<?> actual = job.getTasks().get(expected.taskId());
                assertNotNull(actual);
                assertEquals(expected.status().name(), actual.getStatus().name());
                assertEquals(expected.retryCount(), actual.getRetryCount());
                assertEquals(expected.attemptNumber(), actual.getAttemptNumber());
                if (expected.status() == ReferenceModel.TaskStatus.ASSIGNED) {
                    AssignmentIdentity identity =
                            actual.getAssignmentIdentity().orElseThrow();
                    assertEquals(
                            expected.currentAssignment().assignmentId(),
                            identity.assignmentId()
                    );
                    assertEquals(
                            expected.currentAssignment().workerId(),
                            identity.workerId()
                    );
                } else {
                    assertTrue(actual.getAssignmentIdentity().isEmpty());
                }
            }
        }

        private String formattedTrace() {
            return "event trace:\n" + String.join("\n", trace);
        }

        private void trace(String event, String detail) {
            trace.add("%03d %-22s %s".formatted(++traceSequence, event, detail));
        }

        @Override
        public void close() throws Exception {
            try {
                stopScheduler();
            } finally {
                if (database != null) {
                    database.close();
                }
            }
        }

        private void closeAfterConstructionFailure(Throwable constructionFailure) {
            try {
                stopScheduler();
            } catch (Exception closeFailure) {
                constructionFailure.addSuppressed(closeFailure);
            } finally {
                if (database != null) {
                    database.close();
                }
            }
        }

        private static SchedulerConfig modelConfig() {
            return SchedulerConfig.fromEnvironment(Map.of(
                    "TASKFLOW_TASK_TIMEOUT_MS", "10000",
                    "TASKFLOW_TASK_LEASE_MS", "100",
                    "TASKFLOW_MAX_TASK_RETRIES", "64",
                    "TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY",
                    String.valueOf(MAILBOX_CAPACITY)
            ));
        }

        private static void registerExecutor(
                InMemoryPeerRegistry registry,
                String executorId
        ) {
            PeerInfo peer = new PeerInfo(
                    executorId,
                    modelConfig(),
                    List.of()
            );
            PeerInfo.CapacitySnapshotOutcome outcome = peer.applyCapacityHeartbeat(
                    new PongMessage(
                            executorId,
                            Instant.EPOCH.toString(),
                            List.of(TASK_TYPE),
                            UUID.nameUUIDFromBytes(
                                    executorId.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                            ).toString(),
                            1L,
                            1,
                            1,
                            Map.of(TASK_TYPE, 1)
                    )
            );
            assertEquals(PeerInfo.CapacitySnapshotOutcome.ACCEPTED, outcome);
            registry.register(executorId, peer);
        }

        private static TaskResultMessage successfulResult(
                ReferenceModel.Assignment assignment,
                Object payload
        ) {
            return new TaskResultMessage(
                    assignment.workerId(),
                    Instant.EPOCH.toString(),
                    assignment.taskId(),
                    assignment.jobId(),
                    assignment.attemptNumber(),
                    assignment.assignmentId(),
                    payload,
                    true,
                    null
            );
        }

        private static boolean isTerminal(String status) {
            return "COMPLETED".equals(status) || "FAILED".equals(status);
        }
    }

    private static final class ReferenceModel {
        private enum TaskStatus {
            PENDING,
            ASSIGNED,
            COMPLETED,
            FAILED
        }

        private enum AssignmentDisposition {
            NEW,
            DUPLICATE,
            STALE
        }

        private enum ResultDisposition {
            COMMITTED,
            DUPLICATE,
            STALE
        }

        private final String jobId;
        private final Map<String, Task> tasks = new LinkedHashMap<>();
        private final Map<String, Assignment> assignmentsById =
                new LinkedHashMap<>();
        private final List<Assignment> staleAssignments = new ArrayList<>();
        private int assignedCount;

        private ReferenceModel(String jobId, int taskCount) {
            this.jobId = jobId;
            for (int index = 0; index < taskCount; index++) {
                String taskId = "task-" + jobId + "-" + index;
                tasks.put(taskId, new Task(taskId));
            }
        }

        private AssignmentDisposition observeAssignment(
                TaskAssignMessage message
        ) {
            Assignment incoming = Assignment.from(message);
            Assignment known = assignmentsById.get(incoming.assignmentId());
            if (known != null) {
                assertEquals(known, incoming, "Assignment identity changed on replay");
                return AssignmentDisposition.DUPLICATE;
            }

            Task task = tasks.get(incoming.taskId());
            assertNotNull(task, "Assignment targeted an unknown model task");
            if (task.status != TaskStatus.PENDING) {
                staleAssignments.add(incoming);
                assignmentsById.put(incoming.assignmentId(), incoming);
                return AssignmentDisposition.STALE;
            }
            assertEquals(task.attemptNumber + 1, incoming.attemptNumber());
            assertEquals(jobId, incoming.jobId());
            assertEquals(0, assignedCount, "Capacity-one model received two assignments");
            task.status = TaskStatus.ASSIGNED;
            task.attemptNumber = incoming.attemptNumber();
            task.currentAssignment = incoming;
            assignmentsById.put(incoming.assignmentId(), incoming);
            assignedCount++;
            return AssignmentDisposition.NEW;
        }

        private ResultDisposition applySuccessfulResult(Assignment result) {
            Task task = tasks.get(result.taskId());
            assertNotNull(task);
            if (task.authoritativeAssignmentId != null
                    && task.authoritativeAssignmentId.equals(result.assignmentId())) {
                return ResultDisposition.DUPLICATE;
            }
            if (task.status != TaskStatus.ASSIGNED
                    || !result.equals(task.currentAssignment)) {
                return ResultDisposition.STALE;
            }
            task.status = TaskStatus.COMPLETED;
            task.currentAssignment = null;
            task.authoritativeAssignmentId = result.assignmentId();
            task.authoritativeResults++;
            assignedCount--;
            assertTrue(assignedCount >= 0, "Reference capacity became negative");
            return ResultDisposition.COMMITTED;
        }

        private void applyRetryableFailure(
                Assignment assignment,
                String reason
        ) {
            Task task = tasks.get(assignment.taskId());
            assertNotNull(task);
            assertEquals(TaskStatus.ASSIGNED, task.status);
            assertEquals(task.currentAssignment, assignment);
            task.status = TaskStatus.PENDING;
            task.retryCount++;
            task.currentAssignment = null;
            staleAssignments.add(assignment);
            assignedCount--;
            assertTrue(
                    assignedCount >= 0,
                    "Reference capacity became negative after " + reason
            );
        }

        private Assignment currentAssignment() {
            return tasks.values().stream()
                    .map(Task::currentAssignment)
                    .filter(java.util.Objects::nonNull)
                    .findFirst()
                    .orElse(null);
        }

        private Assignment randomStaleAssignment(SplittableRandom random) {
            if (staleAssignments.isEmpty()) {
                return null;
            }
            return staleAssignments.get(random.nextInt(staleAssignments.size()));
        }

        private Assignment assignment(String assignmentId) {
            return assignmentsById.get(assignmentId);
        }

        private List<Task> tasks() {
            return List.copyOf(tasks.values());
        }

        private int assignedCount() {
            return assignedCount;
        }

        private boolean terminal() {
            return tasks.values().stream()
                    .allMatch(task -> task.status == TaskStatus.COMPLETED);
        }

        private String jobStatus() {
            return terminal() ? "COMPLETED" : "RUNNING";
        }

        private static final class Task {
            private final String taskId;
            private TaskStatus status = TaskStatus.PENDING;
            private int retryCount;
            private int attemptNumber;
            private Assignment currentAssignment;
            private String authoritativeAssignmentId;
            private int authoritativeResults;

            private Task(String taskId) {
                this.taskId = taskId;
            }

            private String taskId() {
                return taskId;
            }

            private TaskStatus status() {
                return status;
            }

            private int retryCount() {
                return retryCount;
            }

            private int attemptNumber() {
                return attemptNumber;
            }

            private Assignment currentAssignment() {
                return currentAssignment;
            }

            private int authoritativeResults() {
                return authoritativeResults;
            }
        }

        private record Assignment(
                String taskId,
                String jobId,
                int attemptNumber,
                String assignmentId,
                String workerId,
                long leaseExpiresAt
        ) {
            private static Assignment from(TaskAssignMessage message) {
                return new Assignment(
                        message.getTaskId(),
                        message.getJobId(),
                        message.getAttemptNumber(),
                        message.getAssignmentId(),
                        message.getNodeId(),
                        message.getLeaseExpiresAtEpochMillis()
                );
            }

        }
    }

    private static final class RecordingOutboxPublisher
            implements SchedulerOutput, BrokerOutboxPublisher {
        private final BlockingQueue<Publication> publications;
        private final Map<Long, BrokerOutboxStore.OutboxRecord> records =
                new LinkedHashMap<>();
        private final Map<String, BrokerOutboxStore.OutboxRecord>
                assignmentRecords = new LinkedHashMap<>();
        private final Map<Long, Integer> attempts = new LinkedHashMap<>();
        private final Set<Long> successfullyPublished = new LinkedHashSet<>();

        private RecordingOutboxPublisher(int capacity) {
            publications = new ArrayBlockingQueue<>(capacity);
        }

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new AssertionError("Durable scheduler bypassed the assignment outbox");
        }

        @Override
        public boolean sendJobResult(
                String requesterNodeId,
                JobResultMessage message
        ) {
            throw new AssertionError("Durable scheduler bypassed the result outbox");
        }

        @Override
        public BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(
                PeerInfo peer,
                TaskAssignMessage message
        ) {
            TaskAssignMessage routed = new TaskAssignMessage(
                    peer.getNodeId(),
                    message.getTime(),
                    message.getTaskId(),
                    message.getJobId(),
                    message.getTaskType(),
                    message.getAttemptNumber(),
                    message.getAssignmentId(),
                    message.getLeaseExpiresAtEpochMillis(),
                    message.getPayload(),
                    message.getParameter()
            );
            return new BrokerOutboxStore.OutboxMessage(
                    TransportRoute.TASK_ASSIGN,
                    peer.getNodeId(),
                    "COORDINATOR",
                    routed
            );
        }

        @Override
        public BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(
                String requesterNodeId,
                JobResultMessage message
        ) {
            return new BrokerOutboxStore.OutboxMessage(
                    TransportRoute.JOB_RESULT,
                    requesterNodeId,
                    "COORDINATOR",
                    message
            );
        }

        @Override
        public synchronized boolean publishOutbox(
                BrokerOutboxStore.OutboxRecord record
        ) {
            records.putIfAbsent(record.outboxId(), record);
            if (record.message().message() instanceof TaskAssignMessage assignment) {
                assignmentRecords.putIfAbsent(assignment.getAssignmentId(), record);
            }
            int attempt = attempts.merge(record.outboxId(), 1, Integer::sum);
            boolean successful = attempt > 1;
            if (successful) {
                successfullyPublished.add(record.outboxId());
            }
            if (!publications.offer(new Publication(record, successful))) {
                throw new AssertionError("Bounded model publication queue saturated");
            }
            return successful;
        }

        private Publication awaitPublication(long timeout, TimeUnit unit)
                throws InterruptedException {
            return publications.poll(timeout, unit);
        }

        private Publication pollPublication() {
            return publications.poll();
        }

        private synchronized BrokerOutboxStore.OutboxRecord assignmentRecord(
                String assignmentId
        ) {
            return assignmentRecords.get(assignmentId);
        }

        private synchronized boolean wasSuccessfullyPublished(
                String assignmentId
        ) {
            BrokerOutboxStore.OutboxRecord record =
                    assignmentRecords.get(assignmentId);
            return record != null && successfullyPublished.contains(record.outboxId());
        }

        private synchronized boolean wasSuccessfullyPublished(long outboxId) {
            return successfullyPublished.contains(outboxId);
        }

        private synchronized List<BrokerOutboxStore.OutboxRecord> records() {
            return List.copyOf(records.values());
        }

        private record Publication(
                BrokerOutboxStore.OutboxRecord record,
                boolean successful
        ) {
        }
    }

    private static final class Settlement implements TransportAcknowledgement {
        private final CountDownLatch settled = new CountDownLatch(1);
        private final AtomicReference<DeliveryDisposition> disposition =
                new AtomicReference<>();

        @Override
        public void settle(
                DeliveryDisposition requested,
                String reasonCode
        ) {
            disposition.compareAndSet(null, requested);
            settled.countDown();
        }

        @Override
        public void ack() {
            disposition.compareAndSet(null, DeliveryDisposition.ACK_SUCCESS);
            settled.countDown();
        }

        @Override
        public void requeue() {
            disposition.compareAndSet(null, DeliveryDisposition.RETRY_TRANSIENT);
            settled.countDown();
        }

        @Override
        public void reject() {
            disposition.compareAndSet(null, DeliveryDisposition.REJECT_INVALID);
            settled.countDown();
        }

        private DeliveryDisposition await(long timeout, TimeUnit unit)
                throws InterruptedException {
            assertTrue(settled.await(timeout, unit), "Delivery was not settled");
            return disposition.get();
        }
    }

    private static final class MutableClock implements TaskFlowClock {
        private long epochMillis;

        private MutableClock(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        private synchronized void setEpochMillis(long epochMillis) {
            assertTrue(
                    epochMillis >= this.epochMillis,
                    "Generated clock cannot move backwards"
            );
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
    }

    private static final class DeterministicAssignmentIds
            implements AssignmentIdGenerator {
        private long next;

        @Override
        public synchronized String nextAssignmentId() {
            next++;
            return UUID.nameUUIDFromBytes(
                    ("model-assignment-" + next)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
            ).toString();
        }
    }
}
