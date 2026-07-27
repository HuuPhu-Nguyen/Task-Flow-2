package server.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import transport.TransportRoute;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reusable persistence and recovery contract for a TaskFlow state-store
 * adapter.
 *
 * <p>An adapter subclass supplies only lifecycle, schema-fixture, and outbox
 * fault-injection hooks. Every behavioral test below runs unchanged against
 * the adapter's public {@link JobStateStore} and {@link BrokerOutboxStore}
 * surfaces.</p>
 */
public abstract class PersistenceContractTest {
    private static final String TASK_TYPE = "TEST_TASK";
    private static final String REQUESTER_ID = "requester-contract";
    private static final String WORKER_ID = "executor-contract";
    private static final String LEASE_OWNER = "COORDINATOR_contract";
    private static final String ASSIGNMENT_X =
            "00000000-0000-0000-0000-000000000701";
    private static final String ASSIGNMENT_Y =
            "00000000-0000-0000-0000-000000000702";

    @TempDir
    protected Path tempDir;

    /**
     * Opens one adapter instance at the supplied durable location.
     */
    protected abstract StoreHandle openStore(Path location) throws Exception;

    /**
     * Returns the adapter's currently supported durable schema version.
     */
    protected abstract int currentSchemaVersion();

    /**
     * Reads the schema version from an open adapter instance.
     */
    protected abstract int schemaVersion(StoreHandle store) throws Exception;

    /**
     * Creates accepted work at a supported previous-schema boundary.
     */
    protected abstract MigrationSeed preparePreviousSchema(Path location)
            throws Exception;

    /**
     * Installs an adapter-level fault that rejects broker-outbox writes until
     * the returned handle is closed.
     */
    protected abstract AutoCloseable failOutboxWrites(Path location)
            throws Exception;

    @Test
    void atomicJobAndTaskCreation() throws Exception {
        Path location = database("atomic-job-task");
        try (StoreHandle store = openStore(location)) {
            JobStateStore state = store.state();
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.COMMITTED,
                    commitJob(
                            state,
                            "job-existing",
                            "v1:existing",
                            task("task-shared", "existing-payload")
                    ).outcome()
            );

            JobStateStore.JobSubmissionDecision rolledBack = commitJob(
                    state,
                    "job-rolled-back",
                    "v1:rolled-back",
                    task("task-unique-before-failure", "must-roll-back"),
                    task("task-shared", "duplicate-primary-key")
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.STORAGE_FAILURE,
                    rolledBack.outcome()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.NEW_SUBMISSION,
                    inspectJob(state, "job-rolled-back", "v1:rolled-back").outcome()
            );
            assertFalse(state.hasJob("job-rolled-back"));
            assertNull(findJob(state, "job-rolled-back"));

            assertEquals(
                    JobStateStore.JobSubmissionOutcome.COMMITTED,
                    commitJob(
                            state,
                            "job-atomic",
                            "v1:atomic",
                            task("task-job-atomic-0", "payload-0"),
                            task("task-job-atomic-1", "payload-1")
                    ).outcome()
            );
            JobStateStore.ResumableJobState accepted =
                    requireJob(state, "job-atomic");
            assertEquals(
                    List.of("task-job-atomic-0", "task-job-atomic-1"),
                    accepted.tasks().stream()
                            .map(JobStateStore.ResumableTaskState::taskId)
                            .toList()
            );
            assertEquals(
                    List.of("payload-0", "payload-1"),
                    accepted.tasks().stream()
                            .map(JobStateStore.ResumableTaskState::payload)
                            .toList()
            );
        }
    }

    @Test
    void duplicateSubmissionIsIdempotentAndClassified() throws Exception {
        Path location = database("submission-idempotency");
        try (StoreHandle store = openStore(location)) {
            JobStateStore state = store.state();
            String jobId = "job-idempotent-contract";
            String requestHash = "v1:exact-request";
            List<JobStateStore.TaskStartupState> tasks = List.of(
                    task("task-job-idempotent-contract-0", "payload-0"),
                    task("task-job-idempotent-contract-1", "payload-1")
            );

            JobStateStore.JobSubmissionDecision committed =
                    commitJob(state, jobId, requestHash, tasks);
            JobStateStore.JobSubmissionDecision replayed =
                    commitJob(state, jobId, requestHash, tasks);

            assertEquals(
                    JobStateStore.JobSubmissionOutcome.COMMITTED,
                    committed.outcome()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.REPLAY,
                    replayed.outcome()
            );
            assertEquals("RUNNING", replayed.status());
            assertEquals(TASK_TYPE, replayed.taskType());
            assertEquals(2, requireJob(state, jobId).tasks().size());
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.REQUEST_CONFLICT,
                    inspectJob(state, jobId, "v1:changed-request").outcome()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.OWNER_CONFLICT,
                    state.inspectJobSubmission(
                            jobId,
                            RequesterTokens.hashToken("different-owner-token"),
                            ownerKey(jobId),
                            requestHash
                    ).outcome()
            );
        }
    }

    @Test
    void conditionalAssignmentRequiresPendingState() throws Exception {
        Path location = database("conditional-assignment");
        String jobId = "job-conditional-assignment";
        String taskId = "task-job-conditional-assignment-0";
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);

            BrokerOutboxStore.TaskAssignmentCommit committed = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_X,
                    100L,
                    1_000L
            );
            BrokerOutboxStore.TaskAssignmentCommit exactReplay = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_X,
                    100L,
                    1_000L
            );
            BrokerOutboxStore.TaskAssignmentCommit staleSecondWriter = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_Y,
                    101L,
                    1_001L
            );

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    committed.outcome()
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    exactReplay.outcome()
            );
            assertEquals(
                    committed.assignment().identity(),
                    exactReplay.assignment().identity()
            );
            assertEquals(
                    committed.assignment().outboxRecord().outboxId(),
                    exactReplay.assignment().outboxRecord().outboxId()
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    staleSecondWriter.outcome()
            );
            assertNull(staleSecondWriter.assignment());
            assertEquals(1, store.state().loadTaskAttempts(jobId).size());
            assertEquals(1, store.outbox().loadPendingBrokerOutbox(10).size());
        }
    }

    @Test
    void retryTransitionPersistsAndAdvancesAttemptNumber() throws Exception {
        Path location = database("retry-generation");
        String jobId = "job-retry-generation";
        String taskId = "task-job-retry-generation-0";
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            BrokerOutboxStore.TaskAssignmentCommit first = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_X,
                    100L,
                    1_000L
            );
            assertEquals(1, first.assignment().identity().attemptNumber());

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    store.state().commitAssignedTaskFailure(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "transient_failure",
                            200L
                    )
            );
            JobStateStore.ResumableTaskState pending =
                    requireTask(store.state(), jobId, taskId);
            assertEquals("PENDING", pending.status());
            assertEquals(1, pending.retryCount());
            assertEquals(1, pending.attemptNumber());
            assertNull(pending.assignmentId());

            BrokerOutboxStore.TaskAssignmentCommit second = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_Y,
                    300L,
                    1_300L
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    second.outcome()
            );
            assertEquals(2, second.assignment().identity().attemptNumber());
            assertEquals(
                    List.of(
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            JobStateStore.TaskAttemptOutcome.RUNNING
                    ),
                    store.state().loadTaskAttempts(jobId).stream()
                            .map(JobStateStore.TaskAttemptRecord::outcome)
                            .toList()
            );
        }
    }

    @Test
    void assignmentAndOutboxCommitAtomically() throws Exception {
        Path location = database("assignment-outbox-atomic");
        String jobId = "job-assignment-outbox-atomic";
        String taskId = "task-job-assignment-outbox-atomic-0";
        long outboxId;

        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            try (AutoCloseable fault = failOutboxWrites(location)) {
                BrokerOutboxStore.TaskAssignmentCommit rolledBack = assign(
                        store,
                        taskId,
                        jobId,
                        ASSIGNMENT_X,
                        100L,
                        1_000L
                );
                assertEquals(
                        JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                        rolledBack.outcome()
                );
                assertNull(rolledBack.assignment());
                JobStateStore.ResumableTaskState pending =
                        requireTask(store.state(), jobId, taskId);
                assertEquals("PENDING", pending.status());
                assertEquals(0, pending.attemptNumber());
                assertNull(pending.assignmentId());
                assertTrue(store.state().loadTaskAttempts(jobId).isEmpty());
                assertTrue(
                        store.outbox().loadPendingBrokerOutbox(10).isEmpty()
                );
            }

            BrokerOutboxStore.TaskAssignmentCommit committed = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_X,
                    100L,
                    1_000L
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    committed.outcome()
            );
            outboxId = committed.assignment().outboxRecord().outboxId();
            assertAssignmentMatchesOutbox(committed.assignment());
            assertCurrentAssignment(
                    store.state(),
                    jobId,
                    taskId,
                    1,
                    ASSIGNMENT_X,
                    1_000L
            );
            assertEquals(
                    List.of(outboxId),
                    store.outbox().loadPendingBrokerOutbox(10).stream()
                            .map(BrokerOutboxStore.OutboxRecord::outboxId)
                            .toList()
            );
        }

        try (StoreHandle reopened = openStore(location)) {
            assertCurrentAssignment(
                    reopened.state(),
                    jobId,
                    taskId,
                    1,
                    ASSIGNMENT_X,
                    1_000L
            );
            BrokerOutboxStore.OutboxRecord replayable =
                    reopened.outbox().loadPendingBrokerOutbox(10).getFirst();
            assertEquals(outboxId, replayable.outboxId());
            assertEquals(
                    ASSIGNMENT_X,
                    ((TaskAssignMessage) replayable.message().message())
                            .getAssignmentId()
            );
        }
    }

    @Test
    void matchingAssignmentCommitsExactlyOnce() throws Exception {
        Path location = database("matching-result-once");
        String jobId = "job-matching-result-once";
        String taskId = "task-job-matching-result-once-0";
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            assign(store, taskId, jobId, ASSIGNMENT_X, 100L, 1_000L);

            JobStateStore.ResultCommitOutcome committed =
                    store.state().commitTaskResult(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            200L,
                            100L,
                            "authoritative-result"
                    );
            JobStateStore.ResultCommitOutcome duplicate =
                    store.state().commitTaskResult(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            201L,
                            101L,
                            "authoritative-result"
                    );

            assertEquals(JobStateStore.ResultCommitOutcome.COMMITTED, committed);
            assertEquals(
                    JobStateStore.ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED,
                    duplicate
            );
            List<JobStateStore.TaskAttemptRecord> attempts =
                    store.state().loadTaskAttempts(jobId);
            assertEquals(1, attempts.size());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.SUCCEEDED,
                    attempts.getFirst().outcome()
            );
            JobStateStore.ResumableTaskState completed =
                    requireTask(store.state(), jobId, taskId);
            assertEquals("COMPLETED", completed.status());
            assertEquals("authoritative-result", completed.resultPayload());
        }
    }

    @Test
    void staleResultIsClassifiedWithoutReplacingCurrentAssignment()
            throws Exception {
        Path location = database("stale-result");
        String jobId = "job-stale-result-contract";
        String taskId = "task-job-stale-result-contract-0";
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            assign(store, taskId, jobId, ASSIGNMENT_X, 100L, 1_000L);
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    store.state().commitAssignedTaskFailure(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "lease_expired",
                            1_000L
                    )
            );
            assign(store, taskId, jobId, ASSIGNMENT_Y, 1_000L, 2_000L);

            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    store.state().commitTaskResult(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            1_100L,
                            1_000L,
                            "obsolete-result"
                    )
            );
            assertCurrentAssignment(
                    store.state(),
                    jobId,
                    taskId,
                    2,
                    ASSIGNMENT_Y,
                    2_000L
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    store.state().commitTaskResult(
                            taskId,
                            2,
                            ASSIGNMENT_Y,
                            WORKER_ID,
                            1_200L,
                            200L,
                            "current-result"
                    )
            );
            assertEquals(
                    List.of(
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            JobStateStore.TaskAttemptOutcome.SUCCEEDED
                    ),
                    store.state().loadTaskAttempts(jobId).stream()
                            .map(JobStateStore.TaskAttemptRecord::outcome)
                            .toList()
            );
        }
    }

    @Test
    void terminalStatesAreMonotonic() throws Exception {
        Path location = database("terminal-monotonic");
        String jobId = "job-terminal-monotonic";
        String taskId = "task-job-terminal-monotonic-0";
        Object aggregate = Map.of("joined", "authoritative-result");
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            assign(store, taskId, jobId, ASSIGNMENT_X, 100L, 1_000L);
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    store.state().commitTaskResult(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            200L,
                            100L,
                            "authoritative-result"
                    )
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    store.outbox().commitJobCompletedAndEnqueueBrokerOutbox(
                            jobId,
                            aggregate,
                            jobResultOutbox(jobId, aggregate)
                    ).outcome()
            );

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    store.state().commitJobCompleted(jobId, aggregate, 300L)
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    store.state().commitJobCompleted(
                            jobId,
                            Map.of("joined", "different"),
                            301L
                    )
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    store.state().commitJobFailed(jobId, List.of(), 302L)
            );
            assertEquals(
                    aggregate,
                    store.state().loadCompletedJobResult(jobId)
                            .orElseThrow()
                            .resultPayload()
            );
        }
    }

    @Test
    void finalJobAndFinalResultOutboxCommitAtomically() throws Exception {
        Path location = database("final-outbox-atomic");
        String jobId = "job-final-outbox-atomic";
        String taskId = "task-job-final-outbox-atomic-0";
        Object aggregate = Map.of("aggregate", "final-result");
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            BrokerOutboxStore.TaskAssignmentCommit assignment = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_X,
                    100L,
                    1_000L
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    store.state().commitTaskResult(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            200L,
                            100L,
                            "task-result"
                    )
            );
            assertTrue(store.outbox().markBrokerOutboxPublished(
                    assignment.assignment().outboxRecord().outboxId(),
                    250L
            ));
            BrokerOutboxStore.OutboxMessage message =
                    jobResultOutbox(jobId, aggregate);

            try (AutoCloseable fault = failOutboxWrites(location)) {
                BrokerOutboxStore.OutboxCommit rolledBack =
                        store.outbox().commitJobCompletedAndEnqueueBrokerOutbox(
                                jobId,
                                aggregate,
                                message
                        );
                assertEquals(
                        JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                        rolledBack.outcome()
                );
                assertNull(rolledBack.outboxRecord());
                assertTrue(
                        store.state().loadCompletedJobResult(jobId).isEmpty()
                );
                JobStateStore.ResumableTaskState task =
                        requireTask(store.state(), jobId, taskId);
                assertEquals("COMPLETED", task.status());
                assertEquals("task-result", task.resultPayload());
                assertTrue(
                        store.outbox().loadPendingBrokerOutbox(10).isEmpty()
                );
            }

            BrokerOutboxStore.OutboxCommit committed =
                    store.outbox().commitJobCompletedAndEnqueueBrokerOutbox(
                            jobId,
                            aggregate,
                            message
                    );
            BrokerOutboxStore.OutboxCommit replayed =
                    store.outbox().commitJobCompletedAndEnqueueBrokerOutbox(
                            jobId,
                            aggregate,
                            message
                    );

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    committed.outcome()
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    replayed.outcome()
            );
            assertEquals(
                    committed.outboxRecord().outboxId(),
                    replayed.outboxRecord().outboxId()
            );
            assertEquals(
                    aggregate,
                    store.state().loadCompletedJobResult(jobId)
                            .orElseThrow()
                            .resultPayload()
            );
            assertEquals(
                    List.of(committed.outboxRecord().outboxId()),
                    store.outbox().loadPendingBrokerOutbox(10).stream()
                            .filter(record -> record.message().route()
                                    == TransportRoute.JOB_RESULT)
                            .map(BrokerOutboxStore.OutboxRecord::outboxId)
                            .toList()
            );
        }
    }

    @Test
    void acceptedJobSurvivesCoordinatorRestart() throws Exception {
        Path location = database("accepted-restart");
        String jobId = "job-accepted-restart";
        String requestHash = "v1:accepted-restart";
        try (StoreHandle store = openStore(location)) {
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.COMMITTED,
                    commitJob(
                            store.state(),
                            jobId,
                            requestHash,
                            task("task-job-accepted-restart-0", "payload-0"),
                            task("task-job-accepted-restart-1", "payload-1")
                    ).outcome()
            );
        }

        try (StoreHandle reopened = openStore(location)) {
            JobStateStore.ResumableJobState recovered =
                    requireJob(reopened.state(), jobId);
            assertEquals(TASK_TYPE, recovered.taskType());
            assertEquals(REQUESTER_ID, recovered.requesterId());
            assertEquals(
                    List.of("payload-0", "payload-1"),
                    recovered.tasks().stream()
                            .map(JobStateStore.ResumableTaskState::payload)
                            .toList()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.REPLAY,
                    inspectJob(reopened.state(), jobId, requestHash).outcome()
            );
        }
    }

    @Test
    void restartPreservesUnexpiredLease() throws Exception {
        Path location = database("unexpired-lease-restart");
        String jobId = "job-unexpired-lease-restart";
        String taskId = "task-job-unexpired-lease-restart-0";
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            assign(store, taskId, jobId, ASSIGNMENT_X, 100L, 1_000L);
        }

        try (StoreHandle reopened = openStore(location)) {
            assertFalse(
                    reopened.state().releaseExpiredTaskLeaseForResume(
                            taskId,
                            999L,
                            1
                    )
            );
            assertCurrentAssignment(
                    reopened.state(),
                    jobId,
                    taskId,
                    1,
                    ASSIGNMENT_X,
                    1_000L
            );
            JobStateStore.TaskAttemptRecord attempt =
                    reopened.state().loadTaskAttempts(jobId).getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempt.outcome());
            assertEquals(0L, attempt.finishedAt());
        }
    }

    @Test
    void restartReleasesExpiredLeaseAndFencesOldResult() throws Exception {
        Path location = database("expired-lease-restart");
        String jobId = "job-expired-lease-restart";
        String taskId = "task-job-expired-lease-restart-0";
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            assign(store, taskId, jobId, ASSIGNMENT_X, 100L, 1_000L);
        }

        try (StoreHandle reopened = openStore(location)) {
            assertTrue(
                    reopened.state().releaseExpiredTaskLeaseForResume(
                            taskId,
                            1_000L,
                            1
                    )
            );
            BrokerOutboxStore.TaskAssignmentCommit replacement = assign(
                    reopened,
                    taskId,
                    jobId,
                    ASSIGNMENT_Y,
                    1_000L,
                    2_000L
            );
            assertEquals(2, replacement.assignment().identity().attemptNumber());
            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    reopened.state().commitTaskResult(
                            taskId,
                            1,
                            ASSIGNMENT_X,
                            WORKER_ID,
                            1_100L,
                            1_000L,
                            "obsolete-result"
                    )
            );
            assertCurrentAssignment(
                    reopened.state(),
                    jobId,
                    taskId,
                    2,
                    ASSIGNMENT_Y,
                    2_000L
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    reopened.state().commitTaskResult(
                            taskId,
                            2,
                            ASSIGNMENT_Y,
                            WORKER_ID,
                            1_200L,
                            200L,
                            "current-result"
                    )
            );
        }
    }

    @Test
    void schemaMigrationPreservesAcceptedWork() throws Exception {
        Path location = database("schema-migration");
        MigrationSeed seed = preparePreviousSchema(location);
        assertTrue(seed.previousSchemaVersion() < currentSchemaVersion());

        try (StoreHandle migrated = openStore(location)) {
            assertEquals(currentSchemaVersion(), schemaVersion(migrated));
            JobStateStore.ResumableJobState job =
                    requireJob(migrated.state(), seed.jobId());
            JobStateStore.ResumableTaskState task =
                    requireTask(migrated.state(), seed.jobId(), seed.taskId());
            assertEquals(seed.payload(), task.payload());
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.REPLAY,
                    migrated.state().inspectJobSubmission(
                            seed.jobId(),
                            seed.requesterTokenHash(),
                            seed.requesterIdentityKey(),
                            seed.requestHash()
                    ).outcome()
            );
            assertEquals(seed.taskId(), job.tasks().getFirst().taskId());
        }
    }

    @Test
    void outboxReplayStateTransitionsSurviveRestart() throws Exception {
        Path location = database("outbox-replay-state");
        String jobId = "job-outbox-replay-state";
        String taskId = "task-job-outbox-replay-state-0";
        long outboxId;
        try (StoreHandle store = openStore(location)) {
            commitSingleTaskJob(store.state(), jobId, taskId);
            BrokerOutboxStore.TaskAssignmentCommit assignment = assign(
                    store,
                    taskId,
                    jobId,
                    ASSIGNMENT_X,
                    100L,
                    1_000L
            );
            outboxId = assignment.assignment().outboxRecord().outboxId();
            assertTrue(
                    store.outbox().markBrokerOutboxPublishFailed(
                            outboxId,
                            "injected_publish_failure",
                            500L
                    )
            );
            BrokerOutboxStore.OutboxRecord failed =
                    store.outbox().loadPendingBrokerOutbox(10).getFirst();
            assertEquals(outboxId, failed.outboxId());
            assertEquals(1, failed.attemptCount());
            assertEquals(500L, failed.lastAttemptAt());
            assertEquals("injected_publish_failure", failed.lastError());
        }

        try (StoreHandle reopened = openStore(location)) {
            BrokerOutboxStore.OutboxRecord replayable =
                    reopened.outbox().loadPendingBrokerOutbox(10).getFirst();
            assertEquals(outboxId, replayable.outboxId());
            assertEquals(1, replayable.attemptCount());
            assertEquals(500L, replayable.lastAttemptAt());
            assertTrue(reopened.outbox().markBrokerOutboxPublished(outboxId, 600L));
            assertTrue(reopened.outbox().loadPendingBrokerOutbox(10).isEmpty());
            assertEquals(
                    BrokerOutboxStore.PendingOutboxCount.counted(0L),
                    reopened.outbox().countPendingBrokerOutbox()
            );
        }

        try (StoreHandle published = openStore(location)) {
            assertTrue(published.outbox().loadPendingBrokerOutbox(10).isEmpty());
            assertEquals(
                    BrokerOutboxStore.PendingOutboxCount.counted(0L),
                    published.outbox().countPendingBrokerOutbox()
            );
        }
    }

    protected static StoreHandle storeHandle(
            JobStateStore state,
            BrokerOutboxStore outbox,
            AutoCloseable closeable
    ) {
        return new StoreHandle(state, outbox, closeable);
    }

    protected record MigrationSeed(
            int previousSchemaVersion,
            String jobId,
            String taskId,
            Object payload,
            String requesterTokenHash,
            String requesterIdentityKey,
            String requestHash
    ) {
        public MigrationSeed {
            if (previousSchemaVersion < 1) {
                throw new IllegalArgumentException(
                        "previousSchemaVersion must be positive"
                );
            }
            assertRequired(jobId, "jobId");
            assertRequired(taskId, "taskId");
            assertRequired(requesterTokenHash, "requesterTokenHash");
            assertRequired(requesterIdentityKey, "requesterIdentityKey");
            assertRequired(requestHash, "requestHash");
        }
    }

    protected static final class StoreHandle implements AutoCloseable {
        private final JobStateStore state;
        private final BrokerOutboxStore outbox;
        private final AutoCloseable closeable;

        private StoreHandle(
                JobStateStore state,
                BrokerOutboxStore outbox,
                AutoCloseable closeable
        ) {
            this.state = java.util.Objects.requireNonNull(state, "state");
            this.outbox = java.util.Objects.requireNonNull(outbox, "outbox");
            this.closeable =
                    java.util.Objects.requireNonNull(closeable, "closeable");
        }

        public JobStateStore state() {
            return state;
        }

        public BrokerOutboxStore outbox() {
            return outbox;
        }

        @Override
        public void close() throws Exception {
            closeable.close();
        }
    }

    private Path database(String name) {
        return tempDir.resolve(name + ".db");
    }

    private static JobStateStore.JobSubmissionDecision commitSingleTaskJob(
            JobStateStore state,
            String jobId,
            String taskId
    ) {
        JobStateStore.JobSubmissionDecision decision = commitJob(
                state,
                jobId,
                "v1:" + jobId,
                task(taskId, "payload")
        );
        assertEquals(
                JobStateStore.JobSubmissionOutcome.COMMITTED,
                decision.outcome()
        );
        return decision;
    }

    private static JobStateStore.JobSubmissionDecision commitJob(
            JobStateStore state,
            String jobId,
            String requestHash,
            JobStateStore.TaskStartupState... tasks
    ) {
        return commitJob(state, jobId, requestHash, Arrays.asList(tasks));
    }

    private static JobStateStore.JobSubmissionDecision commitJob(
            JobStateStore state,
            String jobId,
            String requestHash,
            List<JobStateStore.TaskStartupState> tasks
    ) {
        return state.commitJobSubmission(
                jobId,
                TASK_TYPE,
                REQUESTER_ID,
                tokenHash(jobId),
                ownerKey(jobId),
                requestHash,
                "",
                tasks
        );
    }

    private static JobStateStore.JobSubmissionDecision inspectJob(
            JobStateStore state,
            String jobId,
            String requestHash
    ) {
        return state.inspectJobSubmission(
                jobId,
                tokenHash(jobId),
                ownerKey(jobId),
                requestHash
        );
    }

    private static JobStateStore.TaskStartupState task(
            String taskId,
            Object payload
    ) {
        return new JobStateStore.TaskStartupState(taskId, payload);
    }

    private static String tokenHash(String jobId) {
        return RequesterTokens.hashToken("token-" + jobId);
    }

    private static String ownerKey(String jobId) {
        return "owner-key-" + jobId;
    }

    private static BrokerOutboxStore.TaskAssignmentCommit assign(
            StoreHandle store,
            String taskId,
            String jobId,
            String assignmentId,
            long startedAt,
            long leaseExpiresAt
    ) {
        return store.outbox().commitTaskAssignmentAndEnqueueBrokerOutbox(
                taskId,
                WORKER_ID,
                startedAt,
                LEASE_OWNER,
                leaseExpiresAt,
                assignmentId,
                assignmentOutbox(taskId, jobId)
        );
    }

    private static BrokerOutboxStore.OutboxMessage assignmentOutbox(
            String taskId,
            String jobId
    ) {
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.TASK_ASSIGN,
                WORKER_ID,
                LEASE_OWNER,
                new TaskAssignMessage(
                        WORKER_ID,
                        "2026-07-28T00:00:00Z",
                        taskId,
                        jobId,
                        TASK_TYPE,
                        "payload",
                        ""
                )
        );
    }

    private static BrokerOutboxStore.OutboxMessage jobResultOutbox(
            String jobId,
            Object aggregate
    ) {
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.JOB_RESULT,
                REQUESTER_ID,
                LEASE_OWNER,
                new JobResultMessage(
                        LEASE_OWNER,
                        "2026-07-28T00:00:00Z",
                        jobId,
                        TASK_TYPE,
                        true,
                        aggregate,
                        List.of("task-result")
                )
        );
    }

    private static JobStateStore.ResumableJobState requireJob(
            JobStateStore state,
            String jobId
    ) {
        JobStateStore.ResumableJobState job = findJob(state, jobId);
        assertNotNull(job, "Expected resumable job " + jobId);
        return job;
    }

    private static JobStateStore.ResumableJobState findJob(
            JobStateStore state,
            String jobId
    ) {
        return state.loadRunningJobsForResume().stream()
                .filter(job -> jobId.equals(job.jobId()))
                .findFirst()
                .orElse(null);
    }

    private static JobStateStore.ResumableTaskState requireTask(
            JobStateStore state,
            String jobId,
            String taskId
    ) {
        return requireJob(state, jobId).tasks().stream()
                .filter(task -> taskId.equals(task.taskId()))
                .findFirst()
                .orElseThrow();
    }

    private static void assertCurrentAssignment(
            JobStateStore state,
            String jobId,
            String taskId,
            int attemptNumber,
            String assignmentId,
            long leaseExpiresAt
    ) {
        JobStateStore.ResumableTaskState task =
                requireTask(state, jobId, taskId);
        assertEquals("ASSIGNED", task.status());
        assertEquals(WORKER_ID, task.assignedPeerId());
        assertEquals(LEASE_OWNER, task.leaseOwnerId());
        assertEquals(attemptNumber, task.attemptNumber());
        assertEquals(assignmentId, task.assignmentId());
        assertEquals(leaseExpiresAt, task.leaseExpiresAt());
    }

    private static void assertAssignmentMatchesOutbox(
            BrokerOutboxStore.CommittedTaskAssignment committed
    ) {
        assertNotNull(committed);
        BrokerOutboxStore.OutboxRecord outbox = committed.outboxRecord();
        assertEquals(TransportRoute.TASK_ASSIGN, outbox.message().route());
        assertEquals(WORKER_ID, outbox.message().peerNodeId());
        TaskAssignMessage message =
                (TaskAssignMessage) outbox.message().message();
        assertEquals(committed.identity().taskId(), message.getTaskId());
        assertEquals(
                committed.identity().attemptNumber(),
                message.getAttemptNumber()
        );
        assertEquals(
                committed.identity().assignmentId(),
                message.getAssignmentId()
        );
        assertEquals(
                committed.identity().leaseExpiresAtEpochMillis(),
                message.getLeaseExpiresAtEpochMillis()
        );
    }

    private static void assertRequired(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }
}
