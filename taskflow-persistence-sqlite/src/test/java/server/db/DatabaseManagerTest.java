package server.db;

import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import server.registry.PeerMetricsSnapshot;
import server.registry.PeerRegistryRecord;
import server.registry.PeerStatus;
import server.registry.PeerTransport;
import transport.TransportRoute;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseManagerTest {
    private static final String ASSIGNMENT_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final String SECOND_ASSIGNMENT_ID = "550e8400-e29b-41d4-a716-446655440001";
    private static final String OTHER_ASSIGNMENT_ID = "550e8400-e29b-41d4-a716-446655440002";

    @TempDir
    Path tempDir;

    @Test
    void persistsJobAndTaskLifecycleToConfiguredDatabasePath() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-1", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-1", "job-1");
            db.markTaskAssigned("task-1", "peer-1", 123L);
            db.markTaskCompleted("task-1", 456L, 333L, "result");
            db.markJobCompleted("job-1");

            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, db.getSchemaVersion());
            assertTrue(db.hasJob("job-1"));
            assertFalse(db.hasJob("missing-job"));

            List<DatabaseManager.JobRecord> jobs = db.getJobHistory();
            assertEquals(1, jobs.size());
            DatabaseManager.JobRecord job = jobs.getFirst();
            assertEquals("job-1", job.jobId());
            assertEquals("TEST_TASK", job.taskType());
            assertEquals("requester-1", job.requesterId());
            assertEquals("COMPLETED", job.status());
            assertEquals(1, job.fileCount());

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-1");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("task-1", task.taskId());
            assertEquals("job-1", task.jobId());
            assertEquals("peer-1", task.assignedPeerId());
            assertEquals("COMPLETED", task.status());
            assertEquals(123L, task.startedAt());
            assertEquals(456L, task.completedAt());
            assertEquals(333L, task.durationMs());
            assertEquals(0, task.retryCount());
            assertEquals("", task.leaseOwnerId());
            assertEquals(0L, task.leaseExpiresAt());
            assertEquals(1, task.attemptNumber());
            assertNotNull(task.assignmentId());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-1");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals("job-1", attempt.jobId());
            assertEquals("task-1", attempt.taskId());
            assertEquals(1, attempt.attemptNumber());
            assertEquals(task.assignmentId(), attempt.assignmentId());
            assertEquals("peer-1", attempt.peerId());
            assertEquals(123L, attempt.startedAt());
            assertEquals(0L, attempt.leaseExpiresAt());
            assertEquals(456L, attempt.finishedAt());
            assertEquals(333L, attempt.durationMs());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, attempt.outcome());
            assertEquals("", attempt.failureReason());
        } finally {
            db.close();
        }
    }

    @Test
    void submissionCommitIsTypedAndDeterministicAcrossRestart() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-submission-idempotency.db");
        String tokenHash = RequesterTokens.hashToken("owner-token");
        String requestHash = "v1:canonical-request";

        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.COMMITTED,
                    db.commitJobSubmission(
                            "job-idempotent",
                            "TEST_TASK",
                            "requester-route-1",
                            tokenHash,
                            "owner-key",
                            requestHash,
                            "parameter",
                            List.of(new JobStateStore.TaskStartupState("task-job-idempotent-0", "payload"))
                    ).outcome()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.REPLAY,
                    db.inspectJobSubmission("job-idempotent", tokenHash, "owner-key", requestHash).outcome()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.REQUEST_CONFLICT,
                    db.inspectJobSubmission("job-idempotent", tokenHash, "owner-key", "v1:different").outcome()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.OWNER_CONFLICT,
                    db.inspectJobSubmission(
                            "job-idempotent",
                            RequesterTokens.hashToken("other-token"),
                            "owner-key",
                            requestHash
                    ).outcome()
            );
            assertEquals(1, db.getTasksForJob("job-idempotent").size());
        }

        try (DatabaseManager restarted = new DatabaseManager(dbPath.toString())) {
            JobStateStore.JobSubmissionDecision replay = restarted.inspectJobSubmission(
                    "job-idempotent",
                    tokenHash,
                    "owner-key",
                    requestHash
            );
            assertEquals(JobStateStore.JobSubmissionOutcome.REPLAY, replay.outcome());
            assertEquals("RUNNING", replay.status());
            assertEquals("TEST_TASK", replay.taskType());
            assertEquals(1, restarted.getTasksForJob("job-idempotent").size());
        }
    }

    @Test
    void concurrentIdenticalSubmissionCommitsOneJobAndOneTaskSet() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-concurrent-submission-idempotency.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            CountDownLatch start = new CountDownLatch(1);
            try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
                List<Future<JobStateStore.JobSubmissionOutcome>> outcomes = List.of(
                        executor.submit(() -> commitAfterStart(db, start)),
                        executor.submit(() -> commitAfterStart(db, start))
                );
                start.countDown();

                Set<JobStateStore.JobSubmissionOutcome> distinct = Set.of(
                        outcomes.get(0).get(2, TimeUnit.SECONDS),
                        outcomes.get(1).get(2, TimeUnit.SECONDS)
                );
                assertEquals(Set.of(
                        JobStateStore.JobSubmissionOutcome.COMMITTED,
                        JobStateStore.JobSubmissionOutcome.REPLAY
                ), distinct);
            }

            assertEquals(1, db.getJobHistory().size());
            assertEquals(2, db.getTasksForJob("job-concurrent-submit").size());
        }
    }

    @Test
    void failedTaskInsertRollsBackSubmissionHashAndJobTogether() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-submission-hash-rollback.db");
        String tokenHash = RequesterTokens.hashToken("owner-token");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertTrue(db.insertJob("existing-job", "TEST_TASK", "requester", 1));
            assertTrue(db.insertTask("shared-task-id", "existing-job"));

            assertEquals(
                    JobStateStore.JobSubmissionOutcome.STORAGE_FAILURE,
                    db.commitJobSubmission(
                            "rolled-back-job",
                            "TEST_TASK",
                            "requester",
                            tokenHash,
                            "owner-key",
                            "v1:must-roll-back",
                            "",
                            List.of(new JobStateStore.TaskStartupState("shared-task-id", "payload"))
                    ).outcome()
            );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.NEW_SUBMISSION,
                    db.inspectJobSubmission(
                            "rolled-back-job",
                            tokenHash,
                            "owner-key",
                            "v1:must-roll-back"
                    ).outcome()
            );
            assertTrue(db.getJobHistory().stream().noneMatch(job -> job.jobId().equals("rolled-back-job")));
            assertTrue(db.getTasksForJob("rolled-back-job").isEmpty());
        }
    }

    @Test
    void taskAssignmentStorageFaultPreservesPendingStateAndReplayIsTyped() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-assignment-storage-fault.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-assignment-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-assignment-storage", "job-assignment-storage");
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_task_assignment_commit
                        BEFORE UPDATE OF status ON tasks
                        WHEN NEW.status='ASSIGNED'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected assignment commit');
                        END
                        """);
            }

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                    db.commitTaskAssignment(
                            "task-assignment-storage",
                            "peer-1",
                            100L,
                            "coordinator-lease",
                            900L,
                            1,
                            ASSIGNMENT_ID
                    )
            );
            DatabaseManager.TaskRecord pending = db.getTasksForJob("job-assignment-storage").getFirst();
            assertEquals("PENDING", pending.status());
            assertEquals(0, pending.attemptNumber());
            assertNull(pending.assignmentId());
            assertTrue(db.loadTaskAttempts("job-assignment-storage").isEmpty());

            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("DROP TRIGGER fail_task_assignment_commit");
            }
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitTaskAssignment(
                            "task-assignment-storage",
                            "peer-1",
                            100L,
                            "coordinator-lease",
                            900L,
                            1,
                            ASSIGNMENT_ID
                    )
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    db.commitTaskAssignment(
                            "task-assignment-storage",
                            "peer-1",
                            100L,
                            "coordinator-lease",
                            900L,
                            1,
                            ASSIGNMENT_ID
                    )
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    db.commitTaskAssignment(
                            "task-assignment-storage",
                            "peer-1",
                            101L,
                            "different-lease",
                            901L,
                            1,
                            ASSIGNMENT_ID
                    )
            );
            assertEquals("ASSIGNED", db.getTasksForJob("job-assignment-storage").getFirst().status());
            assertEquals(1, db.loadTaskAttempts("job-assignment-storage").size());
        }
    }

    @Test
    void matchingAssignmentCommitsExactlyOnceAndDuplicateIsTyped() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-result-commit-once.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-result-once", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-result-once", "job-result-once");
            assertTrue(db.markTaskAssigned(
                    "task-result-once",
                    "peer-1",
                    100L,
                    "coordinator-lease",
                    900L,
                    1,
                    ASSIGNMENT_ID
            ));

            assertEquals(
                    JobStateStore.ResultCommitOutcome.UNKNOWN_TASK,
                    db.commitTaskResult(
                            "missing-task",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            "missing"
                    )
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            "task-result-once",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            "first-result"
                    )
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED,
                    db.commitTaskResult(
                            "task-result-once",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            300L,
                            200L,
                            "duplicate-result"
                    )
            );

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-result-once").getFirst();
            assertEquals("COMPLETED", task.status());
            assertEquals("FINALIZING", db.getJobHistory().getFirst().status());
            assertEquals(200L, task.completedAt());
            assertEquals(100L, task.durationMs());
            assertEquals(1, task.attemptNumber());
            assertEquals(ASSIGNMENT_ID, task.assignmentId());
            assertEquals(
                    "first-result",
                    db.loadRunningJobsForResume().getFirst().tasks().getFirst().resultPayload()
            );
            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-result-once");
            assertEquals(1, attempts.size());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, attempts.getFirst().outcome());
            assertEquals(200L, attempts.getFirst().finishedAt());
        }
    }

    @Test
    void jsonNullTaskResultRemainsPresentForFinalizationRecovery() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-null-result-finalization.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-null-result", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-null-result", "job-null-result");
            assertTrue(db.markTaskAssigned(
                    "task-null-result",
                    "peer-1",
                    100L,
                    "lease",
                    900L,
                    1,
                    ASSIGNMENT_ID
            ));

            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            "task-null-result",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            null
                    )
            );

            assertEquals("FINALIZING", db.getJobHistory().getFirst().status());
            JobStateStore.ResumableTaskState restored = db.loadRunningJobsForResume()
                    .getFirst()
                    .tasks()
                    .getFirst();
            assertNull(restored.resultPayload());
            assertTrue(restored.resultPayloadPresent());
        }
    }

    @Test
    void conditionalResultCommitRejectsOldAttemptWrongAssignmentAndWrongWorker() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-result-commit-stale.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-result-stale", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-result-stale", "job-result-stale");
            assertTrue(db.markTaskAssigned(
                    "task-result-stale",
                    "peer-old",
                    50L,
                    "old-lease",
                    80L,
                    1,
                    ASSIGNMENT_ID
            ));
            assertTrue(db.markTaskRetried(
                    "task-result-stale",
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "old_attempt_finished",
                    90L
            ));
            assertTrue(db.markTaskAssigned(
                    "task-result-stale",
                    "peer-1",
                    100L,
                    "coordinator-lease",
                    900L,
                    2,
                    SECOND_ASSIGNMENT_ID
            ));

            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    db.commitTaskResult(
                            "task-result-stale", 1, SECOND_ASSIGNMENT_ID, "peer-1", 200L, 100L, "old-attempt")
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    db.commitTaskResult(
                            "task-result-stale", 2, OTHER_ASSIGNMENT_ID, "peer-1", 200L, 100L, "wrong-id")
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    db.commitTaskResult(
                            "task-result-stale", 2, SECOND_ASSIGNMENT_ID, "peer-2", 200L, 100L, "wrong-worker")
            );

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-result-stale").getFirst();
            assertEquals("ASSIGNED", task.status());
            assertEquals("peer-1", task.assignedPeerId());
            assertEquals(2, task.attemptNumber());
            assertEquals(SECOND_ASSIGNMENT_ID, task.assignmentId());
            assertEquals(900L, task.leaseExpiresAt());
            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-result-stale");
            assertEquals(2, attempts.size());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, attempts.getFirst().outcome());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempts.get(1).outcome());
        }
    }

    @Test
    void sameWorkerAbaResultIsStaleAtStoreBoundary() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-result-commit-aba.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-result-aba", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-result-aba", "job-result-aba");
            assertTrue(db.markTaskAssigned(
                    "task-result-aba", "peer-a", 100L, "lease-1", 500L, 1, ASSIGNMENT_ID));
            assertTrue(db.markTaskRetried(
                    "task-result-aba",
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "lease_expired",
                    150L
            ));
            assertTrue(db.markTaskAssigned(
                    "task-result-aba", "peer-a", 200L, "lease-2", 900L, 2, SECOND_ASSIGNMENT_ID));

            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    db.commitTaskResult(
                            "task-result-aba", 1, ASSIGNMENT_ID, "peer-a", 250L, 150L, "old-result")
            );
            DatabaseManager.TaskRecord afterOldResult = db.getTasksForJob("job-result-aba").getFirst();
            assertEquals("ASSIGNED", afterOldResult.status());
            assertEquals(2, afterOldResult.attemptNumber());
            assertEquals(SECOND_ASSIGNMENT_ID, afterOldResult.assignmentId());
            assertEquals("peer-a", afterOldResult.assignedPeerId());
            assertEquals(900L, afterOldResult.leaseExpiresAt());
            assertNull(db.loadRunningJobsForResume().getFirst().tasks().getFirst().resultPayload());

            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            "task-result-aba", 2, SECOND_ASSIGNMENT_ID, "peer-a", 300L, 100L, "current-result")
            );
            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-result-aba");
            assertEquals(2, attempts.size());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, attempts.getFirst().outcome());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, attempts.get(1).outcome());
            assertEquals(
                    "current-result",
                    db.loadRunningJobsForResume().getFirst().tasks().getFirst().resultPayload()
            );
        }
    }

    @Test
    void staleAttemptOutputReferenceCannotReplaceAuthoritativePointer() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-result-output-pointer.db");
        String jobId = "job-result-output-pointer";
        String taskId = "task-result-output-pointer";
        ObjectReference firstAttemptReference = outputReference(
                jobId,
                taskId,
                1,
                ASSIGNMENT_ID,
                "1"
        );
        ObjectReference secondAttemptReference = outputReference(
                jobId,
                taskId,
                2,
                SECOND_ASSIGNMENT_ID,
                "2"
        );

        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob(jobId, "TEST_TASK", "requester-1", 1);
            db.insertTask(taskId, jobId);
            assertTrue(db.markTaskAssigned(
                    taskId, "peer-1", 100L, "lease-1", 500L, 1, ASSIGNMENT_ID));
            assertTrue(db.markTaskRetried(
                    taskId,
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "lease_expired",
                    150L
            ));
            assertTrue(db.markTaskAssigned(
                    taskId,
                    "peer-1",
                    200L,
                    "lease-2",
                    900L,
                    2,
                    SECOND_ASSIGNMENT_ID
            ));

            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    db.commitTaskResult(
                            taskId,
                            2,
                            SECOND_ASSIGNMENT_ID,
                            "peer-1",
                            250L,
                            50L,
                            Map.of("objectReference", firstAttemptReference)
                    )
            );
            assertNull(db.loadRunningJobsForResume()
                    .getFirst().tasks().getFirst().resultPayload());

            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            taskId,
                            2,
                            SECOND_ASSIGNMENT_ID,
                            "peer-1",
                            300L,
                            100L,
                            Map.of("objectReference", secondAttemptReference)
                    )
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                    db.commitTaskResult(
                            taskId,
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            350L,
                            250L,
                            Map.of("objectReference", firstAttemptReference)
                    )
            );
        }

        try (DatabaseManager restarted = new DatabaseManager(dbPath.toString())) {
            Object restored = restarted.loadRunningJobsForResume()
                    .getFirst().tasks().getFirst().resultPayload();
            assertInstanceOf(Map.class, restored);
            Object nested = ((Map<?, ?>) restored).get("objectReference");
            assertInstanceOf(Map.class, nested);
            assertEquals(
                    secondAttemptReference.key(),
                    ((Map<?, ?>) nested).get("key")
            );
        }
    }

    @Test
    void classifiesExactAttemptOutputAgainstActiveAndAuthoritativeState()
            throws Exception {
        Path dbPath = tempDir.resolve("taskflow-output-gc-classification.db");
        String jobId = "job-output-gc";
        String taskId = "task-output-gc";
        TaskFlowObjectKeys.AttemptOutputIdentity first =
                new TaskFlowObjectKeys.AttemptOutputIdentity(
                        jobId,
                        taskId,
                        1,
                        ASSIGNMENT_ID
                );
        TaskFlowObjectKeys.AttemptOutputIdentity second =
                new TaskFlowObjectKeys.AttemptOutputIdentity(
                        jobId,
                        taskId,
                        2,
                        SECOND_ASSIGNMENT_ID
                );

        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob(jobId, "TEST_TASK", "requester-1", 1);
            db.insertTask(taskId, jobId);
            assertTrue(db.markTaskAssigned(
                    taskId,
                    "peer-1",
                    100L,
                    "lease-1",
                    500L,
                    1,
                    ASSIGNMENT_ID
            ));
            assertEquals(
                    OrphanOutputStateStore.AttemptOutputClassification.ACTIVE,
                    db.classifyAttemptOutput(first)
            );

            assertTrue(db.markTaskRetried(
                    taskId,
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "lease_expired",
                    150L
            ));
            assertTrue(db.markTaskAssigned(
                    taskId,
                    "peer-1",
                    200L,
                    "lease-2",
                    900L,
                    2,
                    SECOND_ASSIGNMENT_ID
            ));
            assertEquals(
                    OrphanOutputStateStore.AttemptOutputClassification.ORPHAN_CANDIDATE,
                    db.classifyAttemptOutput(first)
            );
            assertEquals(
                    OrphanOutputStateStore.AttemptOutputClassification.ACTIVE,
                    db.classifyAttemptOutput(second)
            );

            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            taskId,
                            2,
                            SECOND_ASSIGNMENT_ID,
                            "peer-1",
                            300L,
                            100L,
                            Map.of(
                                    "objectReference",
                                    outputReference(
                                            jobId,
                                            taskId,
                                            2,
                                            SECOND_ASSIGNMENT_ID,
                                            "2"
                                    )
                            )
                    )
            );
            assertEquals(
                    OrphanOutputStateStore.AttemptOutputClassification.AUTHORITATIVE,
                    db.classifyAttemptOutput(second)
            );
            assertEquals(
                    OrphanOutputStateStore.AttemptOutputClassification.ORPHAN_CANDIDATE,
                    db.classifyAttemptOutput(
                            new TaskFlowObjectKeys.AttemptOutputIdentity(
                                    "missing-job",
                                    "missing-task",
                                    1,
                                    OTHER_ASSIGNMENT_ID
                            )
                    )
            );
        }
    }

    @Test
    void deletionFailureRetryStateSurvivesRestartAndClearsIdempotently()
            throws Exception {
        Path dbPath = tempDir.resolve("taskflow-output-gc-retry.db");
        String key = TaskFlowObjectKeys.attemptOutputKey(
                "job-gc-retry",
                "task-gc-retry",
                1,
                ASSIGNMENT_ID
        );

        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertEquals(
                    OrphanOutputStateStore.MutationOutcome.COMMITTED,
                    db.recordOrphanOutputDeletionFailure(key, 100L, "first")
            );
            assertEquals(
                    OrphanOutputStateStore.MutationOutcome.COMMITTED,
                    db.recordOrphanOutputDeletionFailure(
                            key,
                            200L,
                            "x".repeat(2_000)
                    )
            );
            OrphanOutputStateStore.DeletionFailure failure =
                    db.loadOrphanOutputDeletionFailures(10).failures().getFirst();
            assertEquals(100L, failure.firstFailedAt());
            assertEquals(200L, failure.lastAttemptAt());
            assertEquals(2, failure.attemptCount());
            assertEquals(1_024, failure.lastError().length());
        }

        try (DatabaseManager reopened = new DatabaseManager(dbPath.toString())) {
            OrphanOutputStateStore.DeletionFailureBatch recovered =
                    reopened.loadOrphanOutputDeletionFailures(10);
            assertEquals(OrphanOutputStateStore.LoadOutcome.LOADED, recovered.outcome());
            assertEquals(List.of(key),
                    recovered.failures().stream()
                            .map(OrphanOutputStateStore.DeletionFailure::objectKey)
                            .toList());
            assertEquals(
                    OrphanOutputStateStore.MutationOutcome.COMMITTED,
                    reopened.clearOrphanOutputDeletionFailure(key)
            );
            assertEquals(
                    OrphanOutputStateStore.MutationOutcome.ALREADY_APPLIED,
                    reopened.clearOrphanOutputDeletionFailure(key)
            );
            assertEquals(
                    List.of(),
                    reopened.loadOrphanOutputDeletionFailures(10).failures()
            );
        }
    }

    @Test
    void closedDatabaseClassifiesGcOperationsAsStorageFailures() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-output-gc-storage-failure.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());
        db.close();
        String key = TaskFlowObjectKeys.attemptOutputKey(
                "job-gc-storage",
                "task-gc-storage",
                1,
                ASSIGNMENT_ID
        );

        assertEquals(
                OrphanOutputStateStore.AttemptOutputClassification.STORAGE_FAILURE,
                db.classifyAttemptOutput(
                        TaskFlowObjectKeys.parseAttemptOutputKey(key).orElseThrow()
                )
        );
        assertEquals(
                OrphanOutputStateStore.LoadOutcome.STORAGE_FAILURE,
                db.loadOrphanOutputDeletionFailures(10).outcome()
        );
        assertEquals(
                OrphanOutputStateStore.MutationOutcome.STORAGE_FAILURE,
                db.recordOrphanOutputDeletionFailure(key, 100L, "unavailable")
        );
        assertEquals(
                OrphanOutputStateStore.MutationOutcome.STORAGE_FAILURE,
                db.clearOrphanOutputDeletionFailure(key)
        );
    }

    @Test
    void migratesSchemaVersion12WithEmptyDurableGcRetryState() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-v12-output-gc-migration.db");
        try (DatabaseManager ignored = new DatabaseManager(dbPath.toString())) {
            // Create a complete current schema before reconstructing the v12 boundary.
        }
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE orphan_output_gc_failures");
            statement.execute("UPDATE schema_version SET version=12 WHERE id=1");
        }

        try (DatabaseManager migrated = new DatabaseManager(dbPath.toString())) {
            assertEquals(13, migrated.getSchemaVersion());
            assertTrue(tableExists(dbPath, "orphan_output_gc_failures"));
            assertEquals(
                    List.of(),
                    migrated.loadOrphanOutputDeletionFailures(10).failures()
            );
        }
    }

    @Test
    void resultCommitStorageFailureRollsBackTaskAndAttempt() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-result-commit-storage-failure.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-result-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-result-storage", "job-result-storage");
            assertTrue(db.markTaskAssigned(
                    "task-result-storage", "peer-1", 100L, "lease", 900L, 1, ASSIGNMENT_ID));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_task_result_commit
                        BEFORE UPDATE OF status ON tasks
                        WHEN NEW.status='COMPLETED'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected result commit failure');
                        END
                        """);
            }

            assertEquals(
                    JobStateStore.ResultCommitOutcome.STORAGE_FAILURE,
                    db.commitTaskResult(
                            "task-result-storage", 1, ASSIGNMENT_ID, "peer-1", 200L, 100L, "result")
            );
            DatabaseManager.TaskRecord task = db.getTasksForJob("job-result-storage").getFirst();
            assertEquals("ASSIGNED", task.status());
            assertEquals(ASSIGNMENT_ID, task.assignmentId());
            assertEquals(900L, task.leaseExpiresAt());
            JobStateStore.TaskAttemptRecord attempt = db.loadTaskAttempts("job-result-storage").getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempt.outcome());
            assertEquals(0L, attempt.finishedAt());
            assertNull(db.loadRunningJobsForResume().getFirst().tasks().getFirst().resultPayload());

            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("DROP TRIGGER fail_task_result_commit");
            }
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            "task-result-storage", 1, ASSIGNMENT_ID, "peer-1", 200L, 100L, "result")
            );
        }
    }

    @Test
    void lastResultAndFinalizingIntentRollbackTogetherOnIntentFault() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-finalizing-intent-fault.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-finalizing-intent", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-finalizing-intent", "job-finalizing-intent");
            assertTrue(db.markTaskAssigned(
                    "task-finalizing-intent",
                    "peer-1",
                    100L,
                    "lease",
                    900L,
                    1,
                    ASSIGNMENT_ID
            ));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_finalizing_intent
                        BEFORE UPDATE OF status ON jobs
                        WHEN OLD.status='RUNNING' AND NEW.status='FINALIZING'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected finalizing intent failure');
                        END
                        """);
            }

            assertEquals(
                    JobStateStore.ResultCommitOutcome.STORAGE_FAILURE,
                    db.commitTaskResult(
                            "task-finalizing-intent",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            "result"
                    )
            );
            assertEquals("RUNNING", db.getJobHistory().getFirst().status());
            assertEquals("ASSIGNED", db.getTasksForJob("job-finalizing-intent").getFirst().status());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.RUNNING,
                    db.loadTaskAttempts("job-finalizing-intent").getFirst().outcome()
            );

            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("DROP TRIGGER fail_finalizing_intent");
            }
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            "task-finalizing-intent",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            "result"
                    )
            );
            assertEquals("FINALIZING", db.getJobHistory().getFirst().status());
            assertEquals("COMPLETED", db.getTasksForJob("job-finalizing-intent").getFirst().status());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.SUCCEEDED,
                    db.loadTaskAttempts("job-finalizing-intent").getFirst().outcome()
            );
            assertEquals(1, db.loadRunningJobsForResume().size());
            assertTrue(db.loadPendingBrokerOutbox(10).isEmpty());
        }
    }

    @Test
    void resultCommitRejectsIncompleteExpectedTaskSetWithoutPartialCompletion() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-incomplete-task-set-result.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-incomplete-task-set", "TEST_TASK", "requester-1", 2);
            db.insertTask("task-incomplete-task-set", "job-incomplete-task-set");
            assertTrue(db.markTaskAssigned(
                    "task-incomplete-task-set",
                    "peer-1",
                    100L,
                    "lease",
                    900L,
                    1,
                    ASSIGNMENT_ID
            ));

            assertEquals(
                    JobStateStore.ResultCommitOutcome.STORAGE_FAILURE,
                    db.commitTaskResult(
                            "task-incomplete-task-set",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            "result"
                    )
            );
            assertEquals("RUNNING", db.getJobHistory().getFirst().status());
            assertEquals("ASSIGNED", db.getTasksForJob("job-incomplete-task-set").getFirst().status());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.RUNNING,
                    db.loadTaskAttempts("job-incomplete-task-set").getFirst().outcome()
            );
        }
    }

    @Test
    void assignedFailureCommitIsGenerationFencedAndReplayIsTyped() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-assigned-failure-fence.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-failure-fence", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-failure-fence", "job-failure-fence");
            assertTrue(db.markTaskAssigned(
                    "task-failure-fence", "peer-1", 100L, "lease", 900L, 1, ASSIGNMENT_ID));

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    db.commitAssignedTaskFailure(
                            "task-failure-fence",
                            1,
                            OTHER_ASSIGNMENT_ID,
                            "peer-1",
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "stale failure",
                            200L
                    )
            );
            assertEquals("ASSIGNED", db.getTasksForJob("job-failure-fence").getFirst().status());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.RUNNING,
                    db.loadTaskAttempts("job-failure-fence").getFirst().outcome()
            );

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitAssignedTaskFailure(
                            "task-failure-fence",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "processor failed",
                            200L
                    )
            );
            DatabaseManager.TaskRecord retried = db.getTasksForJob("job-failure-fence").getFirst();
            assertEquals("PENDING", retried.status());
            assertEquals(1, retried.retryCount());
            assertNull(retried.assignmentId());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    db.loadTaskAttempts("job-failure-fence").getFirst().outcome()
            );

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    db.commitAssignedTaskFailure(
                            "task-failure-fence",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "processor failed",
                            200L
                    )
            );
        }
    }

    @Test
    void dispatchFailureReplayCannotAliasANewerGenerationWithSameRetryCount() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-dispatch-failure-aba.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-dispatch-failure-aba", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-dispatch-failure-aba", "job-dispatch-failure-aba");
            assertTrue(db.markTaskAssigned(
                    "task-dispatch-failure-aba", "peer-1", 100L, "lease-1", 900L, 1, ASSIGNMENT_ID));
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitAssignedTaskFailure(
                            "task-dispatch-failure-aba",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            0,
                            JobStateStore.TaskAttemptOutcome.DISPATCH_FAILED,
                            "send failed",
                            110L
                    )
            );
            assertTrue(db.markTaskAssigned(
                    "task-dispatch-failure-aba",
                    "peer-1",
                    200L,
                    "lease-2",
                    1_000L,
                    2,
                    SECOND_ASSIGNMENT_ID
            ));
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitAssignedTaskFailure(
                            "task-dispatch-failure-aba",
                            2,
                            SECOND_ASSIGNMENT_ID,
                            "peer-1",
                            0,
                            JobStateStore.TaskAttemptOutcome.DISPATCH_FAILED,
                            "send failed again",
                            210L
                    )
            );

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    db.commitAssignedTaskFailure(
                            "task-dispatch-failure-aba",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            0,
                            JobStateStore.TaskAttemptOutcome.DISPATCH_FAILED,
                            "send failed",
                            110L
                    )
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    db.commitAssignedTaskFailure(
                            "task-dispatch-failure-aba",
                            2,
                            SECOND_ASSIGNMENT_ID,
                            "peer-1",
                            0,
                            JobStateStore.TaskAttemptOutcome.DISPATCH_FAILED,
                            "send failed again",
                            210L
                    )
            );
            assertEquals(2, db.getTasksForJob("job-dispatch-failure-aba").getFirst().attemptNumber());
            assertEquals(2, db.loadTaskAttempts("job-dispatch-failure-aba").size());
        }
    }

    @Test
    void assignedFailureStorageFaultRollsBackTaskAndAttempt() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-assigned-failure-storage.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-failure-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-failure-storage", "job-failure-storage");
            assertTrue(db.markTaskAssigned(
                    "task-failure-storage", "peer-1", 100L, "lease", 900L, 1, ASSIGNMENT_ID));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_assigned_failure_commit
                        BEFORE UPDATE OF status ON tasks
                        WHEN NEW.status='PENDING'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected assigned failure commit');
                        END
                        """);
            }

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                    db.commitAssignedTaskFailure(
                            "task-failure-storage",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "processor failed",
                            200L
                    )
            );
            DatabaseManager.TaskRecord assigned = db.getTasksForJob("job-failure-storage").getFirst();
            assertEquals("ASSIGNED", assigned.status());
            assertEquals(0, assigned.retryCount());
            assertEquals(ASSIGNMENT_ID, assigned.assignmentId());
            JobStateStore.TaskAttemptRecord attempt = db.loadTaskAttempts("job-failure-storage").getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempt.outcome());
            assertEquals(0L, attempt.finishedAt());
        }
    }

    @Test
    void assignedTerminalFailureStorageFaultRollsBackTaskAndAttempt() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-assigned-terminal-storage.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-terminal-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-terminal-storage", "job-terminal-storage");
            assertTrue(db.markTaskAssigned(
                    "task-terminal-storage", "peer-1", 100L, "lease", 900L, 1, ASSIGNMENT_ID));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_assigned_terminal_commit
                        BEFORE UPDATE OF status ON tasks
                        WHEN NEW.status='FAILED'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected assigned terminal commit');
                        END
                        """);
            }

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                    db.commitAssignedTaskFailure(
                            "task-terminal-storage",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            1,
                            JobStateStore.TaskAttemptOutcome.TERMINAL_FAILURE,
                            "retry exhausted",
                            200L
                    )
            );
            DatabaseManager.TaskRecord assigned = db.getTasksForJob("job-terminal-storage").getFirst();
            assertEquals("ASSIGNED", assigned.status());
            assertEquals(0, assigned.retryCount());
            assertEquals(ASSIGNMENT_ID, assigned.assignmentId());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.RUNNING,
                    db.loadTaskAttempts("job-terminal-storage").getFirst().outcome()
            );
        }
    }

    @Test
    void assignmentCommitBeforePublishLeavesOneDurableIdentityAndPendingOutbox() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-outbox-test.db");
        BrokerOutboxStore.CommittedTaskAssignment committed;
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-outbox", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-outbox-0", "job-outbox");

            committed = db.createTaskAssignmentAndEnqueueBrokerOutbox(
                    "task-outbox-0",
                    "peer-1",
                    123L,
                    "coordinator-lease",
                    456L,
                    ASSIGNMENT_ID,
                    taskAssignmentOutboxTemplate("peer-1", "task-outbox-0", "job-outbox")
            ).orElseThrow();

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-outbox");
            assertEquals("ASSIGNED", tasks.getFirst().status());
            assertEquals("peer-1", tasks.getFirst().assignedPeerId());
            assertEquals("coordinator-lease", tasks.getFirst().leaseOwnerId());
            assertEquals(456L, tasks.getFirst().leaseExpiresAt());
            assertEquals(1, tasks.getFirst().attemptNumber());
            assertEquals(ASSIGNMENT_ID, committed.identity().assignmentId());
            assertEquals(committed.identity().assignmentId(), tasks.getFirst().assignmentId());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-outbox");
            assertEquals(1, attempts.size());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempts.getFirst().outcome());
            assertEquals(committed.identity().assignmentId(), attempts.getFirst().assignmentId());
            assertEquals(456L, attempts.getFirst().leaseExpiresAt());

            List<BrokerOutboxStore.OutboxRecord> pending = db.loadPendingBrokerOutbox(10);
            assertEquals(1, pending.size());
            assertEquals(
                    BrokerOutboxStore.PendingOutboxCount.counted(1L),
                    db.countPendingBrokerOutbox()
            );
            assertEquals(
                    BrokerOutboxStore.PendingOutboxMetrics.observed(1L, 123L),
                    db.observePendingBrokerOutbox()
            );
            assertEquals(committed.outboxRecord().outboxId(), pending.getFirst().outboxId());
            assertEquals(123L, pending.getFirst().createdAt());
            assertEquals(TransportRoute.TASK_ASSIGN, pending.getFirst().message().route());
            assertEquals("peer-1", pending.getFirst().message().peerNodeId());
            TaskAssignMessage message = assertInstanceOf(
                    TaskAssignMessage.class,
                    pending.getFirst().message().message()
            );
            assertEquals("task-outbox-0", message.getTaskId());
            assertEquals(committed.identity().attemptNumber(), message.getAttemptNumber());
            assertEquals(committed.identity().assignmentId(), message.getAssignmentId());
            assertEquals(committed.identity().leaseExpiresAtEpochMillis(),
                    message.getLeaseExpiresAtEpochMillis());
        }

        try (DatabaseManager reopened = new DatabaseManager(dbPath.toString())) {
            DatabaseManager.TaskRecord task = reopened.getTasksForJob("job-outbox").getFirst();
            assertEquals(committed.identity().attemptNumber(), task.attemptNumber());
            assertEquals(committed.identity().assignmentId(), task.assignmentId());
            List<BrokerOutboxStore.OutboxRecord> pending = reopened.loadPendingBrokerOutbox(10);
            assertEquals(1, pending.size());
            assertEquals(committed.outboxRecord().outboxId(), pending.getFirst().outboxId());
            TaskAssignMessage replay = assertInstanceOf(
                    TaskAssignMessage.class,
                    pending.getFirst().message().message()
            );
            assertEquals(committed.identity().attemptNumber(), replay.getAttemptNumber());
            assertEquals(committed.identity().assignmentId(), replay.getAssignmentId());
            assertEquals(committed.identity().leaseExpiresAtEpochMillis(), replay.getLeaseExpiresAtEpochMillis());

            assertTrue(reopened.markBrokerOutboxPublished(committed.outboxRecord().outboxId(), 789L));
            assertEquals(List.of(), reopened.loadPendingBrokerOutbox(10));
            assertEquals(
                    BrokerOutboxStore.PendingOutboxCount.counted(0L),
                    reopened.countPendingBrokerOutbox()
            );
            assertEquals(
                    BrokerOutboxStore.PendingOutboxMetrics.observed(0L, 0L),
                    reopened.observePendingBrokerOutbox()
            );
        }
    }

    @Test
    void pendingOutboxAggregateDistinguishesStorageFailure() throws Exception {
        DatabaseManager db = new DatabaseManager(
                tempDir.resolve("taskflow-outbox-count-failure.db").toString()
        );
        db.close();

        BrokerOutboxStore.PendingOutboxCount count = db.countPendingBrokerOutbox();

        assertEquals(
                BrokerOutboxStore.PendingOutboxCountOutcome.STORAGE_FAILURE,
                count.outcome()
        );
        assertEquals(0L, count.count());
        assertEquals(
                BrokerOutboxStore.PendingOutboxMetrics.storageFailure(),
                db.observePendingBrokerOutbox()
        );
    }

    @Test
    void repeatedTypedAssignmentCommitReturnsExactDurableProjectionAndOutbox() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-outbox-typed-replay.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-outbox-typed-replay", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-outbox-typed-replay-0", "job-outbox-typed-replay");
            BrokerOutboxStore.OutboxMessage template = taskAssignmentOutboxTemplate(
                    "peer-1",
                    "task-outbox-typed-replay-0",
                    "job-outbox-typed-replay"
            );

            BrokerOutboxStore.TaskAssignmentCommit committed =
                    db.commitTaskAssignmentAndEnqueueBrokerOutbox(
                            "task-outbox-typed-replay-0",
                            "peer-1",
                            123L,
                            "coordinator-lease",
                            456L,
                            ASSIGNMENT_ID,
                            template
                    );
            BrokerOutboxStore.TaskAssignmentCommit replayed =
                    db.commitTaskAssignmentAndEnqueueBrokerOutbox(
                            "task-outbox-typed-replay-0",
                            "peer-1",
                            123L,
                            "coordinator-lease",
                            456L,
                            ASSIGNMENT_ID,
                            template
                    );

            assertEquals(JobStateStore.DurableTransitionOutcome.COMMITTED, committed.outcome());
            assertEquals(JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED, replayed.outcome());
            assertEquals(committed.assignment().identity(), replayed.assignment().identity());
            assertEquals(
                    committed.assignment().outboxRecord().outboxId(),
                    replayed.assignment().outboxRecord().outboxId()
            );
            assertEquals(1, db.loadTaskAttempts("job-outbox-typed-replay").size());
            assertEquals(1, db.loadPendingBrokerOutbox(10).size());
        }
    }

    @Test
    void taskAssignmentOutboxRollsBackWhenTaskCannotBeAssigned() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-outbox-rollback-test.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-outbox-rollback", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-outbox-rollback-0", "job-outbox-rollback");
            assertTrue(db.markTaskAssigned("task-outbox-rollback-0", "peer-1", 100L));

            var outbox = db.createTaskAssignmentAndEnqueueBrokerOutbox(
                    "task-outbox-rollback-0",
                    "peer-2",
                    200L,
                    "coordinator-lease",
                    300L,
                    taskAssignmentOutboxTemplate("peer-2", "task-outbox-rollback-0", "job-outbox-rollback")
            );

            assertTrue(outbox.isEmpty());
            assertEquals(List.of(), db.loadPendingBrokerOutbox(10));
            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-outbox-rollback");
            assertEquals("ASSIGNED", tasks.getFirst().status());
            assertEquals("peer-1", tasks.getFirst().assignedPeerId());
            assertEquals(1, db.loadTaskAttempts("job-outbox-rollback").size());
        }
    }

    @Test
    void outboxInsertFailureRollsBackTaskAndAttemptTogether() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-outbox-insert-failure.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-outbox-failure", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-outbox-failure-0", "job-outbox-failure");
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_task_assignment_outbox
                        BEFORE INSERT ON broker_outbox
                        BEGIN
                            SELECT RAISE(ABORT, 'injected outbox failure');
                        END
                        """);
            }

            assertTrue(db.createTaskAssignmentAndEnqueueBrokerOutbox(
                    "task-outbox-failure-0",
                    "peer-1",
                    100L,
                    "coordinator-lease",
                    500L,
                    taskAssignmentOutboxTemplate(
                            "peer-1",
                            "task-outbox-failure-0",
                            "job-outbox-failure"
                    )
            ).isEmpty());

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-outbox-failure").getFirst();
            assertEquals("PENDING", task.status());
            assertEquals(0, task.attemptNumber());
            assertNull(task.assignmentId());
            assertEquals(List.of(), db.loadTaskAttempts("job-outbox-failure"));
            assertEquals(List.of(), db.loadPendingBrokerOutbox(10));
        }
    }

    @Test
    void repeatedAssignmentCallsCreateOnlyOneGenerationAndOutbox() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-outbox-repeat.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-outbox-repeat", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-outbox-repeat-0", "job-outbox-repeat");

            Optional<BrokerOutboxStore.CommittedTaskAssignment> first =
                    db.createTaskAssignmentAndEnqueueBrokerOutbox(
                            "task-outbox-repeat-0",
                            "peer-1",
                            100L,
                            "coordinator-lease",
                            500L,
                            taskAssignmentOutboxTemplate(
                                    "peer-1",
                                    "task-outbox-repeat-0",
                                    "job-outbox-repeat"
                            )
                    );
            Optional<BrokerOutboxStore.CommittedTaskAssignment> repeated =
                    db.createTaskAssignmentAndEnqueueBrokerOutbox(
                            "task-outbox-repeat-0",
                            "peer-1",
                            101L,
                            "coordinator-lease",
                            501L,
                            taskAssignmentOutboxTemplate(
                                    "peer-1",
                                    "task-outbox-repeat-0",
                                    "job-outbox-repeat"
                            )
                    );

            assertTrue(first.isPresent());
            assertTrue(repeated.isEmpty());
            assertEquals(1, db.getTasksForJob("job-outbox-repeat").getFirst().attemptNumber());
            assertEquals(1, db.loadTaskAttempts("job-outbox-repeat").size());
            assertEquals(1, db.loadPendingBrokerOutbox(10).size());
        }
    }

    @Test
    void assignmentTransactionAdvancesPersistedGenerationAfterRetry() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-outbox-next-generation.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-outbox-next", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-outbox-next-0", "job-outbox-next");

            BrokerOutboxStore.CommittedTaskAssignment first =
                    db.createTaskAssignmentAndEnqueueBrokerOutbox(
                            "task-outbox-next-0",
                            "peer-1",
                            100L,
                            "coordinator-lease",
                            500L,
                            taskAssignmentOutboxTemplate(
                                    "peer-1",
                                    "task-outbox-next-0",
                                    "job-outbox-next"
                            )
                    ).orElseThrow();
            assertTrue(db.markTaskRetried(
                    "task-outbox-next-0",
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "retry",
                    200L
            ));
            BrokerOutboxStore.CommittedTaskAssignment second =
                    db.createTaskAssignmentAndEnqueueBrokerOutbox(
                            "task-outbox-next-0",
                            "peer-1",
                            300L,
                            "coordinator-lease",
                            700L,
                            taskAssignmentOutboxTemplate(
                                    "peer-1",
                                    "task-outbox-next-0",
                                    "job-outbox-next"
                            )
                    ).orElseThrow();

            assertEquals(1, first.identity().attemptNumber());
            assertEquals(2, second.identity().attemptNumber());
            assertNotEquals(first.identity().assignmentId(), second.identity().assignmentId());
            assertEquals(2, db.getTasksForJob("job-outbox-next").getFirst().attemptNumber());
            assertEquals(2, db.loadTaskAttempts("job-outbox-next").size());
            assertEquals(2, db.loadPendingBrokerOutbox(10).size());
        }
    }

    @Test
    void concurrentAssignmentCallsCreateOnlyOneGenerationAndOutbox() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-outbox-concurrent.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-outbox-concurrent", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-outbox-concurrent-0", "job-outbox-concurrent");
            ExecutorService executor = Executors.newFixedThreadPool(4);
            CountDownLatch start = new CountDownLatch(1);
            try {
                List<Future<Optional<BrokerOutboxStore.CommittedTaskAssignment>>> futures =
                        java.util.stream.IntStream.range(0, 4)
                                .mapToObj(index -> executor.submit(() -> {
                                    start.await();
                                    return db.createTaskAssignmentAndEnqueueBrokerOutbox(
                                            "task-outbox-concurrent-0",
                                            "peer-1",
                                            100L + index,
                                            "coordinator-lease",
                                            500L + index,
                                            taskAssignmentOutboxTemplate(
                                                    "peer-1",
                                                    "task-outbox-concurrent-0",
                                                    "job-outbox-concurrent"
                                            )
                                    );
                                }))
                                .toList();
                start.countDown();

                long committedCount = 0L;
                for (Future<Optional<BrokerOutboxStore.CommittedTaskAssignment>> future : futures) {
                    if (future.get(2, TimeUnit.SECONDS).isPresent()) {
                        committedCount++;
                    }
                }
                assertEquals(1L, committedCount);
            } finally {
                executor.shutdownNow();
            }

            assertEquals(1, db.getTasksForJob("job-outbox-concurrent").getFirst().attemptNumber());
            assertEquals(1, db.loadTaskAttempts("job-outbox-concurrent").size());
            assertEquals(1, db.loadPendingBrokerOutbox(10).size());
        }
    }

    @Test
    void jobCompletionStorageFaultPreservesRunningStateAndReplayIsTyped() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-job-completion-storage.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-completion-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-completion-storage", "job-completion-storage");
            assertTrue(db.markTaskAssigned("task-completion-storage", "peer-1", 100L));
            assertTrue(db.markTaskCompleted("task-completion-storage", 200L, 100L, "result"));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_job_completion_commit
                        BEFORE UPDATE OF status ON jobs
                        WHEN NEW.status='COMPLETED'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected job completion commit');
                        END
                        """);
            }

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                    db.commitJobCompleted("job-completion-storage", Map.of("result", "value"), 300L)
            );
            assertEquals("FINALIZING", db.getJobHistory().getFirst().status());

            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("DROP TRIGGER fail_job_completion_commit");
            }
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitJobCompleted("job-completion-storage", Map.of("result", "value"), 300L)
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    db.commitJobCompleted("job-completion-storage", Map.of("result", "value"), 300L)
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    db.commitJobCompleted("job-completion-storage", Map.of("result", "different"), 300L)
            );
        }
    }

    @Test
    void jobFailureStorageFaultRollsBackTaskJobAndAttemptTogether() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-job-failure-storage.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-failure-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-job-failure-storage", "job-failure-storage");
            assertTrue(db.markTaskAssigned(
                    "task-job-failure-storage", "peer-1", 100L, "lease", 900L, 1, ASSIGNMENT_ID));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_job_failure_commit
                        BEFORE UPDATE OF status ON jobs
                        WHEN NEW.status='FAILED'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected job failure commit');
                        END
                        """);
            }

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                    db.commitJobFailed(
                            "job-failure-storage",
                            List.of(new JobStateStore.TaskFailureUpdate(
                                    "task-job-failure-storage",
                                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                                    "job failed",
                                    200L
                            )),
                            200L
                    )
            );
            assertEquals("RUNNING", db.getJobHistory().getFirst().status());
            DatabaseManager.TaskRecord task = db.getTasksForJob("job-failure-storage").getFirst();
            assertEquals("ASSIGNED", task.status());
            assertEquals(ASSIGNMENT_ID, task.assignmentId());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.RUNNING,
                    db.loadTaskAttempts("job-failure-storage").getFirst().outcome()
            );

            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("DROP TRIGGER fail_job_failure_commit");
            }
            List<JobStateStore.TaskFailureUpdate> failures = List.of(
                    new JobStateStore.TaskFailureUpdate(
                            "task-job-failure-storage",
                            JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                            "job failed",
                            200L
                    )
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitJobFailed("job-failure-storage", failures, 200L)
            );
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    db.commitJobFailed("job-failure-storage", failures, 200L)
            );
        }
    }

    @Test
    void jobFailureReplayRequiresEveryRequestedTaskFailureToBeDurable() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-job-failure-replay-classification.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-failure-replay", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-job-failure-replay", "job-failure-replay");
            assertTrue(db.markJobFailed("job-failure-replay"));

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.STALE_STATE,
                    db.commitJobFailed(
                            "job-failure-replay",
                            List.of(new JobStateStore.TaskFailureUpdate(
                                    "task-job-failure-replay",
                                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                                    "job failed",
                                    200L
                            )),
                            200L
                    )
            );
            assertEquals("PENDING", db.getTasksForJob("job-failure-replay").getFirst().status());
        }
    }

    @Test
    void finalResultOutboxInsertFaultRollsBackTerminalJobState() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-final-outbox-storage.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-final-outbox-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-final-outbox-storage", "job-final-outbox-storage");
            assertTrue(db.markTaskAssigned("task-final-outbox-storage", "peer-1", 100L));
            assertTrue(db.markTaskCompleted("task-final-outbox-storage", 200L, 100L, "result"));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_final_outbox_insert
                        BEFORE INSERT ON broker_outbox
                        BEGIN
                            SELECT RAISE(ABORT, 'injected final outbox insert');
                        END
                        """);
            }
            JobResultMessage result = new JobResultMessage(
                    "COORDINATOR",
                    "2026-07-22T00:00:00Z",
                    "job-final-outbox-storage",
                    "TEST_TASK",
                    true,
                    Map.of("result", "value"),
                    List.of("result")
            );

            BrokerOutboxStore.OutboxCommit commit = db.commitJobCompletedAndEnqueueBrokerOutbox(
                    "job-final-outbox-storage",
                    result.getResultPayload(),
                    jobResultOutboxMessage("requester-1", result)
            );
            assertEquals(JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE, commit.outcome());
            assertNull(commit.outboxRecord());
            assertEquals("FINALIZING", db.getJobHistory().getFirst().status());
            assertTrue(db.loadPendingBrokerOutbox(10).isEmpty());
        }
    }

    @Test
    void failedFinalResultOutboxFaultRollsBackTasksJobAndAttemptBeforeRetry() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-failed-final-outbox-storage.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-failed-final-outbox-storage", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-failed-final-outbox-storage", "job-failed-final-outbox-storage");
            assertTrue(db.markTaskAssigned(
                    "task-failed-final-outbox-storage",
                    "peer-1",
                    100L,
                    "coordinator-lease",
                    900L,
                    1,
                    ASSIGNMENT_ID
            ));
            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("""
                        CREATE TRIGGER fail_failed_final_outbox_insert
                        BEFORE INSERT ON broker_outbox
                        BEGIN
                            SELECT RAISE(ABORT, 'injected failed final outbox insert');
                        END
                        """);
            }
            JobResultMessage result = new JobResultMessage(
                    "COORDINATOR",
                    "2026-07-22T00:00:00Z",
                    "job-failed-final-outbox-storage",
                    "TEST_TASK",
                    false,
                    null,
                    List.of(),
                    "job failed"
            );
            List<BrokerOutboxStore.TaskFailureUpdate> failures = List.of(
                    new BrokerOutboxStore.TaskFailureUpdate(
                            "task-failed-final-outbox-storage",
                            JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                            "job failed",
                            200L
                    )
            );

            BrokerOutboxStore.OutboxCommit failed = db.commitJobFailedAndEnqueueBrokerOutbox(
                    "job-failed-final-outbox-storage",
                    failures,
                    jobResultOutboxMessage("requester-1", result)
            );
            assertEquals(JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE, failed.outcome());
            assertNull(failed.outboxRecord());
            assertEquals("RUNNING", db.getJobHistory().getFirst().status());
            assertEquals(
                    "ASSIGNED",
                    db.getTasksForJob("job-failed-final-outbox-storage").getFirst().status()
            );
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.RUNNING,
                    db.loadTaskAttempts("job-failed-final-outbox-storage").getFirst().outcome()
            );
            assertTrue(db.loadPendingBrokerOutbox(10).isEmpty());

            try (Connection triggerConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement statement = triggerConnection.createStatement()) {
                statement.execute("DROP TRIGGER fail_failed_final_outbox_insert");
            }
            BrokerOutboxStore.OutboxCommit committed = db.commitJobFailedAndEnqueueBrokerOutbox(
                    "job-failed-final-outbox-storage",
                    failures,
                    jobResultOutboxMessage("requester-1", result)
            );
            BrokerOutboxStore.OutboxCommit replayed = db.commitJobFailedAndEnqueueBrokerOutbox(
                    "job-failed-final-outbox-storage",
                    failures,
                    jobResultOutboxMessage("requester-1", result)
            );
            assertEquals(JobStateStore.DurableTransitionOutcome.COMMITTED, committed.outcome());
            assertNotNull(committed.outboxRecord());
            assertEquals(JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED, replayed.outcome());
            assertEquals(committed.outboxRecord().outboxId(), replayed.outboxRecord().outboxId());
            assertEquals("FAILED", db.getJobHistory().getFirst().status());
            assertEquals(
                    "FAILED",
                    db.getTasksForJob("job-failed-final-outbox-storage").getFirst().status()
            );
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                    db.loadTaskAttempts("job-failed-final-outbox-storage").getFirst().outcome()
            );
            assertEquals(1, db.loadPendingBrokerOutbox(10).size());
        }
    }

    @Test
    void repeatedTypedFinalOutboxCommitsReturnExactDurableRecord() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-final-outbox-typed-replay.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-final-outbox-typed-replay", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-final-outbox-typed-replay", "job-final-outbox-typed-replay");
            assertTrue(db.markTaskAssigned("task-final-outbox-typed-replay", "peer-1", 100L));
            assertTrue(db.markTaskCompleted(
                    "task-final-outbox-typed-replay",
                    200L,
                    100L,
                    "result"
            ));
            JobResultMessage result = new JobResultMessage(
                    "COORDINATOR",
                    "2026-07-22T00:00:00Z",
                    "job-final-outbox-typed-replay",
                    "TEST_TASK",
                    true,
                    Map.of("result", "value"),
                    List.of("result")
            );
            BrokerOutboxStore.OutboxMessage message = jobResultOutboxMessage("requester-1", result);

            BrokerOutboxStore.OutboxCommit committed = db.commitJobCompletedAndEnqueueBrokerOutbox(
                    "job-final-outbox-typed-replay",
                    result.getResultPayload(),
                    message
            );
            BrokerOutboxStore.OutboxCommit replayed = db.commitJobCompletedAndEnqueueBrokerOutbox(
                    "job-final-outbox-typed-replay",
                    result.getResultPayload(),
                    message
            );
            BrokerOutboxStore.OutboxCommit conflicting = db.commitJobCompletedAndEnqueueBrokerOutbox(
                    "job-final-outbox-typed-replay",
                    Map.of("result", "different"),
                    message
            );

            assertEquals(JobStateStore.DurableTransitionOutcome.COMMITTED, committed.outcome());
            assertEquals(JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED, replayed.outcome());
            assertEquals(committed.outboxRecord().outboxId(), replayed.outboxRecord().outboxId());
            assertEquals(JobStateStore.DurableTransitionOutcome.STALE_STATE, conflicting.outcome());
            assertNull(conflicting.outboxRecord());
            assertEquals(1, db.loadPendingBrokerOutbox(10).size());
        }
    }

    @Test
    void concurrentFinalizationCreatesOneTerminalStateAndOneOutbox() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-final-outbox-concurrent.db");
        try (DatabaseManager first = new DatabaseManager(dbPath.toString())) {
            first.insertJob("job-final-outbox-concurrent", "TEST_TASK", "requester-1", 1);
            first.insertTask("task-final-outbox-concurrent", "job-final-outbox-concurrent");
            assertTrue(first.markTaskAssigned(
                    "task-final-outbox-concurrent",
                    "peer-1",
                    100L,
                    "lease",
                    900L,
                    1,
                    ASSIGNMENT_ID
            ));
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    first.commitTaskResult(
                            "task-final-outbox-concurrent",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            "result"
                    )
            );

            JobResultMessage result = new JobResultMessage(
                    "COORDINATOR",
                    "2026-07-22T00:00:00Z",
                    "job-final-outbox-concurrent",
                    "TEST_TASK",
                    true,
                    Map.of("result", "value"),
                    List.of("result")
            );
            BrokerOutboxStore.OutboxMessage message = jobResultOutboxMessage("requester-1", result);
            try (DatabaseManager second = new DatabaseManager(dbPath.toString())) {
                ExecutorService executor = Executors.newFixedThreadPool(2);
                CountDownLatch start = new CountDownLatch(1);
                try {
                    List<Future<BrokerOutboxStore.OutboxCommit>> futures = List.of(
                            executor.submit(() -> {
                                start.await();
                                return first.commitJobCompletedAndEnqueueBrokerOutbox(
                                        "job-final-outbox-concurrent",
                                        result.getResultPayload(),
                                        message
                                );
                            }),
                            executor.submit(() -> {
                                start.await();
                                return second.commitJobCompletedAndEnqueueBrokerOutbox(
                                        "job-final-outbox-concurrent",
                                        result.getResultPayload(),
                                        message
                                );
                            })
                    );
                    start.countDown();
                    List<BrokerOutboxStore.OutboxCommit> concurrent = futures.stream()
                            .map(future -> {
                                try {
                                    return future.get(5, TimeUnit.SECONDS);
                                } catch (Exception e) {
                                    throw new IllegalStateException(e);
                                }
                            })
                            .toList();
                    assertEquals(
                            1L,
                            concurrent.stream()
                                    .filter(commit -> commit.outcome()
                                            == JobStateStore.DurableTransitionOutcome.COMMITTED)
                                    .count()
                    );
                    assertEquals(
                            1L,
                            concurrent.stream()
                                    .filter(commit -> commit.outcome()
                                            == JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED)
                                    .count()
                    );
                } finally {
                    executor.shutdownNow();
                }

                BrokerOutboxStore.OutboxCommit firstReplay =
                        first.commitJobCompletedAndEnqueueBrokerOutbox(
                                "job-final-outbox-concurrent",
                                result.getResultPayload(),
                                message
                        );
                BrokerOutboxStore.OutboxCommit secondReplay =
                        second.commitJobCompletedAndEnqueueBrokerOutbox(
                                "job-final-outbox-concurrent",
                                result.getResultPayload(),
                                message
                        );
                assertEquals(JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED, firstReplay.outcome());
                assertEquals(JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED, secondReplay.outcome());
                assertEquals(firstReplay.outboxRecord().outboxId(), secondReplay.outboxRecord().outboxId());
                assertEquals("COMPLETED", first.getJobHistory().getFirst().status());
                assertEquals(1, first.loadPendingBrokerOutbox(10).size());
            }
        }
    }

    @Test
    void completedJobOutboxCommitsTerminalStateAndResultMessage() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-completed-outbox-test.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-completed-outbox", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-completed-outbox-0", "job-completed-outbox");
            assertTrue(db.markTaskAssigned("task-completed-outbox-0", "peer-1", 123L));
            assertTrue(db.markTaskCompleted("task-completed-outbox-0", 456L, 333L, "result"));

            JobResultMessage result = new JobResultMessage(
                    "COORDINATOR",
                    "2026-07-02T00:00:00Z",
                    "job-completed-outbox",
                    "TEST_TASK",
                    true,
                    Map.of("joined", "result"),
                    List.of("result")
            );
            var outbox = db.markJobCompletedAndEnqueueBrokerOutbox(
                    "job-completed-outbox",
                    result.getResultPayload(),
                    jobResultOutboxMessage("requester-1", result)
            );

            assertTrue(outbox.isPresent());
            DatabaseManager.JobRecord job = db.getJobHistory().getFirst();
            assertEquals("COMPLETED", job.status());
            JobStateStore.CompletedJobResultState completed =
                    db.loadCompletedJobResult("job-completed-outbox").orElseThrow();
            assertEquals(Map.of("joined", "result"), completed.resultPayload());

            List<BrokerOutboxStore.OutboxRecord> pending = db.loadPendingBrokerOutbox(10);
            assertEquals(1, pending.size());
            assertEquals(TransportRoute.JOB_RESULT, pending.getFirst().message().route());
            assertEquals("requester-1", pending.getFirst().message().peerNodeId());
            JobResultMessage restored = assertInstanceOf(
                    JobResultMessage.class,
                    pending.getFirst().message().message()
            );
            assertEquals("job-completed-outbox", restored.getJobId());
            assertEquals(Map.of("joined", "result"), restored.getResultPayload());
        }
    }

    @Test
    void failedJobOutboxCommitsTaskFailuresAndResultMessage() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-failed-outbox-test.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-failed-outbox", "TEST_TASK", "requester-1", 2);
            db.insertTask("task-failed-outbox-0", "job-failed-outbox");
            db.insertTask("task-failed-outbox-1", "job-failed-outbox");
            assertTrue(db.markTaskAssigned("task-failed-outbox-0", "peer-1", 123L));

            JobResultMessage result = new JobResultMessage(
                    "COORDINATOR",
                    "2026-07-02T00:00:00Z",
                    "job-failed-outbox",
                    "TEST_TASK",
                    false,
                    List.of(),
                    "job failed"
            );
            var outbox = db.markJobFailedAndEnqueueBrokerOutbox(
                    "job-failed-outbox",
                    List.of(
                            new BrokerOutboxStore.TaskFailureUpdate(
                                    "task-failed-outbox-0",
                                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                                    "job failed",
                                    456L
                            ),
                            new BrokerOutboxStore.TaskFailureUpdate(
                                    "task-failed-outbox-1",
                                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                                    "job failed",
                                    456L
                            )
                    ),
                    jobResultOutboxMessage("requester-1", result)
            );

            assertTrue(outbox.isPresent());
            assertEquals("FAILED", db.getJobHistory().getFirst().status());
            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-failed-outbox");
            assertEquals(List.of("FAILED", "FAILED"), tasks.stream().map(DatabaseManager.TaskRecord::status).toList());
            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-failed-outbox");
            assertEquals(1, attempts.size());
            assertEquals(JobStateStore.TaskAttemptOutcome.JOB_FAILED, attempts.getFirst().outcome());
            assertEquals("job failed", attempts.getFirst().failureReason());

            List<BrokerOutboxStore.OutboxRecord> pending = db.loadPendingBrokerOutbox(10);
            assertEquals(1, pending.size());
            JobResultMessage restored = assertInstanceOf(
                    JobResultMessage.class,
                    pending.getFirst().message().message()
            );
            assertFalse(restored.isSuccessful());
            assertEquals("job failed", restored.getErrorMessage());
        }
    }

    @Test
    void mapsHistoricalTcpPeerRegistryTransportToUnknownAcrossRestart() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-restart-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "TCP_PEER",
                    PeerTransport.UNKNOWN,
                    Set.of("image_conversion", "text_analysis"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    new PeerMetricsSnapshot(2L, 1L, 30L, 250L)
            )));
        } finally {
            db.close();
        }

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement statement = connection.createStatement()) {
            assertEquals(1, statement.executeUpdate(
                    "UPDATE peer_registry SET transport='TCP' WHERE peer_id='peer-1'"
            ));
        }

        DatabaseManager reopened = new DatabaseManager(dbPath.toString());
        try {
            List<PeerRegistryRecord> peers = reopened.loadPeerRecords();
            assertEquals(1, peers.size());
            PeerRegistryRecord peer = peers.getFirst();
            assertEquals("peer-1", peer.peerId());
            assertEquals("TCP_PEER", peer.runtimeType());
            assertEquals(PeerTransport.UNKNOWN, peer.transport());
            assertEquals(Set.of("IMAGE_CONVERSION", "TEXT_ANALYSIS"), peer.supportedTaskTypes());
            assertEquals(100L, peer.firstSeenAtMillis());
            assertEquals(150L, peer.lastHeartbeatAtMillis());
            assertEquals(0L, peer.lastDisconnectedAtMillis());
            assertEquals(PeerStatus.CONNECTED, peer.status());
            assertEquals(new PeerMetricsSnapshot(2L, 1L, 30L, 250L), peer.metricsSnapshot());
        } finally {
            reopened.close();
        }
    }

    @Test
    void peerRegistryUpsertPreservesFirstSeenAndUpdatesHeartbeatCapabilities() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-heartbeat-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("image_conversion"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("text_analysis"),
                    999L,
                    300L,
                    0L,
                    PeerStatus.CONNECTED,
                    new PeerMetricsSnapshot(4L, 0L, 45L, 500L)
            )));

            PeerRegistryRecord peer = db.loadPeerRecords().getFirst();
            assertEquals(100L, peer.firstSeenAtMillis());
            assertEquals(300L, peer.lastHeartbeatAtMillis());
            assertEquals(Set.of("TEXT_ANALYSIS"), peer.supportedTaskTypes());
            assertEquals(PeerStatus.CONNECTED, peer.status());
            assertEquals(new PeerMetricsSnapshot(4L, 0L, 45L, 500L), peer.metricsSnapshot());
        } finally {
            db.close();
        }
    }

    @Test
    void peerRegistryUpsertMarksPeerDisconnected() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-disconnect-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("image_conversion"),
                    100L,
                    200L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("image_conversion"),
                    100L,
                    200L,
                    450L,
                    PeerStatus.DISCONNECTED,
                    new PeerMetricsSnapshot(1L, 1L, 25L, 120L)
            )));

            PeerRegistryRecord peer = db.loadPeerRecords().getFirst();
            assertEquals(PeerStatus.DISCONNECTED, peer.status());
            assertEquals(450L, peer.lastDisconnectedAtMillis());
            assertEquals(200L, peer.lastHeartbeatAtMillis());
            assertEquals(new PeerMetricsSnapshot(1L, 1L, 25L, 120L), peer.metricsSnapshot());
        } finally {
            db.close();
        }
    }

    @Test
    void peerRegistryDuplicatePeerIdUpdatesSingleDurableRecord() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-duplicate-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "PEER",
                    PeerTransport.UNKNOWN,
                    Set.of("image_conversion"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("text_analysis"),
                    50L,
                    175L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));

            List<PeerRegistryRecord> peers = db.loadPeerRecords();
            assertEquals(1, peers.size());
            PeerRegistryRecord peer = peers.getFirst();
            assertEquals("peer-1", peer.peerId());
            assertEquals(100L, peer.firstSeenAtMillis());
            assertEquals(PeerTransport.RABBITMQ, peer.transport());
            assertEquals(Set.of("TEXT_ANALYSIS"), peer.supportedTaskTypes());
        } finally {
            db.close();
        }
    }

    @Test
    void peerRegistryMetadataCoexistsWithTaskRetryHistoryRows() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-task-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-lease",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("test_task"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            db.insertJob("job-retry", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-retry", "job-retry");
            assertTrue(db.markTaskAssigned("task-retry", "peer-lease", 200L));
            assertTrue(db.markTaskRetried("task-retry", 1));
        } finally {
            db.close();
        }

        DatabaseManager reopened = new DatabaseManager(dbPath.toString());
        try {
            List<PeerRegistryRecord> peers = reopened.loadPeerRecords();
            assertEquals(1, peers.size());
            assertEquals("peer-lease", peers.getFirst().peerId());
            assertEquals(PeerStatus.CONNECTED, peers.getFirst().status());

            List<DatabaseManager.TaskRecord> tasks = reopened.getTasksForJob("job-retry");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(1, task.retryCount());
        } finally {
            reopened.close();
        }
    }

    @Test
    void rejectsTaskRowsWithoutExistingJob() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-foreign-key-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertFalse(db.insertTask("orphan-task", "missing-job"));
            assertEquals(0, db.getTasksForJob("missing-job").size());
            assertTrue(tasksTableReferencesJobs(dbPath));
        } finally {
            db.close();
        }
    }

    @Test
    void retriedTaskRowsClearPreviousAssignmentState() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-retry-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-retry", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-retry", "job-retry");
            db.markTaskAssigned("task-retry", "peer-1", 123L);

            assertTrue(db.markTaskRetried("task-retry", 1));

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-retry");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("PENDING", task.status());
            assertEquals(1, task.retryCount());
            assertNull(task.assignedPeerId());
            assertEquals(0L, task.startedAt());
            assertEquals(0L, task.completedAt());
            assertEquals(0L, task.durationMs());
            assertEquals("", task.leaseOwnerId());
            assertEquals(0L, task.leaseExpiresAt());
        } finally {
            db.close();
        }
    }

    @Test
    void assignedTaskPersistsAssignmentIdentityLeaseAndAuditForResume() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-lease-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-lease", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-lease", "job-lease");

            assertTrue(db.markTaskAssigned(
                    "task-lease",
                    "peer-1",
                    100L,
                    "COORDINATOR_A",
                    900L,
                    7,
                    ASSIGNMENT_ID));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-lease").getFirst();
            assertEquals("ASSIGNED", task.status());
            assertEquals("peer-1", task.assignedPeerId());
            assertEquals(100L, task.startedAt());
            assertEquals("COORDINATOR_A", task.leaseOwnerId());
            assertEquals(900L, task.leaseExpiresAt());
            assertEquals(7, task.attemptNumber());
            assertEquals(ASSIGNMENT_ID, task.assignmentId());

            JobStateStore.ResumableTaskState resumedTask =
                    db.loadRunningJobsForResume().getFirst().tasks().getFirst();
            assertEquals("ASSIGNED", resumedTask.status());
            assertEquals("peer-1", resumedTask.assignedPeerId());
            assertEquals(100L, resumedTask.startedAt());
            assertEquals("COORDINATOR_A", resumedTask.leaseOwnerId());
            assertEquals(900L, resumedTask.leaseExpiresAt());
            assertEquals(7, resumedTask.attemptNumber());
            assertEquals(ASSIGNMENT_ID, resumedTask.assignmentId());

            JobStateStore.TaskAttemptRecord attempt = db.loadTaskAttempts("job-lease").getFirst();
            assertEquals(7, attempt.attemptNumber());
            assertEquals(ASSIGNMENT_ID, attempt.assignmentId());
            assertEquals("peer-1", attempt.peerId());
            assertEquals(100L, attempt.startedAt());
            assertEquals(900L, attempt.leaseExpiresAt());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempt.outcome());
        } finally {
            db.close();
        }
    }

    @Test
    void releaseExpiredTaskLeaseForResumeClearsAssignmentAndClosesAttempt() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-expired-lease-release-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-expired-lease", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-expired-lease", "job-expired-lease");
            assertTrue(db.markTaskAssigned(
                    "task-expired-lease",
                    "peer-1",
                    100L,
                    "COORDINATOR_A",
                    150L));

            assertFalse(db.releaseExpiredTaskLeaseForResume("task-expired-lease", 149L));
            assertTrue(db.releaseExpiredTaskLeaseForResume("task-expired-lease", 175L));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-expired-lease").getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(0, task.retryCount());
            assertEquals(0L, task.startedAt());
            assertEquals("", task.leaseOwnerId());
            assertEquals(0L, task.leaseExpiresAt());
            assertEquals(1, task.attemptNumber());
            assertNull(task.assignmentId());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-expired-lease");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, attempt.outcome());
            assertEquals("lease_expired", attempt.failureReason());
            assertEquals(175L, attempt.finishedAt());
            assertEquals(75L, attempt.durationMs());
        } finally {
            db.close();
        }
    }

    @Test
    void persistsTaskAttemptHistoryAcrossRetryAndRestart() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-attempt-history-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-attempts", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-attempts", "job-attempts");
            assertTrue(db.markTaskAssigned("task-attempts", "peer-1", 100L));
            assertTrue(db.markTaskRetried(
                    "task-attempts",
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "processor failed",
                    175L));
            assertTrue(db.markTaskAssigned("task-attempts", "peer-2", 220L));
            assertTrue(db.markTaskCompleted("task-attempts", 280L, 60L, "result"));
        } finally {
            db.close();
        }

        DatabaseManager reopened = new DatabaseManager(dbPath.toString());
        try {
            List<JobStateStore.TaskAttemptRecord> attempts = reopened.loadTaskAttempts("job-attempts");
            assertEquals(2, attempts.size());

            JobStateStore.TaskAttemptRecord first = attempts.getFirst();
            assertEquals(1, first.attemptNumber());
            assertEquals("peer-1", first.peerId());
            assertNotNull(first.assignmentId());
            assertEquals(100L, first.startedAt());
            assertEquals(0L, first.leaseExpiresAt());
            assertEquals(175L, first.finishedAt());
            assertEquals(75L, first.durationMs());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, first.outcome());
            assertEquals("processor failed", first.failureReason());

            JobStateStore.TaskAttemptRecord second = attempts.get(1);
            assertEquals(2, second.attemptNumber());
            assertEquals("peer-2", second.peerId());
            assertNotNull(second.assignmentId());
            assertNotEquals(first.assignmentId(), second.assignmentId());
            assertEquals(220L, second.startedAt());
            assertEquals(280L, second.finishedAt());
            assertEquals(60L, second.durationMs());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, second.outcome());
            assertEquals("", second.failureReason());
        } finally {
            reopened.close();
        }
    }

    @Test
    void resetTaskForResumeRecordsRestartReleaseAttempt() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-restart-release-attempt-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-restart-release", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-restart-release", "job-restart-release");
            assertTrue(db.markTaskAssigned("task-restart-release", "peer-1", 123L));
            assertTrue(db.resetTaskForResume("task-restart-release"));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-restart-release").getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(0, task.retryCount());
            assertEquals(1, task.attemptNumber());
            assertNull(task.assignmentId());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-restart-release");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals("peer-1", attempt.peerId());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, attempt.outcome());
            assertEquals("coordinator_restart", attempt.failureReason());
            assertTrue(attempt.finishedAt() >= 123L);
        } finally {
            db.close();
        }
    }

    @Test
    void markRunningJobsFailedOnStartupClosesRunningAttempts() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-startup-failed-attempt-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-startup-fail", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-startup-fail", "job-startup-fail");
            assertTrue(db.markTaskAssigned("task-startup-fail", "peer-1", 100L));

            assertEquals(1, db.markRunningJobsFailedOnStartup(250L));

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-startup-fail");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.JOB_FAILED, attempt.outcome());
            assertEquals("coordinator_startup_reconciliation", attempt.failureReason());
            assertEquals(250L, attempt.finishedAt());
            assertEquals(150L, attempt.durationMs());
        } finally {
            db.close();
        }
    }

    @Test
    void persistsTaskPayloadsAndCompletedResultsForResume() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-resume-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());
        String requesterTokenHash = RequesterTokens.hashToken("resume-token");
        String requesterIdentityKey = "resume-public-key";

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-resume",
                    "TEST_TASK",
                    "requester-1",
                    requesterTokenHash,
                    requesterIdentityKey,
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-resume-0", "payload-alpha"),
                            new JobStateStore.TaskStartupState("task-job-resume-1", "payload-beta")
                    )
            ));
            db.markTaskAssigned("task-job-resume-0", "peer-1", 123L);
            assertTrue(db.markTaskCompleted("task-job-resume-0", 456L, 333L, "result-alpha"));

            List<JobStateStore.ResumableJobState> jobs = db.loadRunningJobsForResume();
            assertEquals(1, jobs.size());
            JobStateStore.ResumableJobState job = jobs.getFirst();
            assertEquals("job-resume", job.jobId());
            assertEquals("TEST_TASK", job.taskType());
            assertEquals("requester-1", job.requesterId());
            assertEquals(requesterTokenHash, job.requesterTokenHash());
            assertEquals(requesterIdentityKey, job.requesterIdentityKey());
            assertEquals("csv", job.parameter());
            assertEquals(2, job.tasks().size());

            JobStateStore.ResumableTaskState completed = job.tasks().getFirst();
            assertEquals("task-job-resume-0", completed.taskId());
            assertEquals("COMPLETED", completed.status());
            assertEquals("payload-alpha", completed.payload());
            assertEquals("result-alpha", completed.resultPayload());

            JobStateStore.ResumableTaskState pending = job.tasks().get(1);
            assertEquals("task-job-resume-1", pending.taskId());
            assertEquals("PENDING", pending.status());
            assertEquals("payload-beta", pending.payload());
            assertNull(pending.resultPayload());
        } finally {
            db.close();
        }
    }

    @Test
    void loadsCompletedJobResultFromPersistedTaskResults() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-completed-result-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());
        String requesterTokenHash = RequesterTokens.hashToken("completed-token");
        String requesterIdentityKey = "completed-public-key";

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-completed-result",
                    "TEST_TASK",
                    "requester-1",
                    requesterTokenHash,
                    requesterIdentityKey,
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-completed-result-1", "payload-beta"),
                            new JobStateStore.TaskStartupState("task-job-completed-result-0", "payload-alpha")
                    )
            ));
            assertTrue(db.markTaskAssigned("task-job-completed-result-0", "peer-1", 123L));
            assertTrue(db.markTaskCompleted("task-job-completed-result-0", 456L, 333L, "result-alpha"));
            assertTrue(db.markTaskAssigned("task-job-completed-result-1", "peer-2", 789L));
            assertTrue(db.markTaskCompleted("task-job-completed-result-1", 987L, 198L, "result-beta"));
            assertTrue(db.markJobCompleted("job-completed-result"));

            var result = db.loadCompletedJobResult("job-completed-result");

            assertTrue(result.isPresent());
            assertEquals("job-completed-result", result.get().jobId());
            assertEquals("TEST_TASK", result.get().taskType());
            assertEquals(requesterTokenHash, result.get().requesterTokenHash());
            assertEquals(requesterIdentityKey, result.get().requesterIdentityKey());
            assertEquals(List.of("result-alpha", "result-beta"), result.get().resultsByTaskId());
        } finally {
            db.close();
        }
    }

    @Test
    void loadsCompletedJobSemanticResultPayload() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-completed-semantic-result-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-semantic-result",
                    "TEXT_ANALYSIS",
                    "requester-1",
                    RequesterTokens.hashToken("completed-token"),
                    "",
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-semantic-result-0", "payload-alpha"),
                            new JobStateStore.TaskStartupState("task-job-semantic-result-1", "payload-beta")
                    )
            ));
            assertTrue(db.markTaskAssigned("task-job-semantic-result-0", "peer-1", 123L));
            assertTrue(db.markTaskCompleted("task-job-semantic-result-0", 456L, 333L, "result-alpha"));
            assertTrue(db.markTaskAssigned("task-job-semantic-result-1", "peer-2", 789L));
            assertTrue(db.markTaskCompleted("task-job-semantic-result-1", 987L, 198L, "result-beta"));
            assertTrue(db.markJobCompleted("job-semantic-result", Map.of(
                    "documentCount", 2,
                    "totalWords", 42)));

            var result = db.loadCompletedJobResult("job-semantic-result");

            assertTrue(result.isPresent());
            assertEquals(Map.of("documentCount", 2.0, "totalWords", 42.0), result.get().resultPayload());
            assertEquals(List.of("result-alpha", "result-beta"), result.get().resultsByTaskId());
        } finally {
            db.close();
        }
    }

    @Test
    void jobCompletionRejectsMissingTaskResults() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-incomplete-result-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-incomplete-result",
                    "TEST_TASK",
                    "requester-1",
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-incomplete-result-0", "payload-alpha"),
                            new JobStateStore.TaskStartupState("task-job-incomplete-result-1", "payload-beta")
                    )
            ));
            assertTrue(db.markTaskAssigned("task-job-incomplete-result-0", "peer-1", 123L));
            assertTrue(db.markTaskCompleted("task-job-incomplete-result-0", 456L, 333L, "result-alpha"));
            assertFalse(db.markJobCompleted("job-incomplete-result"));
            assertEquals("RUNNING", db.getJobHistory().getFirst().status());

            assertTrue(db.loadCompletedJobResult("job-incomplete-result").isEmpty());
        } finally {
            db.close();
        }
    }

    @Test
    void resetTaskForResumeClearsStaleAssignmentWithoutIncrementingRetry() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-resume-reset-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJobWithTasks(
                    "job-reset",
                    "TEST_TASK",
                    "requester-1",
                    "",
                    List.of(new JobStateStore.TaskStartupState("task-job-reset-0", "payload"))
            );
            db.markTaskAssigned("task-job-reset-0", "peer-1", 123L);

            assertTrue(db.resetTaskForResume("task-job-reset-0"));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-reset").getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(0L, task.startedAt());
            assertEquals(0, task.retryCount());
        } finally {
            db.close();
        }
    }

    @Test
    void taskStatusUpdatesRejectInvalidTransitions() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-transition-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-completed", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-completed", "job-completed");
            assertFalse(db.markTaskCompleted("task-completed", 100L, 10L));
            assertTrue(db.markTaskAssigned("task-completed", "peer-1", 100L));
            assertTrue(db.markTaskCompleted("task-completed", 200L, 100L));
            assertFalse(db.markTaskRetried("task-completed", 1));
            assertFalse(db.markTaskFailed("task-completed"));
            assertFalse(db.markTaskAssigned("task-completed", "peer-2", 300L));

            DatabaseManager.TaskRecord completed = db.getTasksForJob("job-completed").getFirst();
            assertEquals("COMPLETED", completed.status());
            assertEquals("peer-1", completed.assignedPeerId());
            assertEquals(200L, completed.completedAt());
            assertEquals(100L, completed.durationMs());
            assertEquals(0, completed.retryCount());

            db.insertJob("job-failed", "TEST_TASK", "requester-2", 1);
            db.insertTask("task-failed", "job-failed");
            assertTrue(db.markTaskFailed("task-failed"));
            assertFalse(db.markTaskAssigned("task-failed", "peer-3", 400L));
            assertFalse(db.markTaskCompleted("task-failed", 500L, 100L));
            assertFalse(db.markTaskRetried("task-failed", 1));

            DatabaseManager.TaskRecord failed = db.getTasksForJob("job-failed").getFirst();
            assertEquals("FAILED", failed.status());
            assertNull(failed.assignedPeerId());
            assertEquals(0, failed.retryCount());
        } finally {
            db.close();
        }
    }

    @Test
    void jobStatusUpdatesRejectTerminalOverwrites() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-job-transition-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-completed", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-job-completed", "job-completed");
            assertFalse(db.markJobCompleted("job-completed"));
            assertTrue(db.markTaskAssigned("task-job-completed", "peer-1", 100L));
            assertTrue(db.markTaskCompleted("task-job-completed", 200L, 100L, "result"));
            assertEquals("FINALIZING", db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("job-completed"))
                    .findFirst()
                    .orElseThrow()
                    .status());
            assertTrue(db.markJobCompleted("job-completed"));
            assertFalse(db.markJobFailed("job-completed"));

            DatabaseManager.JobRecord completed = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("job-completed"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completed.status());

            db.insertJob("job-failed", "TEST_TASK", "requester-2", 1);
            assertTrue(db.markJobFailed("job-failed"));
            assertFalse(db.markJobCompleted("job-failed"));

            DatabaseManager.JobRecord failed = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("job-failed"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", failed.status());
        } finally {
            db.close();
        }
    }

    @Test
    void migratesLegacyTasksTableToForeignKeySchema() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-legacy-migration-test.db");
        createLegacyDatabaseWithoutTaskForeignKey(dbPath);

        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, db.getSchemaVersion());
            assertTrue(tasksTableReferencesJobs(dbPath));
            assertTrue(columnExists(dbPath, "jobs", "parameter"));
            assertTrue(columnExists(dbPath, "jobs", "requester_token_hash"));
            assertTrue(columnExists(dbPath, "jobs", "requester_identity_key"));
            assertTrue(columnExists(dbPath, "jobs", "result_payload_json"));
            assertTrue(columnExists(dbPath, "jobs", "request_hash"));
            assertTrue(columnExists(dbPath, "tasks", "payload_json"));
            assertTrue(columnExists(dbPath, "tasks", "result_payload_json"));
            assertTrue(columnExists(dbPath, "tasks", "lease_owner_id"));
            assertTrue(columnExists(dbPath, "tasks", "lease_expires_at"));
            assertTrue(tableExists(dbPath, "task_attempts"));
            assertEquals(1, db.getTasksForJob("legacy-job").size());
            assertFalse(db.insertTask("orphan-task", "missing-job"));
        } finally {
            db.close();
        }
    }

    @Test
    void migratesVersion9AssignmentStateAndAttemptAuditThroughCurrentSchema() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-v9-assignment-migration-test.db");
        createVersion9AssignmentDatabase(dbPath);

        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertEquals(13, DatabaseManager.CURRENT_SCHEMA_VERSION);
            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, db.getSchemaVersion());
            assertTrue(columnExists(dbPath, "tasks", "attempt_number"));
            assertTrue(columnExists(dbPath, "tasks", "assignment_id"));
            assertTrue(columnExists(dbPath, "task_attempts", "assignment_id"));
            assertTrue(columnExists(dbPath, "task_attempts", "lease_expires_at"));

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-v9-migration");
            DatabaseManager.TaskRecord pending = taskById(tasks, "task-v9-pending");
            assertEquals("PENDING", pending.status());
            assertEquals(0, pending.attemptNumber());
            assertNull(pending.assignmentId());

            DatabaseManager.TaskRecord assigned = taskById(tasks, "task-v9-assigned");
            assertEquals("ASSIGNED", assigned.status());
            assertEquals("peer-legacy", assigned.assignedPeerId());
            assertEquals("COORDINATOR_old", assigned.leaseOwnerId());
            assertEquals(900L, assigned.leaseExpiresAt());
            assertEquals(0, assigned.attemptNumber());
            assertNull(assigned.assignmentId());

            DatabaseManager.TaskRecord completed = taskById(tasks, "task-v9-completed");
            assertEquals("COMPLETED", completed.status());
            assertEquals(250L, completed.completedAt());
            assertEquals(0, completed.attemptNumber());
            assertNull(completed.assignmentId());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-v9-migration");
            assertEquals(2, attempts.size());
            JobStateStore.TaskAttemptRecord legacyRunning = attempts.stream()
                    .filter(attempt -> attempt.taskId().equals("task-v9-assigned"))
                    .findFirst()
                    .orElseThrow();
            assertEquals(4, legacyRunning.attemptNumber());
            assertNull(legacyRunning.assignmentId());
            assertEquals("peer-legacy", legacyRunning.peerId());
            assertEquals(100L, legacyRunning.startedAt());
            assertEquals(0L, legacyRunning.leaseExpiresAt());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, legacyRunning.outcome());

            JobStateStore.TaskAttemptRecord completedAttempt = attempts.stream()
                    .filter(attempt -> attempt.taskId().equals("task-v9-completed"))
                    .findFirst()
                    .orElseThrow();
            assertNull(completedAttempt.assignmentId());
            assertEquals(0L, completedAttempt.leaseExpiresAt());
            assertEquals(250L, completedAttempt.finishedAt());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, completedAttempt.outcome());
        }
    }

    @Test
    void migratesVersion10CompletedRunningJobToFinalizingIntent() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-v10-finalizing-migration-test.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertTrue(db.insertJobWithTasks(
                    "job-v10-finalizing",
                    "TEST_TASK",
                    "requester-1",
                    RequesterTokens.hashToken("requester-token"),
                    "",
                    "",
                    List.of(new JobStateStore.TaskStartupState("task-v10-finalizing", "payload"))
            ));
            assertTrue(db.markTaskAssigned(
                    "task-v10-finalizing",
                    "peer-1",
                    100L,
                    "lease",
                    900L,
                    1,
                    ASSIGNMENT_ID
            ));
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            "task-v10-finalizing",
                            1,
                            ASSIGNMENT_ID,
                            "peer-1",
                            200L,
                            100L,
                            "result"
                    )
            );
        }

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE jobs SET status='RUNNING' WHERE job_id='job-v10-finalizing'");
            stmt.execute("UPDATE schema_version SET version=10 WHERE id=1");
        }

        try (DatabaseManager migrated = new DatabaseManager(dbPath.toString())) {
            assertEquals(13, migrated.getSchemaVersion());
            assertEquals("FINALIZING", migrated.getJobHistory().getFirst().status());
            assertTrue(columnExists(dbPath, "jobs", "request_hash"));
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.LEGACY_CONFLICT,
                    migrated.inspectJobSubmission(
                            "job-v10-finalizing",
                            RequesterTokens.hashToken("requester-token"),
                            "",
                            "v1:cannot-reconstruct"
                    ).outcome()
            );
            JobStateStore.ResumableTaskState task = migrated.loadRunningJobsForResume()
                    .getFirst()
                    .tasks()
                    .getFirst();
            assertEquals("COMPLETED", task.status());
            assertEquals("result", task.resultPayload());
            assertTrue(task.resultPayloadPresent());
        }
    }

    @Test
    void rejectsFinalizingJobWithoutCompleteResultBearingTaskSet() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-invalid-finalizing-state.db");
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            db.insertJob("job-invalid-finalizing", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-invalid-finalizing", "job-invalid-finalizing");
        }
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE jobs SET status='FINALIZING' WHERE job_id='job-invalid-finalizing'");
        }

        assertThrows(SQLException.class, () -> new DatabaseManager(dbPath.toString()));
        assertEquals(13, schemaVersion(dbPath));
    }

    @Test
    void rollsBackCurrentSchemaMigrationWhenSchemaValidationFails() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-v10-migration-rollback-test.db");
        createVersion9AssignmentDatabase(dbPath);
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE task_attempts DROP COLUMN failure_reason");
        }

        assertThrows(SQLException.class, () -> new DatabaseManager(dbPath.toString()));

        assertEquals(9, schemaVersion(dbPath));
        assertFalse(columnExists(dbPath, "tasks", "attempt_number"));
        assertFalse(columnExists(dbPath, "tasks", "assignment_id"));
        assertFalse(columnExists(dbPath, "task_attempts", "assignment_id"));
        assertFalse(columnExists(dbPath, "task_attempts", "lease_expires_at"));
        assertFalse(columnExists(dbPath, "jobs", "request_hash"));
    }

    @Test
    void rejectsDatabaseSchemaNewerThanRuntimeSupports() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-future-schema-test.db");
        createFutureSchemaVersionDatabase(dbPath);

        SQLException failure = assertThrows(SQLException.class, () -> new DatabaseManager(dbPath.toString()));
        assertTrue(failure.getMessage().contains("newer than supported version"));
    }

    @Test
    void marksRunningJobsAndNonTerminalTasksFailedOnStartup() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-recovery-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("running-job", "TEST_TASK", "requester-1", 2);
            db.insertTask("completed-task", "running-job");
            db.insertTask("assigned-task", "running-job");
            db.markTaskAssigned("completed-task", "peer-1", 100L);
            db.markTaskCompleted("completed-task", 200L, 100L);
            db.markTaskAssigned("assigned-task", "peer-2", 150L);

            db.insertJob("completed-job", "TEST_TASK", "requester-2", 1);
            db.insertTask("completed-job-task", "completed-job");
            db.markTaskAssigned("completed-job-task", "peer-3", 220L);
            db.markTaskCompleted("completed-job-task", 250L, 50L, "result");
            db.markJobCompleted("completed-job");

            assertEquals(1, db.markRunningJobsFailedOnStartup(999L));

            DatabaseManager.JobRecord runningJob = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("running-job"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", runningJob.status());
            assertEquals(999L, runningJob.completedAt());

            DatabaseManager.JobRecord completedJob = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("completed-job"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completedJob.status());
            assertTrue(completedJob.completedAt() > 0L);

            List<DatabaseManager.TaskRecord> runningTasks = db.getTasksForJob("running-job");
            DatabaseManager.TaskRecord completedTask = runningTasks.stream()
                    .filter(task -> task.taskId().equals("completed-task"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completedTask.status());
            assertEquals(200L, completedTask.completedAt());

            DatabaseManager.TaskRecord assignedTask = runningTasks.stream()
                    .filter(task -> task.taskId().equals("assigned-task"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", assignedTask.status());
            assertEquals(999L, assignedTask.completedAt());
        } finally {
            db.close();
        }
    }

    @Test
    void rollsBackAtomicJobStartupWhenTaskInsertFails() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-startup-rollback-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("existing-job", "TEST_TASK", "requester-1", 1);
            db.insertTask("duplicate-task", "existing-job");

            assertFalse(db.insertJobWithTasks(
                    "new-job",
                    "TEST_TASK",
                    "requester-2",
                    1,
                    List.of("duplicate-task")
            ));

            assertTrue(db.getJobHistory().stream().noneMatch(job -> job.jobId().equals("new-job")));
            assertEquals(0, db.getTasksForJob("new-job").size());
            assertEquals(1, db.getTasksForJob("existing-job").size());
        } finally {
            db.close();
        }
    }

    private static BrokerOutboxStore.OutboxMessage taskAssignmentOutboxTemplate(String peerId,
                                                                                String taskId,
                                                                                String jobId) {
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.TASK_ASSIGN,
                peerId,
                "COORDINATOR",
                new TaskAssignMessage(
                        peerId,
                        "2026-07-02T00:00:00Z",
                        taskId,
                        jobId,
                        "TEST_TASK",
                        "payload",
                        ""
                )
        );
    }

    private static JobStateStore.JobSubmissionOutcome commitAfterStart(DatabaseManager db,
                                                                        CountDownLatch start)
            throws Exception {
        assertTrue(start.await(2, TimeUnit.SECONDS));
        return db.commitJobSubmission(
                "job-concurrent-submit",
                "TEST_TASK",
                "requester-route",
                RequesterTokens.hashToken("owner-token"),
                "owner-key",
                "v1:concurrent-request",
                "",
                List.of(
                        new JobStateStore.TaskStartupState("task-job-concurrent-submit-0", "payload-0"),
                        new JobStateStore.TaskStartupState("task-job-concurrent-submit-1", "payload-1")
                )
        ).outcome();
    }

    private static BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(String requesterId,
                                                                          JobResultMessage result) {
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.JOB_RESULT,
                requesterId,
                "COORDINATOR",
                result
        );
    }

    private static void createLegacyDatabaseWithoutTaskForeignKey(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE jobs (
                    job_id           TEXT    PRIMARY KEY,
                    task_type        TEXT    NOT NULL,
                    requester_node_id TEXT   NOT NULL,
                    status           TEXT    NOT NULL DEFAULT 'RUNNING',
                    submitted_at     INTEGER NOT NULL,
                    completed_at     INTEGER,
                    file_count       INTEGER NOT NULL
                )
            """);
            stmt.execute("""
                CREATE TABLE tasks (
                    task_id          TEXT    PRIMARY KEY,
                    job_id           TEXT    NOT NULL,
                    assigned_peer_id TEXT,
                    status           TEXT    NOT NULL DEFAULT 'PENDING',
                    started_at       INTEGER,
                    completed_at     INTEGER,
                    duration_ms      INTEGER,
                    retry_count      INTEGER NOT NULL DEFAULT 0
                )
            """);
            stmt.execute("""
                INSERT INTO jobs(job_id, task_type, requester_node_id, status, submitted_at, file_count)
                VALUES('legacy-job', 'TEST_TASK', 'requester-1', 'RUNNING', 100, 1)
            """);
            stmt.execute("""
                INSERT INTO tasks(task_id, job_id, status)
                VALUES('legacy-task', 'legacy-job', 'PENDING')
            """);
        }
    }

    private static void createVersion9AssignmentDatabase(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA foreign_keys=ON");
            stmt.execute("""
                CREATE TABLE schema_version (
                    id         INTEGER PRIMARY KEY CHECK (id = 1),
                    version    INTEGER NOT NULL CHECK (version >= 0),
                    applied_at INTEGER NOT NULL
                )
            """);
            stmt.execute("INSERT INTO schema_version(id, version, applied_at) VALUES(1, 9, 100)");
            stmt.execute("""
                CREATE TABLE jobs (
                    job_id                  TEXT    PRIMARY KEY,
                    task_type               TEXT    NOT NULL,
                    requester_node_id       TEXT    NOT NULL,
                    requester_token_hash    TEXT    NOT NULL DEFAULT '',
                    requester_identity_key  TEXT    NOT NULL DEFAULT '',
                    status                  TEXT    NOT NULL DEFAULT 'RUNNING',
                    submitted_at            INTEGER NOT NULL,
                    completed_at            INTEGER,
                    file_count              INTEGER NOT NULL,
                    parameter               TEXT    NOT NULL DEFAULT '',
                    result_payload_json      TEXT
                )
            """);
            stmt.execute("""
                CREATE TABLE tasks (
                    task_id             TEXT    PRIMARY KEY,
                    job_id              TEXT    NOT NULL,
                    assigned_peer_id    TEXT,
                    status              TEXT    NOT NULL DEFAULT 'PENDING',
                    started_at          INTEGER,
                    completed_at        INTEGER,
                    duration_ms         INTEGER,
                    retry_count         INTEGER NOT NULL DEFAULT 0,
                    payload_json        TEXT,
                    result_payload_json TEXT,
                    lease_owner_id      TEXT    NOT NULL DEFAULT '',
                    lease_expires_at    INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                )
            """);
            stmt.execute("""
                CREATE TABLE task_attempts (
                    attempt_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id         TEXT    NOT NULL,
                    task_id        TEXT    NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    peer_id        TEXT    NOT NULL,
                    started_at     INTEGER NOT NULL,
                    finished_at    INTEGER,
                    duration_ms    INTEGER,
                    outcome        TEXT    NOT NULL DEFAULT 'RUNNING',
                    failure_reason TEXT    NOT NULL DEFAULT '',
                    UNIQUE(task_id, attempt_number),
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
                )
            """);
            stmt.execute("""
                INSERT INTO jobs(
                    job_id, task_type, requester_node_id, requester_token_hash,
                    status, submitted_at, file_count, parameter
                ) VALUES('job-v9-migration', 'TEST_TASK', 'requester-1', 'token-hash',
                         'RUNNING', 50, 3, '')
            """);
            stmt.execute("""
                INSERT INTO tasks(
                    task_id, job_id, assigned_peer_id, status, started_at,
                    completed_at, duration_ms, retry_count, payload_json,
                    result_payload_json, lease_owner_id, lease_expires_at
                ) VALUES
                    ('task-v9-pending', 'job-v9-migration', NULL, 'PENDING', NULL,
                     NULL, NULL, 0, '"pending"', NULL, '', 0),
                    ('task-v9-assigned', 'job-v9-migration', 'peer-legacy', 'ASSIGNED', 100,
                     NULL, NULL, 1, '"assigned"', NULL, 'COORDINATOR_old', 900),
                    ('task-v9-completed', 'job-v9-migration', 'peer-complete', 'COMPLETED', 200,
                     250, 50, 0, '"completed"', '"result"', '', 0)
            """);
            stmt.execute("""
                INSERT INTO task_attempts(
                    job_id, task_id, attempt_number, peer_id, started_at,
                    finished_at, duration_ms, outcome, failure_reason
                ) VALUES
                    ('job-v9-migration', 'task-v9-assigned', 4, 'peer-legacy', 100,
                     NULL, NULL, 'RUNNING', ''),
                    ('job-v9-migration', 'task-v9-completed', 2, 'peer-complete', 200,
                     250, 50, 'SUCCEEDED', '')
            """);
        }
    }

    private static DatabaseManager.TaskRecord taskById(List<DatabaseManager.TaskRecord> tasks, String taskId) {
        return tasks.stream()
                .filter(task -> task.taskId().equals(taskId))
                .findFirst()
                .orElseThrow();
    }

    private static ObjectReference outputReference(String jobId,
                                                   String taskId,
                                                   int attemptNumber,
                                                   String assignmentId,
                                                   String digestCharacter) {
        return new ObjectReference(
                TaskFlowObjectKeys.attemptOutputKey(
                        jobId,
                        taskId,
                        attemptNumber,
                        assignmentId
                ),
                12L,
                digestCharacter.repeat(64),
                "application/octet-stream"
        );
    }

    private static int schemaVersion(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT version FROM schema_version WHERE id=1")) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

    private static void createFutureSchemaVersionDatabase(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE schema_version (
                    id         INTEGER PRIMARY KEY CHECK (id = 1),
                    version    INTEGER NOT NULL CHECK (version >= 0),
                    applied_at INTEGER NOT NULL
                )
            """);
            stmt.execute("INSERT INTO schema_version(id, version, applied_at) VALUES(1, 999, 100)");
        }
    }

    private static boolean tasksTableReferencesJobs(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA foreign_key_list(tasks)")) {
            while (rs.next()) {
                if ("jobs".equals(rs.getString("table"))
                        && "job_id".equals(rs.getString("from"))
                        && "job_id".equals(rs.getString("to"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean columnExists(Path dbPath, String tableName, String columnName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {
            while (rs.next()) {
                if (columnName.equals(rs.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean tableExists(Path dbPath, String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             var ps = conn.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name=?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }
}
