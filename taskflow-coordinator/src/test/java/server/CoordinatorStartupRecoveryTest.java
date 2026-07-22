package server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.RequesterTokens;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.runtime.TaskFlowClock;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorStartupRecoveryTest {
    private static final String TOKEN_HASH = RequesterTokens.hashToken("resume-token");
    private static final String IDENTITY_KEY = "resume-public-key";
    private static final String ASSIGNMENT_ID = "550e8400-e29b-41d4-a716-446655440000";

    @TempDir
    Path tempDir;

    @Test
    void reconcilesAbandonedJobsUsingProvidedCompletionTimestamp() {
        RecordingStore store = new RecordingStore(2);

        assertTrue(CoordinatorStartupRecovery.reconcileAbandonedJobs(store, 123L));

        assertEquals(123L, store.completedAt());
        assertEquals(1, store.reconciliationCalls());
    }

    @Test
    void succeedsWhenNoRunningJobsNeedReconciliation() {
        RecordingStore store = new RecordingStore(0);

        assertTrue(CoordinatorStartupRecovery.reconcileAbandonedJobs(store, 456L));

        assertEquals(456L, store.completedAt());
        assertEquals(1, store.reconciliationCalls());
    }

    @Test
    void reportsFailureWhenStateStoreCannotReconcile() {
        RecordingStore store = new RecordingStore(-1);

        assertFalse(CoordinatorStartupRecovery.reconcileAbandonedJobs(store, 789L));

        assertEquals(789L, store.completedAt());
        assertEquals(1, store.reconciliationCalls());
    }

    @Test
    void resumesRunningJobsWithPersistedPendingPayloads() {
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-resume",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                TOKEN_HASH,
                IDENTITY_KEY,
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        "task-job-resume-0",
                        "PENDING",
                        "payload",
                        null,
                        2
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result = CoordinatorStartupRecovery.recoverPersistedJobs(
                store,
                new FixedClock(123L),
                () -> ASSIGNMENT_ID
        );

        assertTrue(result.successful());
        assertEquals(1, result.resumedJobs().size());
        assertEquals(TOKEN_HASH, result.requesterTokenHashes().get("job-resume"));
        assertEquals(IDENTITY_KEY, result.requesterIdentityKeys().get("job-resume"));
        assertEquals(0, result.failedJobs());
        assertEquals(List.of("task-job-resume-0"), store.resetTasks());
        assertEquals(List.of(), store.releasedLeases());

        EmbarrassinglyParallelJob<?, ?> job = result.resumedJobs().getFirst();
        TaskUnit<?> task = job.getTasks().get("task-job-resume-0");
        assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
        assertEquals(2, task.getRetryCount());
        assertEquals(123L, task.getPendingSinceMillis());
        assertTrue(task.markAssigned("peer-recovered", 123L));
        assertEquals(ASSIGNMENT_ID, task.getAssignmentIdentity().orElseThrow().assignmentId());
    }

    @Test
    void preservesAssignedTasksWithUnexpiredLeasesOnResume() {
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-assigned-lease",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                TOKEN_HASH,
                "",
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        "task-job-assigned-lease-0",
                        "ASSIGNED",
                        "payload",
                        null,
                        1,
                        "peer-1",
                        100L,
                        "COORDINATOR_old",
                        500L,
                        3,
                        ASSIGNMENT_ID
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 250L);

        assertTrue(result.successful());
        assertEquals(1, result.resumedJobs().size());
        assertEquals(List.of(), store.resetTasks());
        assertEquals(List.of(), store.releasedLeases());

        TaskUnit<?> task = result.resumedJobs().getFirst().getTasks().get("task-job-assigned-lease-0");
        assertEquals(TaskUnit.TaskStatus.ASSIGNED, task.getStatus());
        assertEquals("peer-1", task.getAssignedPeerId());
        assertEquals(100L, task.getStartTime());
        assertEquals("COORDINATOR_old", task.getLeaseOwnerId());
        assertEquals(500L, task.getLeaseExpiresAtMillis());
        assertEquals(1, task.getRetryCount());
        AssignmentIdentity identity = task.getAssignmentIdentity().orElseThrow();
        assertEquals(3, identity.attemptNumber());
        assertEquals(ASSIGNMENT_ID, identity.assignmentId());
        assertEquals("peer-1", identity.workerId());
    }

    @Test
    void releasesExpiredAssignedLeasesOnResume() {
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-expired-lease",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                TOKEN_HASH,
                "",
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        "task-job-expired-lease-0",
                        "ASSIGNED",
                        "payload",
                        null,
                        3,
                        "peer-1",
                        100L,
                        "COORDINATOR_old",
                        200L,
                        4,
                        ASSIGNMENT_ID
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 250L);

        assertTrue(result.successful());
        assertEquals(1, result.resumedJobs().size());
        assertEquals(List.of("task-job-expired-lease-0:250"), store.releasedLeases());

        TaskUnit<?> task = result.resumedJobs().getFirst().getTasks().get("task-job-expired-lease-0");
        assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
        assertEquals(3, task.getRetryCount());
        assertEquals(4, task.getAttemptNumber());
        assertTrue(task.getAssignmentIdentity().isEmpty());
    }

    @Test
    void releasesLegacyAssignedTaskWithoutIdentityEvenWhenLeaseIsUnexpired() {
        String taskId = "task-job-legacy-assignment-0";
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-legacy-assignment",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                TOKEN_HASH,
                "",
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        taskId,
                        "ASSIGNED",
                        "payload",
                        null,
                        0,
                        "peer-legacy",
                        100L,
                        "COORDINATOR_old",
                        900L
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 250L);

        assertTrue(result.successful());
        assertEquals(List.of(taskId), store.resetTasks());
        assertEquals(List.of(), store.releasedLeases());
        TaskUnit<?> task = result.resumedJobs().getFirst().getTasks().get(taskId);
        assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
        assertEquals(0, task.getAttemptNumber());
        assertTrue(task.getAssignmentIdentity().isEmpty());
    }

    @Test
    void releasesAssignedTaskWithIncompleteLeaseMetadata() {
        String taskId = "task-job-incomplete-lease-0";
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-incomplete-lease",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                TOKEN_HASH,
                "",
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        taskId,
                        "ASSIGNED",
                        "payload",
                        null,
                        0,
                        "peer-legacy",
                        100L,
                        "",
                        900L,
                        2,
                        ASSIGNMENT_ID
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 250L);

        assertTrue(result.successful());
        assertEquals(List.of(taskId), store.resetTasks());
        assertEquals(List.of(), store.releasedLeases());
        TaskUnit<?> task = result.resumedJobs().getFirst().getTasks().get(taskId);
        assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
        assertEquals(2, task.getAttemptNumber());
    }

    @Test
    void version9AssignedTaskIsMigratedAndReleasedDuringStartupRecovery() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-v9-legacy-assignment.db");
        String jobId = "job-v9-legacy-assignment";
        String taskId = "task-job-v9-legacy-assignment-0";
        try (DatabaseManager current = new DatabaseManager(dbPath.toString())) {
            assertTrue(current.insertJobWithTasks(
                    jobId,
                    "RABBITMQ_TEST_TASK",
                    "requester-1",
                    TOKEN_HASH,
                    "",
                    List.of(new JobStateStore.TaskStartupState(taskId, "payload"))
            ));
            assertTrue(current.markTaskAssigned(
                    taskId,
                    "peer-legacy",
                    100L,
                    "COORDINATOR_old",
                    900L
            ));
        }
        downgradeAssignmentIdentitySchemaToVersion9(dbPath);

        try (DatabaseManager migrated = new DatabaseManager(dbPath.toString())) {
            JobStateStore.ResumableTaskState beforeRecovery =
                    migrated.loadRunningJobsForResume().getFirst().tasks().getFirst();
            assertEquals("ASSIGNED", beforeRecovery.status());
            assertEquals(0, beforeRecovery.attemptNumber());
            assertNull(beforeRecovery.assignmentId());

            CoordinatorStartupRecovery.RecoveryResult result =
                    CoordinatorStartupRecovery.recoverPersistedJobs(migrated, 250L);

            assertTrue(result.successful());
            TaskUnit<?> restored = result.resumedJobs().getFirst().getTasks().get(taskId);
            assertEquals(TaskUnit.TaskStatus.PENDING, restored.getStatus());
            assertEquals(1, restored.getAttemptNumber());
            assertTrue(restored.getAssignmentIdentity().isEmpty());

            DatabaseManager.TaskRecord persisted = migrated.getTasksForJob(jobId).getFirst();
            assertEquals("PENDING", persisted.status());
            assertEquals(1, persisted.attemptNumber());
            assertNull(persisted.assignmentId());
            JobStateStore.TaskAttemptRecord releasedAttempt = migrated.loadTaskAttempts(jobId).getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, releasedAttempt.outcome());
            assertEquals("coordinator_restart", releasedAttempt.failureReason());
        }
    }

    @Test
    void continuesAssignmentAttemptNumberAcrossCoordinatorRecovery() {
        String jobId = "job-attempt-recovery";
        String taskId = "task-job-attempt-recovery-0";
        ResumeStore store = new ResumeStore(
                List.of(new JobStateStore.ResumableJobState(
                        jobId,
                        "RABBITMQ_TEST_TASK",
                        "requester-1",
                        TOKEN_HASH,
                        "",
                        "",
                        List.of(new JobStateStore.ResumableTaskState(
                                taskId,
                                "PENDING",
                                "payload",
                                null,
                                1
                        ))
                )),
                List.of(new JobStateStore.TaskAttemptRecord(
                        jobId,
                        taskId,
                        7,
                        "peer-old",
                        100L,
                        200L,
                        100L,
                        JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                        "processor_failure"
                ))
        );

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 300L);

        assertTrue(result.successful());
        TaskUnit<?> task = result.resumedJobs().getFirst().getTasks().get(taskId);
        assertEquals(7, task.getAttemptNumber());
        assertEquals(1, task.getRetryCount());

        assertTrue(task.markAssigned("peer-new", 400L, "COORDINATOR_new", 900L));
        assertEquals(8, task.getAttemptNumber());
        assertEquals(8, task.getAssignmentIdentity().orElseThrow().attemptNumber());
        assertEquals(1, task.getRetryCount());
    }

    @Test
    void sqliteRestartRecoveryReconstructsCommittedRetryProjectionAndGeneration() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-generation-restart.db");
        String jobId = "job-generation-restart";
        String taskId = "task-job-generation-restart-0";

        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertTrue(db.insertJobWithTasks(
                    jobId,
                    "RABBITMQ_TEST_TASK",
                    "requester-1",
                    TOKEN_HASH,
                    "",
                    List.of(new JobStateStore.TaskStartupState(taskId, "payload"))
            ));
            assertTrue(db.markTaskAssigned(
                    taskId,
                    "peer-old",
                    100L,
                    "COORDINATOR_old",
                    200L,
                    7,
                    ASSIGNMENT_ID
            ));
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitAssignedTaskFailure(
                            taskId,
                            7,
                            ASSIGNMENT_ID,
                            "peer-old",
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "processor_failure",
                            175L
                    )
            );
        }

        AssignmentIdentity replacement;
        try (DatabaseManager reopened = new DatabaseManager(dbPath.toString())) {
            JobStateStore.ResumableTaskState persisted =
                    reopened.loadRunningJobsForResume().getFirst().tasks().getFirst();
            assertEquals("PENDING", persisted.status());
            assertEquals(7, persisted.attemptNumber());
            assertEquals(1, persisted.retryCount());
            assertNull(persisted.assignmentId());
            assertEquals("", persisted.leaseOwnerId());
            assertEquals(0L, persisted.leaseExpiresAt());

            CoordinatorStartupRecovery.RecoveryResult recovered =
                    CoordinatorStartupRecovery.recoverPersistedJobs(reopened, 300L);
            assertTrue(recovered.successful());
            TaskUnit<?> task = recovered.resumedJobs().getFirst().getTasks().get(taskId);
            assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
            assertEquals(7, task.getAttemptNumber());
            assertEquals(1, task.getRetryCount());
            assertTrue(task.getAssignmentIdentity().isEmpty());
            assertEquals("", task.getLeaseOwnerId());
            assertEquals(0L, task.getLeaseExpiresAtMillis());

            assertTrue(task.markAssigned("peer-new", 400L, "COORDINATOR_new", 900L));
            replacement = task.getAssignmentIdentity().orElseThrow();
            assertEquals(8, replacement.attemptNumber());
            assertTrue(reopened.markTaskAssigned(
                    taskId,
                    replacement.workerId(),
                    400L,
                    "COORDINATOR_new",
                    replacement.leaseExpiresAtEpochMillis(),
                    replacement.attemptNumber(),
                    replacement.assignmentId()
            ));
        }

        try (DatabaseManager reopenedAgain = new DatabaseManager(dbPath.toString())) {
            CoordinatorStartupRecovery.RecoveryResult recoveredAgain =
                    CoordinatorStartupRecovery.recoverPersistedJobs(reopenedAgain, 500L);
            assertTrue(recoveredAgain.successful());
            TaskUnit<?> task = recoveredAgain.resumedJobs().getFirst().getTasks().get(taskId);
            assertEquals(8, task.getAttemptNumber());
            assertEquals(replacement, task.getAssignmentIdentity().orElseThrow());
        }
    }

    @Test
    void restoresCompletedTaskResultsForRunningJobs() {
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-completed-before-final-result",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                TOKEN_HASH,
                "",
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        "task-job-completed-before-final-result-0",
                        "COMPLETED",
                        "payload",
                        "result",
                        0
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 456L);

        assertTrue(result.successful());
        EmbarrassinglyParallelJob<?, ?> job = result.resumedJobs().getFirst();
        assertTrue(job.isJobComplete());
        assertEquals(List.of("result"), job.aggregateAndSendResult());
        assertEquals(0, store.failedJobs().size());
    }

    @Test
    void marksRunningJobsFailedWhenPayloadsCannotBeRestored() {
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-missing-payload",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                TOKEN_HASH,
                "",
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        "task-job-missing-payload-0",
                        "PENDING",
                        null,
                        null,
                        0
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 789L);

        assertTrue(result.successful());
        assertEquals(0, result.resumedJobs().size());
        assertEquals(1, result.failedJobs());
        assertEquals(List.of("job-missing-payload:789"), store.failedJobs());
    }

    @Test
    void marksRunningJobsFailedWhenRequesterTokenHashIsMissing() {
        ResumeStore store = new ResumeStore(List.of(new JobStateStore.ResumableJobState(
                "job-missing-token",
                "RABBITMQ_TEST_TASK",
                "requester-1",
                "",
                "",
                "",
                List.of(new JobStateStore.ResumableTaskState(
                        "task-job-missing-token-0",
                        "PENDING",
                        "payload",
                        null,
                        0
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 987L);

        assertTrue(result.successful());
        assertEquals(0, result.resumedJobs().size());
        assertTrue(result.requesterTokenHashes().isEmpty());
        assertTrue(result.requesterIdentityKeys().isEmpty());
        assertEquals(1, result.failedJobs());
        assertEquals(List.of("job-missing-token:987"), store.failedJobs());
    }

    private static class RecordingStore implements JobStateStore {
        private final int reconciliationResult;
        private long completedAt;
        private int reconciliationCalls;

        private RecordingStore(int reconciliationResult) {
            this.reconciliationResult = reconciliationResult;
        }

        @Override
        public boolean insertJobWithTasks(String jobId,
                                          String taskType,
                                          String requesterId,
                                          int fileCount,
                                          Collection<String> taskIds) {
            return false;
        }

        @Override
        public boolean insertJob(String jobId, String taskType, String requesterId, int fileCount) {
            return false;
        }

        @Override
        public boolean insertTask(String taskId, String jobId) {
            return false;
        }

        @Override
        public boolean markTaskAssigned(String taskId, String peerId, long startedAt) {
            return false;
        }

        @Override
        public boolean markTaskCompleted(String taskId, long completedAt, long durationMs) {
            return false;
        }

        @Override
        public boolean markTaskRetried(String taskId, int retryCount) {
            return false;
        }

        @Override
        public boolean markTaskFailed(String taskId) {
            return false;
        }

        @Override
        public boolean markJobCompleted(String jobId) {
            return false;
        }

        @Override
        public boolean markJobFailed(String jobId) {
            return false;
        }

        @Override
        public int markRunningJobsFailedOnStartup(long completedAt) {
            this.completedAt = completedAt;
            reconciliationCalls++;
            return reconciliationResult;
        }

        private long completedAt() {
            return completedAt;
        }

        private int reconciliationCalls() {
            return reconciliationCalls;
        }
    }

    private static void downgradeAssignmentIdentitySchemaToVersion9(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE tasks DROP COLUMN assignment_id");
            stmt.execute("ALTER TABLE tasks DROP COLUMN attempt_number");
            stmt.execute("ALTER TABLE task_attempts DROP COLUMN assignment_id");
            stmt.execute("ALTER TABLE task_attempts DROP COLUMN lease_expires_at");
            stmt.execute("UPDATE schema_version SET version=9, applied_at=100 WHERE id=1");
        }
    }

    private static class ResumeStore implements JobStateStore {
        private final List<ResumableJobState> runningJobs;
        private final List<TaskAttemptRecord> taskAttempts;
        private final List<String> resetTasks = new ArrayList<>();
        private final List<String> releasedLeases = new ArrayList<>();
        private final List<String> failedJobs = new ArrayList<>();

        private ResumeStore(List<ResumableJobState> runningJobs) {
            this(runningJobs, List.of());
        }

        private ResumeStore(List<ResumableJobState> runningJobs,
                            List<TaskAttemptRecord> taskAttempts) {
            this.runningJobs = runningJobs;
            this.taskAttempts = taskAttempts;
        }

        @Override
        public boolean insertJobWithTasks(String jobId,
                                          String taskType,
                                          String requesterId,
                                          int fileCount,
                                          Collection<String> taskIds) {
            return false;
        }

        @Override
        public boolean insertJob(String jobId, String taskType, String requesterId, int fileCount) {
            return false;
        }

        @Override
        public boolean insertTask(String taskId, String jobId) {
            return false;
        }

        @Override
        public boolean markTaskAssigned(String taskId, String peerId, long startedAt) {
            return false;
        }

        @Override
        public boolean markTaskCompleted(String taskId, long completedAt, long durationMs) {
            return false;
        }

        @Override
        public boolean markTaskRetried(String taskId, int retryCount) {
            return false;
        }

        @Override
        public boolean markTaskFailed(String taskId) {
            return false;
        }

        @Override
        public boolean markJobCompleted(String jobId) {
            return false;
        }

        @Override
        public boolean markJobFailed(String jobId) {
            return false;
        }

        @Override
        public int markRunningJobsFailedOnStartup(long completedAt) {
            return 0;
        }

        @Override
        public List<ResumableJobState> loadRunningJobsForResume() {
            return runningJobs;
        }

        @Override
        public List<TaskAttemptRecord> loadTaskAttempts(String jobId) {
            return taskAttempts.stream()
                    .filter(attempt -> jobId.equals(attempt.jobId()))
                    .toList();
        }

        @Override
        public boolean resetTaskForResume(String taskId) {
            resetTasks.add(taskId);
            return true;
        }

        @Override
        public boolean releaseExpiredTaskLeaseForResume(String taskId, long releasedAt) {
            releasedLeases.add(taskId + ":" + releasedAt);
            return true;
        }

        @Override
        public boolean markRunningJobFailedOnStartup(String jobId, long completedAt) {
            failedJobs.add(jobId + ":" + completedAt);
            return true;
        }

        private List<String> resetTasks() {
            return List.copyOf(resetTasks);
        }

        private List<String> releasedLeases() {
            return List.copyOf(releasedLeases);
        }

        private List<String> failedJobs() {
            return List.copyOf(failedJobs);
        }
    }

    private record FixedClock(long epochMillis) implements TaskFlowClock {
        @Override
        public Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public long nowEpochMillis() {
            return epochMillis;
        }
    }
}
