package server;

import org.junit.jupiter.api.Test;
import protocol.RequesterTokens;
import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorStartupRecoveryTest {
    private static final String TOKEN_HASH = RequesterTokens.hashToken("resume-token");
    private static final String IDENTITY_KEY = "resume-public-key";

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
                        "ASSIGNED",
                        "payload",
                        null,
                        2
                ))
        )));

        CoordinatorStartupRecovery.RecoveryResult result =
                CoordinatorStartupRecovery.recoverPersistedJobs(store, 123L);

        assertTrue(result.successful());
        assertEquals(1, result.resumedJobs().size());
        assertEquals(TOKEN_HASH, result.requesterTokenHashes().get("job-resume"));
        assertEquals(IDENTITY_KEY, result.requesterIdentityKeys().get("job-resume"));
        assertEquals(0, result.failedJobs());
        assertEquals(List.of("task-job-resume-0:123"), store.releasedLeases());

        EmbarrassinglyParallelJob<?, ?> job = result.resumedJobs().getFirst();
        TaskUnit<?> task = job.getTasks().get("task-job-resume-0");
        assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
        assertEquals(2, task.getRetryCount());
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
                        500L
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
                        200L
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

    private static class ResumeStore implements JobStateStore {
        private final List<ResumableJobState> runningJobs;
        private final List<String> resetTasks = new ArrayList<>();
        private final List<String> releasedLeases = new ArrayList<>();
        private final List<String> failedJobs = new ArrayList<>();

        private ResumeStore(List<ResumableJobState> runningJobs) {
            this.runningJobs = runningJobs;
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
}
