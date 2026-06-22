package server;

import org.junit.jupiter.api.Test;
import server.db.JobStateStore;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorStartupRecoveryTest {

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
}
