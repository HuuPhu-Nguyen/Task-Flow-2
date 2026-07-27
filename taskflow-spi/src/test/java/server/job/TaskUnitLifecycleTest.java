package server.job;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskUnitLifecycleTest {

    private static class DummyTask extends TaskUnit<String> {
        DummyTask(String taskId, String jobId, String payload) {
            super(taskId, jobId, payload);
        }
    }

    @Test
    void acceptsCompletionOnlyFromAssignedPeer() {
        DummyTask task = new DummyTask("t-1", "job-1", "payload");
        long startAt = System.currentTimeMillis() - 25;
        assertTrue(task.markAssigned("peer-a", startAt));

        assertEquals(-1L, task.markCompletedBy("peer-b"));
        assertEquals(TaskUnit.TaskStatus.ASSIGNED, task.getStatus());

        long duration = task.markCompletedBy("peer-a");
        assertTrue(duration >= 0L);
        assertEquals(TaskUnit.TaskStatus.COMPLETED, task.getStatus());
    }

    @Test
    void exactCompletionRequiresAttemptAssignmentAndPeerWithoutPartialMutation() {
        DummyTask task = new DummyTask("t-fenced", "job-1", "payload");
        AssignmentIdentity identity = new AssignmentIdentity(
                "t-fenced",
                3,
                "550e8400-e29b-41d4-a716-446655440000",
                "peer-a",
                500L
        );
        task.restoreAssignedForResume(identity, 100L, "coordinator-1", 1);

        assertEquals(-1L, task.markCompletedBy(
                "peer-a", 2, identity.assignmentId(), 250L));
        assertEquals(-1L, task.markCompletedBy(
                "peer-a", 3, "550e8400-e29b-41d4-a716-446655440001", 250L));
        assertEquals(-1L, task.markCompletedBy(
                "peer-b", 3, identity.assignmentId(), 250L));
        assertEquals(TaskUnit.TaskStatus.ASSIGNED, task.getStatus());
        assertEquals(identity, task.getAssignmentIdentity().orElseThrow());
        assertEquals("peer-a", task.getAssignedPeerId());
        assertEquals(500L, task.getLeaseExpiresAtMillis());

        assertEquals(150L, task.markCompletedBy(
                "peer-a", 3, identity.assignmentId(), 250L));
        assertEquals(TaskUnit.TaskStatus.COMPLETED, task.getStatus());
        assertTrue(task.getAssignmentIdentity().isEmpty());
    }

    @Test
    void transitionsToTerminalFailureAfterRetryLimit() {
        DummyTask task = new DummyTask("t-2", "job-1", "payload");

        for (int i = 1; i < 20; i++) {
            assertTrue(task.markAssigned("peer-a", System.currentTimeMillis() - 5));
            TaskUnit.FailureOutcome outcome = task.failAttemptBy("peer-a", 20);
            assertEquals(TaskUnit.FailureOutcome.RETRY_SCHEDULED, outcome);
            assertEquals(i, task.getRetryCount());
            assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
        }

        assertTrue(task.markAssigned("peer-a", System.currentTimeMillis() - 5));
        TaskUnit.FailureOutcome terminalOutcome = task.failAttemptBy("peer-a", 20);
        assertEquals(TaskUnit.FailureOutcome.TERMINAL_FAILURE, terminalOutcome);
        assertEquals(20, task.getRetryCount());
        assertEquals(TaskUnit.TaskStatus.FAILED, task.getStatus());
    }

    @Test
    void permanentFailureTerminalizesTheFirstAttemptWithoutRetry() {
        DummyTask task = new DummyTask("t-permanent", "job-1", "payload");
        assertTrue(task.markAssigned("peer-a", System.currentTimeMillis() - 5));

        TaskUnit.FailureOutcome outcome = task.failAttemptBy("peer-a", 20, false);

        assertEquals(TaskUnit.FailureOutcome.TERMINAL_FAILURE, outcome);
        assertEquals(1, task.getRetryCount());
        assertEquals(TaskUnit.TaskStatus.FAILED, task.getStatus());
    }

    @Test
    void failureCreatesNextAssignmentGenerationIndependentlyFromRetryCount() {
        DummyTask task = new DummyTask("t-generation", "job-1", "payload");

        assertTrue(task.markAssigned("peer-a", 100L, "coordinator-1", 500L));
        AssignmentIdentity first = task.getAssignmentIdentity().orElseThrow();
        assertEquals(1, first.attemptNumber());
        assertEquals(0, task.getRetryCount());

        assertEquals(TaskUnit.FailureOutcome.RETRY_SCHEDULED, task.failAttemptBy("peer-a", 3));
        assertEquals(1, task.getAttemptNumber());
        assertEquals(1, task.getRetryCount());

        assertTrue(task.markAssigned("peer-a", 200L, "coordinator-1", 600L));
        AssignmentIdentity second = task.getAssignmentIdentity().orElseThrow();
        assertEquals(2, second.attemptNumber());
        assertNotEquals(first.assignmentId(), second.assignmentId());
        assertEquals(1, task.getRetryCount());
    }

    @Test
    void leaseExpiryReleaseCreatesNextGenerationWithoutInventingRetry() {
        DummyTask task = new DummyTask("t-lease", "job-1", "payload");

        assertTrue(task.markAssigned("peer-a", 100L, "coordinator-1", 200L));
        assertTrue(task.isLeaseExpired(200L));
        AssignmentIdentity expired = task.getAssignmentIdentity().orElseThrow();

        task.resetToPending();

        assertEquals(0, task.getRetryCount());
        assertEquals(1, task.getAttemptNumber());
        assertTrue(task.markAssigned("peer-b", 300L, "coordinator-2", 500L));
        AssignmentIdentity replacement = task.getAssignmentIdentity().orElseThrow();
        assertEquals(2, replacement.attemptNumber());
        assertNotEquals(expired.assignmentId(), replacement.assignmentId());
        assertEquals(0, task.getRetryCount());
    }

    @Test
    void dispatchReplayReusesExactCurrentAssignmentIdentity() {
        DummyTask task = new DummyTask("t-replay", "job-1", "payload");

        assertTrue(task.markAssigned("peer-a", 100L, "coordinator-1", 500L));
        AssignmentIdentity original = task.getAssignmentIdentity().orElseThrow();

        assertFalse(task.markAssigned("peer-a", 200L, "coordinator-1", 600L));
        assertEquals(original, task.getAssignmentIdentity().orElseThrow());
        assertEquals(1, task.getAttemptNumber());
        assertEquals(0, task.getRetryCount());
    }

    @Test
    void installsExactIdentityCommittedByStateStore() {
        DummyTask task = new DummyTask("t-committed", "job-1", "payload");
        AssignmentIdentity committed = new AssignmentIdentity(
                "t-committed",
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                "peer-a",
                500L
        );

        assertTrue(task.markAssigned(committed, 100L, "coordinator-1"));

        assertEquals(committed, task.getAssignmentIdentity().orElseThrow());
        assertEquals("peer-a", task.getAssignedPeerId());
        assertEquals(100L, task.getStartTime());
        assertEquals("coordinator-1", task.getLeaseOwnerId());
        assertEquals(500L, task.getLeaseExpiresAtMillis());
    }

    @Test
    void rejectsNonNextCommittedGenerationWithoutMutation() {
        DummyTask task = new DummyTask("t-invalid-committed", "job-1", "payload");
        AssignmentIdentity skipped = new AssignmentIdentity(
                "t-invalid-committed",
                2,
                "550e8400-e29b-41d4-a716-446655440000",
                "peer-a",
                500L
        );

        assertThrows(IllegalArgumentException.class,
                () -> task.markAssigned(skipped, 100L, "coordinator-1"));

        assertEquals(TaskUnit.TaskStatus.PENDING, task.getStatus());
        assertEquals(0, task.getAttemptNumber());
        assertTrue(task.getAssignmentIdentity().isEmpty());
        assertEquals(0L, task.getStartTime());
    }

    @Test
    void restoredGenerationContinuesMonotonicallyAfterRecovery() {
        DummyTask task = new DummyTask("t-recovery", "job-1", "payload");

        task.restorePendingForResume(2, 7);

        assertEquals(7, task.getAttemptNumber());
        assertEquals(2, task.getRetryCount());
        assertTrue(task.markAssigned("peer-a", 100L, "coordinator-2", 500L));
        assertEquals(8, task.getAssignmentIdentity().orElseThrow().attemptNumber());
        assertEquals(2, task.getRetryCount());
        assertThrows(IllegalArgumentException.class, () -> task.restorePendingForResume(2, 6));
        assertEquals(TaskUnit.TaskStatus.ASSIGNED, task.getStatus());
        assertEquals(8, task.getAttemptNumber());
        assertEquals(2, task.getRetryCount());
        assertEquals("peer-a", task.getAssignmentIdentity().orElseThrow().workerId());
    }

    @Test
    void assignedRecoveryPreservesExactIdentityThenContinuesWithNextGeneration() {
        DummyTask task = new DummyTask("t-assigned-recovery", "job-1", "payload");
        AssignmentIdentity restored = new AssignmentIdentity(
                "t-assigned-recovery",
                4,
                "550e8400-e29b-41d4-a716-446655440000",
                "peer-a",
                500L
        );

        task.restoreAssignedForResume(restored, 100L, "coordinator-old", 1);

        assertEquals(restored, task.getAssignmentIdentity().orElseThrow());
        assertEquals(4, task.getAttemptNumber());
        assertEquals(1, task.getRetryCount());

        task.resetToPending();
        assertTrue(task.markAssigned("peer-b", 600L, "coordinator-new", 900L));
        assertEquals(5, task.getAssignmentIdentity().orElseThrow().attemptNumber());
        assertEquals(1, task.getRetryCount());
    }

    @Test
    void committedJobFailureProjectionReleasesAssignmentExactlyOnce() {
        DummyTask task = new DummyTask("t-job-failure", "job-1", "payload");
        assertTrue(task.markAssigned("peer-a", 100L, "coordinator-1", 500L));

        assertEquals("peer-a", task.projectCommittedJobFailure().orElseThrow());
        assertEquals(TaskUnit.TaskStatus.FAILED, task.getStatus());
        assertEquals(0, task.getRetryCount());
        assertTrue(task.getAssignmentIdentity().isEmpty());
        assertEquals(0L, task.getStartTime());
        assertEquals("", task.getLeaseOwnerId());
        assertEquals(0L, task.getLeaseExpiresAtMillis());

        assertTrue(task.projectCommittedJobFailure().isEmpty());
        assertEquals(TaskUnit.TaskStatus.FAILED, task.getStatus());
    }
}
