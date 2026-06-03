package server.job;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
