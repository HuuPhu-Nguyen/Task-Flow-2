package server.job;

import org.junit.jupiter.api.Test;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskUnitDeterministicPortsTest {
    private static final String ASSIGNMENT_ONE = "00000000-0000-0000-0000-000000000101";
    private static final String ASSIGNMENT_TWO = "00000000-0000-0000-0000-000000000102";

    @Test
    void injectedClockAndIdsDriveEveryTaskTransitionExactly() {
        MutableClock clock = new MutableClock(1_000L);
        DeterministicIds ids = new DeterministicIds(ASSIGNMENT_ONE, ASSIGNMENT_TWO);
        DummyTask task = new DummyTask(clock, ids);

        assertEquals(1_000L, task.getPendingSinceMillis());
        assertTrue(task.markAssigned("peer-a", 1_000L, "coordinator-test", 1_500L));
        assertEquals(ASSIGNMENT_ONE, task.getAssignmentIdentity().orElseThrow().assignmentId());

        clock.setEpochMillis(1_250L);
        assertEquals(
                TaskUnit.FailureOutcome.RETRY_SCHEDULED,
                task.failAttemptBy("peer-a", 2)
        );
        assertEquals(1_250L, task.getPendingSinceMillis());

        assertTrue(task.markAssigned("peer-b", 1_250L, "coordinator-test", 1_750L));
        assertEquals(ASSIGNMENT_TWO, task.getAssignmentIdentity().orElseThrow().assignmentId());

        clock.setEpochMillis(1_400L);
        assertEquals(150L, task.markCompletedBy("peer-b"));
        assertEquals(TaskUnit.TaskStatus.COMPLETED, task.getStatus());
    }

    @Test
    void pluginTaskCanBeReboundBeforeItsFirstSchedulerTransition() {
        MutableClock clock = new MutableClock(9_000L);
        DummyTask task = new DummyTask();

        task.configureTransitionPorts(clock, () -> ASSIGNMENT_ONE);

        assertEquals(9_000L, task.getPendingSinceMillis());
        assertTrue(task.markAssigned("peer-a", 9_000L));
        assertEquals(ASSIGNMENT_ONE, task.getAssignmentIdentity().orElseThrow().assignmentId());
    }

    private static final class DummyTask extends TaskUnit<String> {
        private DummyTask() {
            super("task-1", "job-1", "payload");
        }

        private DummyTask(TaskFlowClock clock, AssignmentIdGenerator assignmentIdGenerator) {
            super("task-1", "job-1", "payload", clock, assignmentIdGenerator);
        }
    }

    private static final class MutableClock implements TaskFlowClock {
        private long epochMillis;

        private MutableClock(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        private void setEpochMillis(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        @Override
        public Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public long nowEpochMillis() {
            return epochMillis;
        }
    }

    private static final class DeterministicIds implements AssignmentIdGenerator {
        private final Deque<String> ids;

        private DeterministicIds(String... ids) {
            this.ids = new ArrayDeque<>(Arrays.asList(ids));
        }

        @Override
        public String nextAssignmentId() {
            if (ids.isEmpty()) {
                throw new IllegalStateException("No deterministic assignment ID remains.");
            }
            return ids.removeFirst();
        }
    }
}
