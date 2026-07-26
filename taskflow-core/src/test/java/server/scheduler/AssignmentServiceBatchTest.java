package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TaskStateMachine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssignmentServiceBatchTest {

    @Test
    void dispatchAttemptsStopAtConfiguredCycleBudget() {
        SchedulerConfig config = SchedulerConfig.defaults();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register(
                "peer-1",
                new PeerInfo("peer-1", config, List.of(TestTaskPlugin.TASK_TYPE))
        );
        Fixture fixture = new Fixture(config, registry);
        fixture.addJob("job-bounded", 5);

        SchedulerLoop.StageResult first = fixture.assignments.dispatchPendingTasks(2);

        assertEquals(2, first.processed());
        assertTrue(first.immediateWorkRemaining());
        assertEquals(2, fixture.output.assignments.size());
        assertEquals(2, registry.get("peer-1").getActiveTasks());

        SchedulerLoop.StageResult second = fixture.assignments.dispatchPendingTasks(1);

        assertEquals(1, second.processed());
        assertEquals(3, fixture.output.assignments.size());
        assertEquals(3, registry.get("peer-1").getActiveTasks());
        assertTrue(second.immediateWorkRemaining());

        SchedulerLoop.StageResult capacityExhausted = fixture.assignments.dispatchPendingTasks(1);
        assertEquals(1, capacityExhausted.processed());
        assertFalse(capacityExhausted.immediateWorkRemaining());
        assertEquals(3, fixture.output.assignments.size());
    }

    @Test
    void noCapacitySweepPersistsAcrossBatchesAndDoesNotSpin() {
        Fixture fixture = new Fixture(SchedulerConfig.defaults(), new InMemoryPeerRegistry());
        for (int index = 0; index < 5; index++) {
            fixture.addJob("job-no-capacity-" + index, 1);
        }

        SchedulerLoop.StageResult first = fixture.assignments.dispatchPendingTasks(2);
        SchedulerLoop.StageResult second = fixture.assignments.dispatchPendingTasks(2);
        SchedulerLoop.StageResult third = fixture.assignments.dispatchPendingTasks(2);
        SchedulerLoop.StageResult stopped = fixture.assignments.dispatchPendingTasks(2);

        assertEquals(2, first.processed());
        assertTrue(first.immediateWorkRemaining());
        assertEquals(2, second.processed());
        assertTrue(second.immediateWorkRemaining());
        assertEquals(1, third.processed());
        assertFalse(third.immediateWorkRemaining());
        assertEquals(0, stopped.processed());
        assertFalse(stopped.immediateWorkRemaining());
        assertEquals(500L, fixture.assignments.millisUntilNextDispatchRecheck());

        fixture.assignments.signalSchedulingStateMayHaveChanged();
        SchedulerLoop.StageResult rechecked = fixture.assignments.dispatchPendingTasks(2);
        assertEquals(2, rechecked.processed());
        assertTrue(rechecked.immediateWorkRemaining());
    }

    private static final class Fixture {
        private final SchedulerState state;
        private final CapturingOutput output = new CapturingOutput();
        private final AssignmentService assignments;

        private Fixture(SchedulerConfig config, InMemoryPeerRegistry registry) {
            MutableClock clock = new MutableClock(1_000L);
            SchedulerEventLog events = new SchedulerEventLog();
            SchedulerPersistence persistence = new SchedulerPersistence(null, events);
            SchedulerMetrics metrics = new SchedulerMetrics();
            SchedulerOutboxService outbox = new SchedulerOutboxService(
                    persistence,
                    output,
                    clock,
                    events
            );
            state = new SchedulerState(config);
            JobCompletionService completions = new JobCompletionService(
                    state,
                    registry,
                    persistence,
                    output,
                    config,
                    clock,
                    metrics,
                    outbox,
                    events
            );
            AtomicInteger ids = new AtomicInteger();
            AssignmentIdGenerator idGenerator = () -> String.format(
                    "00000000-0000-0000-0000-%012d",
                    ids.incrementAndGet()
            );
            assignments = new AssignmentService(
                    state,
                    registry,
                    persistence,
                    output,
                    config,
                    "coordinator-1",
                    clock,
                    idGenerator,
                    metrics,
                    outbox,
                    new TaskTransitionDecisions(new TaskStateMachine()),
                    completions,
                    events
            );
        }

        private void addJob(String jobId, int taskCount) {
            List<Object> payloads = new ArrayList<>();
            for (int index = 0; index < taskCount; index++) {
                payloads.add("payload-" + index);
            }
            JobSubmitMessage message = new JobSubmitMessage(
                    "requester-1",
                    "2026-07-24T00:00:00Z",
                    jobId,
                    TestTaskPlugin.TASK_TYPE,
                    payloads,
                    "",
                    "token-" + jobId
            );
            EmbarrassinglyParallelJob<?, ?> job =
                    new TestTaskPlugin().createJob(message, "requester-1");
            job.initializeTasks(message);
            state.addActiveJob(job, "", "");
        }
    }

    private static final class CapturingOutput implements SchedulerOutput {
        private final List<TaskAssignMessage> assignments = new ArrayList<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            assignments.add(message);
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            return false;
        }
    }

    private static final class MutableClock implements TaskFlowClock {
        private final long nowMillis;

        private MutableClock(long nowMillis) {
            this.nowMillis = nowMillis;
        }

        @Override
        public Instant now() {
            return Instant.ofEpochMilli(nowMillis);
        }

        @Override
        public long nowEpochMillis() {
            return nowMillis;
        }
    }
}
