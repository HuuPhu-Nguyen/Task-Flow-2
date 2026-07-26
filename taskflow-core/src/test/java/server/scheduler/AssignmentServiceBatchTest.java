package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TaskStateMachine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssignmentServiceBatchTest {

    @Test
    void oneLargeJobAndTenSmallJobsAllDispatchInTheirFirstCompleteRound() {
        SchedulerConfig config = config(100, 1);
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register(
                "peer-1",
                new PeerInfo("peer-1", config, List.of(TestTaskPlugin.TASK_TYPE))
        );
        Fixture fixture = new Fixture(config, registry);
        fixture.addJob("job-large", 10_000);
        for (int index = 0; index < 10; index++) {
            fixture.addJob("job-small-" + index, 1);
        }

        SchedulerLoop.StageResult firstRound = fixture.assignments.dispatchPendingTasks(11);

        assertEquals(11, firstRound.processed());
        assertEquals(1L, fixture.assignments.completedRounds());
        assertEquals(
                List.of(
                        "job-large",
                        "job-small-0",
                        "job-small-1",
                        "job-small-2",
                        "job-small-3",
                        "job-small-4",
                        "job-small-5",
                        "job-small-6",
                        "job-small-7",
                        "job-small-8",
                        "job-small-9"
                ),
                fixture.output.assignments.stream().map(TaskAssignMessage::getJobId).toList()
        );
    }

    @Test
    void configuredQuotaPersistsOneRoundAcrossDispatchBatchBoundaries() {
        SchedulerConfig config = config(3, 2);
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register(
                "peer-1",
                new PeerInfo("peer-1", config, List.of(TestTaskPlugin.TASK_TYPE))
        );
        Fixture fixture = new Fixture(config, registry);
        fixture.addJob("job-a", 4);
        fixture.addJob("job-b", 4);
        fixture.addJob("job-c", 4);

        SchedulerLoop.StageResult firstBatch = fixture.assignments.dispatchPendingTasks(3);
        SchedulerLoop.StageResult secondBatch = fixture.assignments.dispatchPendingTasks(3);

        assertEquals(3, firstBatch.processed());
        assertTrue(firstBatch.immediateWorkRemaining());
        assertEquals(3, secondBatch.processed());
        assertEquals(1L, fixture.assignments.completedRounds());
        assertEquals(
                List.of("job-a", "job-a", "job-b", "job-c", "job-c", "job-a"),
                fixture.output.assignments.stream().map(TaskAssignMessage::getJobId).toList()
        );
    }

    @Test
    void retryPriorityRemainsInsideOneJobsQuota() {
        SchedulerConfig config = config(10, 2);
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register(
                "peer-1",
                new PeerInfo("peer-1", config, List.of(TestTaskPlugin.TASK_TYPE))
        );
        Fixture fixture = new Fixture(config, registry);
        EmbarrassinglyParallelJob<?, ?> job = fixture.createJob("job-retry-priority", 3);
        TaskUnit<?> retry = job.getTasks().get("task-job-retry-priority-2");
        retry.restorePendingForResume(1, 1);
        fixture.indexJob(job);

        fixture.assignments.dispatchPendingTasks(2);

        assertEquals(
                List.of("task-job-retry-priority-2", "task-job-retry-priority-0"),
                fixture.output.assignments.stream().map(TaskAssignMessage::getTaskId).toList()
        );
    }

    @Test
    void dispatchAttemptsStopAtConfiguredCycleBudget() {
        SchedulerConfig config = SchedulerConfig.defaults();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register(
                "peer-1",
                capacityPeer("peer-1", config, 3)
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

    private static PeerInfo capacityPeer(String peerId,
                                         SchedulerConfig config,
                                         int totalCapacityUnits) {
        PeerInfo peer = new PeerInfo(peerId, config);
        peer.applyCapacityHeartbeat(new PongMessage(
                peerId,
                "2026-07-26T00:00:00Z",
                List.of(TestTaskPlugin.TASK_TYPE),
                "550e8400-e29b-41d4-a716-446655440040",
                1L,
                totalCapacityUnits,
                totalCapacityUnits,
                Map.of(TestTaskPlugin.TASK_TYPE, totalCapacityUnits)
        ));
        return peer;
    }

    @Test
    void noCapacitySweepPersistsAcrossBatchesAndDoesNotSpin() {
        SchedulerConfig config = SchedulerConfig.defaults();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        Fixture fixture = new Fixture(config, registry);
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
        assertEquals(0, fixture.state.workloadSnapshot().runnableJobs());
        assertEquals(5, fixture.state.workloadSnapshot().capacityWaitingJobs());

        registry.register(
                "peer-1",
                new PeerInfo("peer-1", config, List.of(TestTaskPlugin.TASK_TYPE))
        );
        SchedulerLoop.StageResult capacitySignalled =
                fixture.assignments.dispatchPendingTasks(2);
        assertEquals(2, capacitySignalled.processed());
        assertTrue(capacitySignalled.immediateWorkRemaining());
        assertEquals(2, fixture.output.assignments.size());
        assertEquals(3, fixture.state.workloadSnapshot().capacityWaitingJobs());
    }

    @Test
    void timedRecheckRestoresWaitingJobsWithoutWallClockSleep() {
        Fixture fixture = new Fixture(SchedulerConfig.defaults(), new InMemoryPeerRegistry());
        EmbarrassinglyParallelJob<?, ?> job = fixture.addJob("job-recheck", 1);

        fixture.assignments.dispatchPendingTasks(1);
        TaskUnit<?> task = job.getTasks().values().iterator().next();
        fixture.state.indexPendingTask(task, true);

        assertEquals(0, fixture.state.workloadSnapshot().runnableJobs());
        assertEquals(1, fixture.state.workloadSnapshot().capacityWaitingJobs());

        fixture.clock.advanceMillis(500L);
        SchedulerLoop.StageResult rechecked = fixture.assignments.dispatchPendingTasks(1);

        assertEquals(1, rechecked.processed());
        assertFalse(rechecked.immediateWorkRemaining());
        assertEquals(1, fixture.state.workloadSnapshot().capacityWaitingJobs());
        assertEquals(500L, fixture.assignments.millisUntilNextDispatchRecheck());
    }

    private static final class Fixture {
        private final SchedulerState state;
        private final CapturingOutput output = new CapturingOutput();
        private final AssignmentService assignments;
        private final MutableClock clock;

        private Fixture(SchedulerConfig config, InMemoryPeerRegistry registry) {
            clock = new MutableClock(1_000L);
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

        private EmbarrassinglyParallelJob<?, ?> addJob(String jobId, int taskCount) {
            EmbarrassinglyParallelJob<?, ?> job = createJob(jobId, taskCount);
            indexJob(job);
            return job;
        }

        private EmbarrassinglyParallelJob<?, ?> createJob(String jobId, int taskCount) {
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
            return job;
        }

        private void indexJob(EmbarrassinglyParallelJob<?, ?> job) {
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
        private long nowMillis;

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

        private void advanceMillis(long millis) {
            nowMillis += millis;
        }
    }

    private static SchedulerConfig config(int dispatchBatchSize,
                                          int assignmentsPerJobPerRound) {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        return new SchedulerConfig(
                defaults.taskTimeoutMillis(),
                defaults.taskLeaseMillis(),
                defaults.maxTaskRetries(),
                defaults.inboundQueueCapacity(),
                defaults.jobResultMaxDeliveryAttempts(),
                defaults.schedulerMessageBatchSize(),
                defaults.schedulerDeadlineBatchSize(),
                dispatchBatchSize,
                assignmentsPerJobPerRound,
                defaults.schedulerOutboxBatchSize(),
                defaults.metricsLogIntervalMillis(),
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );
    }
}
