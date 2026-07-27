package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TaskStateMachine;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseServiceBatchTest {

    @Test
    void combinedTimeoutAndLeaseWorkStopsAtDeadlineBudget() {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        SchedulerConfig config = new SchedulerConfig(
                1L,
                1_000L,
                defaults.maxTaskRetries(),
                defaults.inboundQueueCapacity(),
                defaults.jobResultMaxDeliveryAttempts(),
                defaults.metricsLogIntervalMillis(),
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );
        FixedClock clock = new FixedClock(100L);
        SchedulerState state = new SchedulerState(config);
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo("peer-1", config, List.of(TestTaskPlugin.TASK_TYPE));
        registry.register(peer.getNodeId(), peer);
        for (int index = 0; index < 3; index++) {
            EmbarrassinglyParallelJob<?, ?> job = oneTaskJob("job-deadline-" + index);
            TaskUnit<?> task = job.getTasks().values().iterator().next();
            AssignmentIdentity identity = new AssignmentIdentity(
                    task.getTaskId(),
                    1,
                    new UUID(0L, index + 1L).toString(),
                    peer.getNodeId(),
                    1_000L
            );
            task.markAssigned(identity, 1L, "coordinator-1");
            state.addActiveJob(job, "", "");
            registry.reserveTaskCapacity(
                    CapacityReservations.forAssignment(job, task, identity)
            );
        }
        SchedulerEventLog events = new SchedulerEventLog(clock, "COORDINATOR_TEST");
        SchedulerPersistence persistence = new SchedulerPersistence(null, events);
        SchedulerMetrics metrics = new SchedulerMetrics();
        RejectingOutput output = new RejectingOutput();
        JobCompletionService completions = new JobCompletionService(
                state,
                registry,
                persistence,
                output,
                config,
                clock,
                metrics,
                new SchedulerOutboxService(persistence, output, clock, events),
                events
        );
        LeaseService leases = new LeaseService(
                state,
                config,
                clock,
                new TaskTransitionDecisions(new TaskStateMachine()),
                new AttemptService(state, registry, persistence, metrics),
                completions,
                persistence,
                events
        );

        SchedulerLoop.StageResult first = leases.processDueDeadlines(2);

        assertEquals(2, first.processed());
        assertTrue(first.immediateWorkRemaining());
        assertEquals(1, peer.getActiveTasks());

        SchedulerLoop.StageResult second = leases.processDueDeadlines(2);

        assertEquals(1, second.processed());
        assertFalse(second.immediateWorkRemaining());
        assertEquals(0, peer.getActiveTasks());
    }

    private static EmbarrassinglyParallelJob<?, ?> oneTaskJob(String jobId) {
        JobSubmitMessage message = new JobSubmitMessage(
                "requester-1",
                "2026-07-24T00:00:00Z",
                jobId,
                TestTaskPlugin.TASK_TYPE,
                List.of("payload"),
                "",
                "token-" + jobId
        );
        EmbarrassinglyParallelJob<?, ?> job =
                new TestTaskPlugin().createJob(message, "requester-1");
        job.initializeTasks(message);
        return job;
    }

    private static final class RejectingOutput implements SchedulerOutput {
        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            return false;
        }
    }

    private record FixedClock(long nowMillis) implements TaskFlowClock {
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
