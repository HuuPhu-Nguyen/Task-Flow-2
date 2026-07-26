package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.TaskFlowClock;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobCompletionServiceBatchTest {

    @Test
    void dueTerminalDeliveryRetriesUseBoundedBatchAndExactNextWake() {
        SchedulerConfig config = SchedulerConfig.defaults();
        MutableClock clock = new MutableClock(1_000L);
        RejectingOutput output = new RejectingOutput();
        SchedulerState state = new SchedulerState(config);
        SchedulerEventLog events = new SchedulerEventLog();
        SchedulerPersistence persistence = new SchedulerPersistence(null, events);
        JobCompletionService completions = new JobCompletionService(
                state,
                new InMemoryPeerRegistry(),
                persistence,
                output,
                config,
                clock,
                new SchedulerMetrics(),
                new SchedulerOutboxService(persistence, output, clock, events),
                events
        );
        for (int index = 0; index < 3; index++) {
            EmbarrassinglyParallelJob<?, ?> job = emptyJob("job-retry-" + index);
            state.addActiveJob(job, "", "");
            completions.completeJob(job, true, null);
        }
        assertEquals(3, output.attempts);
        assertEquals(1_000L, completions.millisUntilNextRetry());

        clock.advanceMillis(1_000L);
        SchedulerLoop.StageResult first = completions.retryPendingJobResults(2);

        assertEquals(2, first.processed());
        assertTrue(first.immediateWorkRemaining());
        assertEquals(5, output.attempts);
        assertEquals(0L, completions.millisUntilNextRetry());

        SchedulerLoop.StageResult second = completions.retryPendingJobResults(2);

        assertEquals(1, second.processed());
        assertFalse(second.immediateWorkRemaining());
        assertEquals(6, output.attempts);
        assertEquals(1_000L, completions.millisUntilNextRetry());
    }

    private static EmbarrassinglyParallelJob<?, ?> emptyJob(String jobId) {
        JobSubmitMessage message = new JobSubmitMessage(
                "requester-1",
                "2026-07-24T00:00:00Z",
                jobId,
                TestTaskPlugin.TASK_TYPE,
                List.of(),
                "",
                "token-" + jobId
        );
        return new TestTaskPlugin().createJob(message, "requester-1");
    }

    private static final class RejectingOutput implements SchedulerOutput {
        private int attempts;

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            attempts++;
            return false;
        }
    }

    private static final class MutableClock implements TaskFlowClock {
        private long nowMillis;

        private MutableClock(long nowMillis) {
            this.nowMillis = nowMillis;
        }

        private void advanceMillis(long millis) {
            nowMillis += millis;
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
