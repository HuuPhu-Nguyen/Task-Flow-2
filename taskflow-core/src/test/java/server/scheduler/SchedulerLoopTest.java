package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.PongMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerLoopTest {

    @Test
    void oneCycleAppliesExactStageLimitsInRequiredOrder() throws Exception {
        LinkedBlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MessageEnvelope first = pong("peer-1");
        MessageEnvelope second = pong("peer-2");
        MessageEnvelope third = pong("peer-3");
        mailbox.add(first);
        mailbox.add(second);
        mailbox.add(third);
        RecordingWork work = new RecordingWork();
        work.deadlineResult = new SchedulerLoop.StageResult(3, false);
        work.dispatchResult = new SchedulerLoop.StageResult(4, false);
        work.outboundResult = new SchedulerLoop.StageResult(5, false);
        SchedulerLoop loop = new SchedulerLoop(mailbox, work, config(2, 3, 4, 5));

        SchedulerLoop.CycleResult result = loop.runCycle(0L);

        assertEquals(List.of(
                "message:peer-1",
                "message:peer-2",
                "deadlines:3",
                "dispatch:4",
                "outbound:5",
                "metrics"
        ), work.calls);
        assertEquals(2, result.messagesProcessed());
        assertEquals(3, result.deadlinesProcessed());
        assertEquals(4, result.dispatchAttempts());
        assertEquals(5, result.outboundAttempts());
        assertTrue(result.immediateWorkRemaining());
        assertSame(third, mailbox.peek());
    }

    @Test
    void emptyCycleStillRunsEveryBoundedMaintenanceStage() throws Exception {
        RecordingWork work = new RecordingWork();
        SchedulerLoop loop = new SchedulerLoop(new LinkedBlockingQueue<>(), work);

        SchedulerLoop.CycleResult result = loop.runCycle(0L);

        assertEquals(
                List.of("deadlines:100", "dispatch:100", "outbound:100", "metrics"),
                work.calls
        );
        assertEquals(0, result.messagesProcessed());
    }

    @Test
    void continuousMailboxBacklogStillProcessesDeadlinesEveryCycle() throws Exception {
        LinkedBlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        mailbox.add(pong("peer-1"));
        mailbox.add(pong("peer-2"));
        mailbox.add(pong("peer-3"));
        RecordingWork work = new RecordingWork();
        SchedulerLoop loop = new SchedulerLoop(mailbox, work, config(1, 1, 1, 1));

        loop.runCycle(0L);
        loop.runCycle(0L);
        loop.runCycle(0L);

        assertEquals(3, work.deadlineCalls);
        assertEquals(3, work.envelopes.size());
        assertTrue(mailbox.isEmpty());
    }

    @Test
    void continuousDueDeadlineBacklogCannotStarveQueuedTaskResult() throws Exception {
        LinkedBlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MessageEnvelope result = taskResult();
        mailbox.add(result);
        RecordingWork work = new RecordingWork();
        work.deadlineResult = new SchedulerLoop.StageResult(1, true);
        SchedulerLoop loop = new SchedulerLoop(mailbox, work, config(1, 1, 1, 1));

        SchedulerLoop.CycleResult cycle = loop.runCycle(0L);

        assertSame(result, work.envelopes.getFirst());
        assertEquals("message:worker-1", work.calls.getFirst());
        assertEquals("deadlines:1", work.calls.get(1));
        assertTrue(cycle.immediateWorkRemaining());
    }

    @Test
    void idleCycleWaitsForNearestSchedulerOwnedDueTime() throws Exception {
        RecordingWork work = new RecordingWork();
        work.nextScheduledWorkMillis = 37L;
        SchedulerLoop loop = new SchedulerLoop(new LinkedBlockingQueue<>(), work);

        SchedulerLoop.CycleResult cycle = loop.runCycle(0L);

        assertFalse(cycle.immediateWorkRemaining());
        assertEquals(37L, loop.nextPollTimeoutMillis(cycle));
    }

    @Test
    void shutdownAfterDrainProcessesEveryAlreadyAcceptedEnvelopeThenStops() throws Exception {
        LinkedBlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MessageEnvelope first = pong("peer-1");
        MessageEnvelope second = pong("peer-2");
        mailbox.add(first);
        mailbox.add(second);
        RecordingWork work = new RecordingWork(2);
        SchedulerLoop loop = new SchedulerLoop(mailbox, work);
        loop.requestShutdownAfterDrain();
        Thread thread = new Thread(loop, "scheduler-loop-drain-test");

        thread.start();

        assertTrue(work.processed.await(2, TimeUnit.SECONDS));
        thread.join(2_000L);
        assertFalse(thread.isAlive());
        assertEquals(List.of(first, second), work.envelopes);
        assertTrue(mailbox.isEmpty());
    }

    @Test
    void shutdownRequestWakesAnIdleBlockedLoop() throws Exception {
        RecordingWork work = new RecordingWork();
        work.nextScheduledWorkMillis = Long.MAX_VALUE;
        SchedulerLoop loop = new SchedulerLoop(new LinkedBlockingQueue<>(), work);
        Thread thread = new Thread(loop, "scheduler-loop-idle-shutdown-test");
        thread.start();
        assertTrue(work.firstMetricsUpdate.await(2, TimeUnit.SECONDS));

        loop.requestShutdownAfterDrain();
        thread.join(2_000L);

        assertFalse(thread.isAlive());
    }

    @Test
    void externalSchedulingSignalWakesIdleLoopWithoutStoppingIt() throws Exception {
        RecordingWork work = new RecordingWork();
        work.nextScheduledWorkMillis = Long.MAX_VALUE;
        SchedulerLoop loop = new SchedulerLoop(new LinkedBlockingQueue<>(), work);
        Thread thread = new Thread(loop, "scheduler-loop-external-capacity-wake-test");
        thread.start();
        assertTrue(work.firstMetricsUpdate.await(2, TimeUnit.SECONDS));

        loop.requestExternalWakeup();

        assertTrue(work.secondMetricsUpdate.await(2, TimeUnit.SECONDS));
        assertTrue(thread.isAlive());
        loop.requestShutdownAfterDrain();
        thread.join(2_000L);
        assertFalse(thread.isAlive());
    }

    private static MessageEnvelope pong(String peerId) {
        return new MessageEnvelope(
                new PongMessage(peerId, "2026-07-22T00:00:00Z", List.of("TEST_TASK")),
                peerId
        );
    }

    private static MessageEnvelope taskResult() {
        return new MessageEnvelope(
                new TaskResultMessage(
                        "worker-1",
                        "2026-07-22T00:00:00Z",
                        "task-1",
                        "job-1",
                        1,
                        "00000000-0000-0000-0000-000000000001",
                        "result",
                        true,
                        null
                ),
                "worker-1"
        );
    }

    private static SchedulerConfig config(int messages,
                                          int deadlines,
                                          int dispatch,
                                          int outbox) {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        return new SchedulerConfig(
                defaults.taskTimeoutMillis(),
                defaults.taskLeaseMillis(),
                defaults.maxTaskRetries(),
                defaults.inboundQueueCapacity(),
                defaults.jobResultMaxDeliveryAttempts(),
                messages,
                deadlines,
                dispatch,
                outbox,
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

    private static final class RecordingWork implements SchedulerLoop.Work {
        private final List<String> calls = new ArrayList<>();
        private final List<MessageEnvelope> envelopes = new ArrayList<>();
        private final CountDownLatch processed;
        private final CountDownLatch firstMetricsUpdate = new CountDownLatch(1);
        private final CountDownLatch secondMetricsUpdate = new CountDownLatch(1);
        private SchedulerLoop.StageResult deadlineResult = SchedulerLoop.StageResult.idle();
        private SchedulerLoop.StageResult dispatchResult = SchedulerLoop.StageResult.idle();
        private SchedulerLoop.StageResult outboundResult = SchedulerLoop.StageResult.idle();
        private long nextScheduledWorkMillis = 10_000L;
        private int deadlineCalls;
        private int metricsUpdates;

        private RecordingWork() {
            this(0);
        }

        private RecordingWork(int expectedMessages) {
            this.processed = new CountDownLatch(expectedMessages);
        }

        @Override
        public void processEnvelope(MessageEnvelope envelope) {
            envelopes.add(envelope);
            processed.countDown();
            calls.add("message:" + envelope.fromNodeId());
        }

        @Override
        public SchedulerLoop.StageResult processDueDeadlines(int limit) {
            deadlineCalls++;
            calls.add("deadlines:" + limit);
            return deadlineResult;
        }

        @Override
        public SchedulerLoop.StageResult dispatchPendingTasks(int limit) {
            calls.add("dispatch:" + limit);
            return dispatchResult;
        }

        @Override
        public SchedulerLoop.StageResult retryPendingOutbound(int limit) {
            calls.add("outbound:" + limit);
            return outboundResult;
        }

        @Override
        public void updateMetrics() {
            calls.add("metrics");
            metricsUpdates++;
            firstMetricsUpdate.countDown();
            if (metricsUpdates >= 2) {
                secondMetricsUpdate.countDown();
            }
        }

        @Override
        public long millisUntilNextScheduledWork() {
            return nextScheduledWorkMillis;
        }
    }
}
