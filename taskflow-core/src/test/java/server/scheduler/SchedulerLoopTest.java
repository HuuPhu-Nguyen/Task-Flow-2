package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.PongMessage;
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
    void oneCycleDelegatesEnvelopeAndMaintenanceInStableOrder() throws Exception {
        LinkedBlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MessageEnvelope envelope = new MessageEnvelope(
                new PongMessage("peer-1", "2026-07-22T00:00:00Z", List.of("TEST_TASK")),
                "peer-1"
        );
        mailbox.add(envelope);
        RecordingWork work = new RecordingWork();
        SchedulerLoop loop = new SchedulerLoop(mailbox, work);

        loop.runCycle(0L);

        assertSame(envelope, work.envelope);
        assertEquals(List.of(
                "message",
                "timeouts",
                "leases",
                "dispatch",
                "results",
                "metrics"
        ), work.calls);
    }

    @Test
    void emptyCycleStillRunsEveryBoundedMaintenanceStage() throws Exception {
        RecordingWork work = new RecordingWork();
        SchedulerLoop loop = new SchedulerLoop(new LinkedBlockingQueue<>(), work);

        loop.runCycle(0L);

        assertEquals(List.of("timeouts", "leases", "dispatch", "results", "metrics"), work.calls);
    }

    @Test
    void shutdownAfterDrainProcessesEveryAlreadyAcceptedEnvelopeThenStops() throws Exception {
        LinkedBlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MessageEnvelope first = new MessageEnvelope(
                new PongMessage("peer-1", "2026-07-22T00:00:00Z", List.of("TEST_TASK")),
                "peer-1"
        );
        MessageEnvelope second = new MessageEnvelope(
                new PongMessage("peer-2", "2026-07-22T00:00:01Z", List.of("TEST_TASK")),
                "peer-2"
        );
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

    private static final class RecordingWork implements SchedulerLoop.Work {
        private final List<String> calls = new ArrayList<>();
        private final List<MessageEnvelope> envelopes = new ArrayList<>();
        private final CountDownLatch processed;
        private MessageEnvelope envelope;

        private RecordingWork() {
            this(0);
        }

        private RecordingWork(int expectedMessages) {
            this.processed = new CountDownLatch(expectedMessages);
        }

        @Override
        public void processEnvelope(MessageEnvelope envelope) {
            this.envelope = envelope;
            envelopes.add(envelope);
            processed.countDown();
            calls.add("message");
        }

        @Override
        public void checkTimeouts() {
            calls.add("timeouts");
        }

        @Override
        public void checkLeaseExpirations() {
            calls.add("leases");
        }

        @Override
        public void dispatchPendingTasks() {
            calls.add("dispatch");
        }

        @Override
        public void retryPendingJobResults() {
            calls.add("results");
        }

        @Override
        public void updateMetrics() {
            calls.add("metrics");
        }
    }
}
