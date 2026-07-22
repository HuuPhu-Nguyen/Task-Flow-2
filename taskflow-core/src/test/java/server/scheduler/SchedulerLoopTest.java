package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.PongMessage;
import server.model.MessageEnvelope;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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

    private static final class RecordingWork implements SchedulerLoop.Work {
        private final List<String> calls = new ArrayList<>();
        private MessageEnvelope envelope;

        @Override
        public void processEnvelope(MessageEnvelope envelope) {
            this.envelope = envelope;
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
