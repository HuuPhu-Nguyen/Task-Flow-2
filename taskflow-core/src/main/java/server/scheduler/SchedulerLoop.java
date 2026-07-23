package server.scheduler;

import server.model.MessageEnvelope;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Mailbox/timer/dispatch orchestration only. All transition and infrastructure
 * effects are delegated through {@link Work}, which also makes a cycle
 * deterministic to test with in-memory fakes.
 */
public final class SchedulerLoop implements Runnable {
    static final long DEFAULT_POLL_TIMEOUT_MILLIS = 500L;

    private final BlockingQueue<MessageEnvelope> inboundMailbox;
    private final Work work;
    private volatile boolean shutdownAfterDrain;

    public SchedulerLoop(BlockingQueue<MessageEnvelope> inboundMailbox, Work work) {
        this.inboundMailbox = Objects.requireNonNull(inboundMailbox, "inboundMailbox");
        this.work = Objects.requireNonNull(work, "work");
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            if (shutdownAfterDrain && inboundMailbox.isEmpty()) {
                return;
            }
            try {
                runCycle(shutdownAfterDrain ? 0L : DEFAULT_POLL_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void requestShutdownAfterDrain() {
        shutdownAfterDrain = true;
    }

    void runCycle(long pollTimeoutMillis) throws InterruptedException {
        if (pollTimeoutMillis < 0L) {
            throw new IllegalArgumentException("pollTimeoutMillis must not be negative");
        }
        MessageEnvelope envelope = inboundMailbox.poll(pollTimeoutMillis, TimeUnit.MILLISECONDS);
        if (envelope != null) {
            work.processEnvelope(envelope);
        }
        work.checkTimeouts();
        work.checkLeaseExpirations();
        work.dispatchPendingTasks();
        work.retryPendingJobResults();
        work.updateMetrics();
    }

    /** Narrow orchestration seam implemented by focused services in production. */
    public interface Work {
        void processEnvelope(MessageEnvelope envelope);

        void checkTimeouts();

        void checkLeaseExpirations();

        void dispatchPendingTasks();

        void retryPendingJobResults();

        void updateMetrics();
    }
}
