package server.scheduler;

import server.model.MessageEnvelope;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Mailbox/deadline/dispatch/outbound orchestration only. All transition and
 * infrastructure effects are delegated through {@link Work}, which also makes
 * a bounded cycle deterministic to test with in-memory fakes.
 */
public final class SchedulerLoop implements Runnable {
    private final BlockingQueue<MessageEnvelope> inboundMailbox;
    private final Work work;
    private final int messageBatchSize;
    private final int deadlineBatchSize;
    private final int dispatchBatchSize;
    private final int outboxBatchSize;

    private volatile boolean shutdownAfterDrain;
    private volatile Thread runner;

    public SchedulerLoop(BlockingQueue<MessageEnvelope> inboundMailbox, Work work) {
        this(inboundMailbox, work, SchedulerConfig.defaults());
    }

    SchedulerLoop(BlockingQueue<MessageEnvelope> inboundMailbox,
                  Work work,
                  SchedulerConfig config) {
        this.inboundMailbox = Objects.requireNonNull(inboundMailbox, "inboundMailbox");
        this.work = Objects.requireNonNull(work, "work");
        SchedulerConfig effectiveConfig = config == null ? SchedulerConfig.defaults() : config;
        this.messageBatchSize = effectiveConfig.schedulerMessageBatchSize();
        this.deadlineBatchSize = effectiveConfig.schedulerDeadlineBatchSize();
        this.dispatchBatchSize = effectiveConfig.schedulerDispatchBatchSize();
        this.outboxBatchSize = effectiveConfig.schedulerOutboxBatchSize();
    }

    @Override
    public void run() {
        runner = Thread.currentThread();
        long pollTimeoutMillis = 0L;
        try {
            while (true) {
                if (shutdownAfterDrain && inboundMailbox.isEmpty()) {
                    return;
                }

                CycleResult result;
                try {
                    result = runCycle(shutdownAfterDrain ? 0L : pollTimeoutMillis);
                } catch (InterruptedException e) {
                    if (shutdownAfterDrain) {
                        continue;
                    }
                    Thread.currentThread().interrupt();
                    return;
                }

                if (Thread.interrupted()) {
                    if (!shutdownAfterDrain) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                pollTimeoutMillis = nextPollTimeoutMillis(result);
            }
        } finally {
            runner = null;
        }
    }

    void requestShutdownAfterDrain() {
        shutdownAfterDrain = true;
        Thread runningThread = runner;
        if (runningThread != null) {
            runningThread.interrupt();
        }
    }

    CycleResult runCycle(long pollTimeoutMillis) throws InterruptedException {
        if (pollTimeoutMillis < 0L) {
            throw new IllegalArgumentException("pollTimeoutMillis must not be negative");
        }

        int messagesProcessed = processMessages(pollTimeoutMillis);
        StageResult deadlines = work.processDueDeadlines(deadlineBatchSize);
        StageResult dispatch = work.dispatchPendingTasks(dispatchBatchSize);
        StageResult outbound = work.retryPendingOutbound(outboxBatchSize);
        work.updateMetrics();

        boolean immediateWorkRemaining = !inboundMailbox.isEmpty()
                || deadlines.immediateWorkRemaining()
                || dispatch.immediateWorkRemaining()
                || outbound.immediateWorkRemaining();
        return new CycleResult(
                messagesProcessed,
                deadlines.processed(),
                dispatch.processed(),
                outbound.processed(),
                immediateWorkRemaining
        );
    }

    long nextPollTimeoutMillis(CycleResult previousCycle) {
        Objects.requireNonNull(previousCycle, "previousCycle");
        if (previousCycle.immediateWorkRemaining() || shutdownAfterDrain) {
            return 0L;
        }
        return Math.max(0L, work.millisUntilNextScheduledWork());
    }

    private int processMessages(long pollTimeoutMillis) throws InterruptedException {
        MessageEnvelope envelope = inboundMailbox.poll(pollTimeoutMillis, TimeUnit.MILLISECONDS);
        if (envelope == null) {
            return 0;
        }

        int processed = 0;
        do {
            work.processEnvelope(envelope);
            processed++;
            envelope = processed < messageBatchSize ? inboundMailbox.poll() : null;
        } while (envelope != null);
        return processed;
    }

    public record StageResult(int processed, boolean immediateWorkRemaining) {
        public StageResult {
            if (processed < 0) {
                throw new IllegalArgumentException("processed must not be negative");
            }
        }

        static StageResult idle() {
            return new StageResult(0, false);
        }
    }

    record CycleResult(
            int messagesProcessed,
            int deadlinesProcessed,
            int dispatchAttempts,
            int outboundAttempts,
            boolean immediateWorkRemaining
    ) {
    }

    /** Narrow orchestration seam implemented by focused services in production. */
    public interface Work {
        void processEnvelope(MessageEnvelope envelope);

        StageResult processDueDeadlines(int limit);

        StageResult dispatchPendingTasks(int limit);

        StageResult retryPendingOutbound(int limit);

        void updateMetrics();

        long millisUntilNextScheduledWork();
    }
}
