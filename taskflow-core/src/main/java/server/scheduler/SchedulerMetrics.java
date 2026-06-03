package server.scheduler;

import java.util.concurrent.atomic.AtomicLong;

public class SchedulerMetrics {
    private final AtomicLong queueDepth = new AtomicLong(0);
    private final AtomicLong activeJobs = new AtomicLong(0);
    private final AtomicLong assignedCount = new AtomicLong(0);
    private final AtomicLong retryCount = new AtomicLong(0);
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong terminalFailureCount = new AtomicLong(0);
    private final AtomicLong dispatchLatencyTotalMs = new AtomicLong(0);
    private final AtomicLong dispatchLatencySamples = new AtomicLong(0);

    public void setQueueDepth(long depth) {
        queueDepth.set(Math.max(0L, depth));
    }

    public void setActiveJobs(long count) {
        activeJobs.set(Math.max(0L, count));
    }

    public void recordDispatch(long dispatchLatencyMs) {
        assignedCount.incrementAndGet();
        if (dispatchLatencyMs >= 0) {
            dispatchLatencyTotalMs.addAndGet(dispatchLatencyMs);
            dispatchLatencySamples.incrementAndGet();
        }
    }

    public void recordRetry() {
        retryCount.incrementAndGet();
    }

    public void recordAttemptSuccess() {
        successCount.incrementAndGet();
    }

    public void recordAttemptFailure(boolean terminal) {
        failureCount.incrementAndGet();
        if (terminal) {
            terminalFailureCount.incrementAndGet();
        }
    }

    public Snapshot snapshot() {
        long successes = successCount.get();
        long failures = failureCount.get();
        long totalAttempts = successes + failures;
        double successRate = totalAttempts == 0 ? 0.0 : successes / (double) totalAttempts;

        long latencySamples = dispatchLatencySamples.get();
        double avgDispatchLatencyMs = latencySamples == 0
                ? 0.0
                : dispatchLatencyTotalMs.get() / (double) latencySamples;

        return new Snapshot(
                queueDepth.get(),
                activeJobs.get(),
                assignedCount.get(),
                retryCount.get(),
                successes,
                failures,
                terminalFailureCount.get(),
                avgDispatchLatencyMs,
                successRate
        );
    }

    public record Snapshot(
            long queueDepth,
            long activeJobs,
            long assignedCount,
            long retryCount,
            long successCount,
            long failureCount,
            long terminalFailureCount,
            double avgDispatchLatencyMs,
            double taskSuccessRate
    ) {}
}
