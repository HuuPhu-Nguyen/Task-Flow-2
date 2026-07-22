package server.scheduler;

import server.db.JobStateStore;

import java.util.concurrent.atomic.AtomicLong;

public class SchedulerMetrics {
    private final AtomicLong queueDepth = new AtomicLong(0);
    private final AtomicLong activeJobs = new AtomicLong(0);
    private final AtomicLong assignedCount = new AtomicLong(0);
    private final AtomicLong retryCount = new AtomicLong(0);
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong terminalFailureCount = new AtomicLong(0);
    private final AtomicLong duplicateResultCount = new AtomicLong(0);
    private final AtomicLong staleResultCount = new AtomicLong(0);
    private final AtomicLong unknownResultCount = new AtomicLong(0);
    private final AtomicLong resultStorageFailureCount = new AtomicLong(0);
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

    public void recordResultCommitOutcome(JobStateStore.ResultCommitOutcome outcome) {
        if (outcome == null) {
            return;
        }
        switch (outcome) {
            case DUPLICATE_ALREADY_COMPLETED -> duplicateResultCount.incrementAndGet();
            case STALE_ASSIGNMENT -> staleResultCount.incrementAndGet();
            case UNKNOWN_TASK -> unknownResultCount.incrementAndGet();
            case STORAGE_FAILURE -> resultStorageFailureCount.incrementAndGet();
            case COMMITTED -> {
                // Attempt success is recorded only after the in-memory projection is updated.
            }
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
                duplicateResultCount.get(),
                staleResultCount.get(),
                unknownResultCount.get(),
                resultStorageFailureCount.get(),
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
            long duplicateResultCount,
            long staleResultCount,
            long unknownResultCount,
            long resultStorageFailureCount,
            double avgDispatchLatencyMs,
            double taskSuccessRate
    ) {}
}
