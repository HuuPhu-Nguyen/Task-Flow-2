package server.scheduler;

import server.db.JobStateStore;

import java.util.concurrent.atomic.AtomicLong;

public class SchedulerMetrics {
    public static final String TASK_RESULTS_COMMITTED_TOTAL_NAME =
            "taskflow_task_results_committed_total";
    public static final String TASK_RESULTS_STALE_TOTAL_NAME =
            "taskflow_task_results_stale_total";
    public static final String TASK_RESULTS_DUPLICATE_TOTAL_NAME =
            "taskflow_task_results_duplicate_total";
    public static final String ASSIGNMENT_GENERATIONS_TOTAL_NAME =
            "taskflow_assignment_generations_total";
    public static final String PAYLOAD_INTEGRITY_FAILURES_TOTAL_NAME =
            "taskflow_payload_integrity_failures_total";

    private final AtomicLong queueDepth = new AtomicLong(0);
    private final AtomicLong activeJobs = new AtomicLong(0);
    private final AtomicLong activeTasks = new AtomicLong(0);
    private final AtomicLong assignmentGenerationsTotal = new AtomicLong(0);
    private final AtomicLong retryCount = new AtomicLong(0);
    private final AtomicLong taskResultsCommittedTotal = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong terminalFailureCount = new AtomicLong(0);
    private final AtomicLong taskResultsDuplicateTotal = new AtomicLong(0);
    private final AtomicLong taskResultsStaleTotal = new AtomicLong(0);
    private final AtomicLong unknownResultCount = new AtomicLong(0);
    private final AtomicLong resultStorageFailureCount = new AtomicLong(0);
    private final AtomicLong payloadIntegrityFailuresTotal = new AtomicLong(0);
    private final AtomicLong dispatchLatencyTotalMs = new AtomicLong(0);
    private final AtomicLong dispatchLatencySamples = new AtomicLong(0);

    public void setQueueDepth(long depth) {
        queueDepth.set(Math.max(0L, depth));
    }

    public void setActiveJobs(long count) {
        activeJobs.set(Math.max(0L, count));
    }

    public void setActiveTasks(long count) {
        activeTasks.set(Math.max(0L, count));
    }

    public void recordAssignmentGeneration(long dispatchLatencyMs) {
        assignmentGenerationsTotal.incrementAndGet();
        if (dispatchLatencyMs >= 0) {
            dispatchLatencyTotalMs.addAndGet(dispatchLatencyMs);
            dispatchLatencySamples.incrementAndGet();
        }
    }

    /**
     * Compatibility alias for callers compiled against the pre-fencing metric API.
     */
    @Deprecated(forRemoval = false)
    public void recordDispatch(long dispatchLatencyMs) {
        recordAssignmentGeneration(dispatchLatencyMs);
    }

    public void recordRetry() {
        retryCount.incrementAndGet();
    }

    /**
     * Compatibility alias for callers compiled against the pre-fencing metric API.
     */
    @Deprecated(forRemoval = false)
    public void recordAttemptSuccess() {
        recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.COMMITTED);
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
            case COMMITTED -> taskResultsCommittedTotal.incrementAndGet();
            case DUPLICATE_ALREADY_COMPLETED -> taskResultsDuplicateTotal.incrementAndGet();
            case STALE_ASSIGNMENT -> taskResultsStaleTotal.incrementAndGet();
            case UNKNOWN_TASK -> unknownResultCount.incrementAndGet();
            case STORAGE_FAILURE -> resultStorageFailureCount.incrementAndGet();
        }
    }

    public long recordPayloadIntegrityFailure() {
        return payloadIntegrityFailuresTotal.incrementAndGet();
    }

    public Snapshot snapshot() {
        long successes = taskResultsCommittedTotal.get();
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
                activeTasks.get(),
                assignmentGenerationsTotal.get(),
                retryCount.get(),
                successes,
                failures,
                terminalFailureCount.get(),
                taskResultsDuplicateTotal.get(),
                taskResultsStaleTotal.get(),
                unknownResultCount.get(),
                resultStorageFailureCount.get(),
                payloadIntegrityFailuresTotal.get(),
                avgDispatchLatencyMs,
                successRate
        );
    }

    public record Snapshot(
            long queueDepth,
            long activeJobs,
            long activeTasks,
            long assignmentGenerationsTotal,
            long retryCount,
            long taskResultsCommittedTotal,
            long failureCount,
            long terminalFailureCount,
            long taskResultsDuplicateTotal,
            long taskResultsStaleTotal,
            long unknownResultCount,
            long resultStorageFailureCount,
            long payloadIntegrityFailuresTotal,
            double avgDispatchLatencyMs,
            double taskSuccessRate
    ) {
        public long assignedCount() {
            return assignmentGenerationsTotal;
        }

        public long successCount() {
            return taskResultsCommittedTotal;
        }

        public long duplicateResultCount() {
            return taskResultsDuplicateTotal;
        }

        public long staleResultCount() {
            return taskResultsStaleTotal;
        }
    }
}
