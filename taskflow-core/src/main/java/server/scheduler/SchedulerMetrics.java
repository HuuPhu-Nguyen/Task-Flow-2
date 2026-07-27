package server.scheduler;

import server.db.JobStateStore;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Process-lifetime scheduler metrics updated only after the corresponding
 * scheduler decision has crossed its existing authoritative boundary.
 */
public class SchedulerMetrics {
    public static final String JOBS_ACCEPTED_TOTAL_NAME =
            "taskflow_jobs_accepted_total";
    public static final String JOBS_COMPLETED_TOTAL_NAME =
            "taskflow_jobs_completed_total";
    public static final String JOBS_FAILED_TOTAL_NAME =
            "taskflow_jobs_failed_total";
    public static final String TASKS_ASSIGNED_TOTAL_NAME =
            "taskflow_tasks_assigned_total";
    public static final String TASK_RESULTS_COMMITTED_TOTAL_NAME =
            "taskflow_task_results_committed_total";
    public static final String TASK_RESULTS_STALE_TOTAL_NAME =
            "taskflow_task_results_stale_total";
    public static final String TASK_RESULTS_DUPLICATE_TOTAL_NAME =
            "taskflow_task_results_duplicate_total";
    public static final String ASSIGNMENT_GENERATIONS_TOTAL_NAME =
            "taskflow_assignment_generations_total";
    public static final String TASK_ASSIGNMENT_GENERATIONS_TOTAL_NAME =
            "taskflow_task_assignment_generations_total";
    public static final String TASKS_RETRIED_TOTAL_NAME =
            "taskflow_tasks_retried_total";
    public static final String TASK_LEASE_EXPIRATIONS_TOTAL_NAME =
            "taskflow_task_lease_expirations_total";
    public static final String PAYLOAD_INTEGRITY_FAILURES_TOTAL_NAME =
            "taskflow_payload_integrity_failures_total";
    public static final String SCHEDULER_MAILBOX_DEPTH_NAME =
            "taskflow_scheduler_mailbox_depth";
    public static final String SCHEDULER_PENDING_TASKS_NAME =
            "taskflow_scheduler_pending_tasks";
    public static final String SCHEDULER_DUE_DEADLINES_NAME =
            "taskflow_scheduler_due_deadlines";
    public static final String WORKER_CAPACITY_USED_NAME =
            "taskflow_worker_capacity_used";
    public static final String ASSIGNMENT_LATENCY_SECONDS_NAME =
            "taskflow_assignment_latency_seconds";
    public static final String RESULT_COMMIT_LATENCY_SECONDS_NAME =
            "taskflow_result_commit_latency_seconds";
    public static final String RECOVERY_DURATION_SECONDS_NAME =
            "taskflow_recovery_duration_seconds";

    private static final double[] ASSIGNMENT_LATENCY_BUCKETS_SECONDS = {
            0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
            1.0, 2.5, 5.0, 10.0
    };
    private static final double[] RESULT_COMMIT_LATENCY_BUCKETS_SECONDS = {
            0.010, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
            10.0, 30.0, 60.0, 120.0, 300.0
    };
    private static final double[] RECOVERY_DURATION_BUCKETS_SECONDS = {
            0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
            1.0, 2.5, 5.0, 10.0, 30.0, 60.0
    };

    private final AtomicLong queueDepth = new AtomicLong(0);
    private final AtomicLong activeJobs = new AtomicLong(0);
    private final AtomicLong activeTasks = new AtomicLong(0);
    private final AtomicLong pendingTasks = new AtomicLong(0);
    private final AtomicLong dueDeadlines = new AtomicLong(0);
    private final AtomicLong workerCapacityUsed = new AtomicLong(0);
    private final AtomicLong jobsAcceptedTotal = new AtomicLong(0);
    private final AtomicLong jobsCompletedTotal = new AtomicLong(0);
    private final AtomicLong jobsFailedTotal = new AtomicLong(0);
    private final AtomicLong tasksAssignedTotal = new AtomicLong(0);
    private final AtomicLong assignmentGenerationsTotal = new AtomicLong(0);
    private final AtomicLong retryCount = new AtomicLong(0);
    private final AtomicLong taskLeaseExpirationsTotal = new AtomicLong(0);
    private final AtomicLong taskResultsCommittedTotal = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong terminalFailureCount = new AtomicLong(0);
    private final AtomicLong taskResultsDuplicateTotal = new AtomicLong(0);
    private final AtomicLong taskResultsStaleTotal = new AtomicLong(0);
    private final AtomicLong unknownResultCount = new AtomicLong(0);
    private final AtomicLong resultStorageFailureCount = new AtomicLong(0);
    private final AtomicLong payloadIntegrityFailuresTotal = new AtomicLong(0);
    private final Histogram assignmentLatency =
            new Histogram(ASSIGNMENT_LATENCY_BUCKETS_SECONDS);
    private final Histogram resultCommitLatency =
            new Histogram(RESULT_COMMIT_LATENCY_BUCKETS_SECONDS);
    private final Histogram recoveryDuration =
            new Histogram(RECOVERY_DURATION_BUCKETS_SECONDS);

    public void setQueueDepth(long depth) {
        queueDepth.set(Math.max(0L, depth));
    }

    public void setActiveJobs(long count) {
        activeJobs.set(Math.max(0L, count));
    }

    public void setActiveTasks(long count) {
        activeTasks.set(Math.max(0L, count));
    }

    public void setPendingTasks(long count) {
        pendingTasks.set(Math.max(0L, count));
    }

    public void setDueDeadlines(long count) {
        dueDeadlines.set(Math.max(0L, count));
    }

    public void setWorkerCapacityUsed(long count) {
        workerCapacityUsed.set(Math.max(0L, count));
    }

    public void recordJobAccepted() {
        jobsAcceptedTotal.incrementAndGet();
    }

    public void recordJobTerminal(boolean successful) {
        if (successful) {
            jobsCompletedTotal.incrementAndGet();
        } else {
            jobsFailedTotal.incrementAndGet();
        }
    }

    public void recordAssignmentGeneration(long dispatchLatencyMs) {
        tasksAssignedTotal.incrementAndGet();
        assignmentGenerationsTotal.incrementAndGet();
        assignmentLatency.recordMillis(dispatchLatencyMs);
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

    public void recordLeaseExpiration() {
        taskLeaseExpirationsTotal.incrementAndGet();
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
        recordResultCommitOutcome(outcome, -1L);
    }

    public void recordResultCommitOutcome(
            JobStateStore.ResultCommitOutcome outcome,
            long commitLatencyMs
    ) {
        if (outcome == null) {
            return;
        }
        switch (outcome) {
            case COMMITTED -> {
                taskResultsCommittedTotal.incrementAndGet();
                resultCommitLatency.recordMillis(commitLatencyMs);
            }
            case DUPLICATE_ALREADY_COMPLETED -> taskResultsDuplicateTotal.incrementAndGet();
            case STALE_ASSIGNMENT -> taskResultsStaleTotal.incrementAndGet();
            case UNKNOWN_TASK -> unknownResultCount.incrementAndGet();
            case STORAGE_FAILURE -> resultStorageFailureCount.incrementAndGet();
        }
    }

    public void recordRecoveryDuration(long durationMillis) {
        recoveryDuration.recordMillis(durationMillis);
    }

    public long recordPayloadIntegrityFailure() {
        return payloadIntegrityFailuresTotal.incrementAndGet();
    }

    public Snapshot snapshot() {
        long successes = taskResultsCommittedTotal.get();
        long failures = failureCount.get();
        long totalAttempts = successes + failures;
        double successRate = totalAttempts == 0 ? 0.0 : successes / (double) totalAttempts;
        HistogramSnapshot assignmentLatencySnapshot = assignmentLatency.snapshot();
        double avgDispatchLatencyMs = assignmentLatencySnapshot.count() == 0L
                ? 0.0
                : assignmentLatencySnapshot.sumSeconds()
                * 1_000.0
                / assignmentLatencySnapshot.count();

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
                successRate,
                jobsAcceptedTotal.get(),
                jobsCompletedTotal.get(),
                jobsFailedTotal.get(),
                tasksAssignedTotal.get(),
                taskLeaseExpirationsTotal.get(),
                pendingTasks.get(),
                dueDeadlines.get(),
                workerCapacityUsed.get(),
                assignmentLatencySnapshot,
                resultCommitLatency.snapshot(),
                recoveryDuration.snapshot()
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
            double taskSuccessRate,
            long jobsAcceptedTotal,
            long jobsCompletedTotal,
            long jobsFailedTotal,
            long tasksAssignedTotal,
            long taskLeaseExpirationsTotal,
            long pendingTasks,
            long dueDeadlines,
            long workerCapacityUsed,
            HistogramSnapshot assignmentLatencySeconds,
            HistogramSnapshot resultCommitLatencySeconds,
            HistogramSnapshot recoveryDurationSeconds
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

    public record HistogramSnapshot(
            List<Double> upperBoundsSeconds,
            List<Long> cumulativeBucketCounts,
            long count,
            double sumSeconds
    ) {
        public HistogramSnapshot {
            upperBoundsSeconds = List.copyOf(upperBoundsSeconds);
            cumulativeBucketCounts = List.copyOf(cumulativeBucketCounts);
            if (upperBoundsSeconds.size() != cumulativeBucketCounts.size()) {
                throw new IllegalArgumentException(
                        "Histogram bounds and bucket counts must have the same size"
                );
            }
            if (count < 0L || sumSeconds < 0.0) {
                throw new IllegalArgumentException(
                        "Histogram count and sum must not be negative"
                );
            }
        }
    }

    private static final class Histogram {
        private final double[] upperBoundsSeconds;
        private final AtomicLongArray bucketCounts;
        private final AtomicLong count = new AtomicLong();
        private final AtomicLong sumMillis = new AtomicLong();

        private Histogram(double[] upperBoundsSeconds) {
            this.upperBoundsSeconds = upperBoundsSeconds.clone();
            this.bucketCounts = new AtomicLongArray(upperBoundsSeconds.length);
        }

        private synchronized void recordMillis(long valueMillis) {
            if (valueMillis < 0L) {
                return;
            }
            double valueSeconds = valueMillis / 1_000.0;
            count.incrementAndGet();
            sumMillis.addAndGet(valueMillis);
            for (int i = 0; i < upperBoundsSeconds.length; i++) {
                if (valueSeconds <= upperBoundsSeconds[i]) {
                    bucketCounts.incrementAndGet(i);
                    break;
                }
            }
        }

        private synchronized HistogramSnapshot snapshot() {
            java.util.ArrayList<Double> bounds =
                    new java.util.ArrayList<>(upperBoundsSeconds.length);
            java.util.ArrayList<Long> cumulative =
                    new java.util.ArrayList<>(upperBoundsSeconds.length);
            long running = 0L;
            for (int i = 0; i < upperBoundsSeconds.length; i++) {
                bounds.add(upperBoundsSeconds[i]);
                running += bucketCounts.get(i);
                cumulative.add(running);
            }
            return new HistogramSnapshot(
                    bounds,
                    cumulative,
                    count.get(),
                    sumMillis.get() / 1_000.0
            );
        }
    }
}
