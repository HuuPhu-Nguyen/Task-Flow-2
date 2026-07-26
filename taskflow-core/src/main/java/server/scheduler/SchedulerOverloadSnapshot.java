package server.scheduler;

import java.util.List;
import java.util.Objects;

/** Immutable live view of coordinator overload pressure. */
public record SchedulerOverloadSnapshot(
        boolean overloaded,
        List<Pressure> reasons,
        Reason primaryReason,
        int jobSubmitPrefetch,
        boolean pendingOutboxObservationHealthy,
        long changedAtEpochMillis
) {
    public SchedulerOverloadSnapshot {
        reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
        if (overloaded != !reasons.isEmpty()) {
            throw new IllegalArgumentException("overloaded must match reasons");
        }
        if (overloaded && primaryReason == null) {
            throw new IllegalArgumentException("primaryReason is required while overloaded");
        }
        if (!overloaded && primaryReason != null) {
            throw new IllegalArgumentException("primaryReason must be null while recovered");
        }
        if (jobSubmitPrefetch <= 0) {
            throw new IllegalArgumentException("jobSubmitPrefetch must be positive");
        }
    }

    public enum Reason {
        TASK_RESULT_RESERVE_CAPACITY,
        SUBMISSION_MAILBOX_CAPACITY,
        MAX_PENDING_OUTBOX_ROWS,
        MAX_ACTIVE_JOBS,
        MAX_ACTIVE_TASKS
    }

    public record Pressure(
            Reason reason,
            long configuredMaximum,
            long observedValue,
            long activeSinceEpochMillis
    ) {
        public Pressure {
            Objects.requireNonNull(reason, "reason");
            if (configuredMaximum <= 0L) {
                throw new IllegalArgumentException("configuredMaximum must be positive");
            }
            if (observedValue < 0L) {
                throw new IllegalArgumentException("observedValue must not be negative");
            }
        }
    }
}
