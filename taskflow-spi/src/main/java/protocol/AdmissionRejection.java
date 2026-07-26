package protocol;

/**
 * Machine-readable detail for a job submission rejected before durable
 * acceptance.
 */
public record AdmissionRejection(
        Limit limit,
        long configuredMaximum,
        long observedValue
) {
    public AdmissionRejection {
        if (limit == null) {
            throw new IllegalArgumentException("Admission rejection limit is required.");
        }
        if (configuredMaximum <= 0L) {
            throw new IllegalArgumentException(
                    "Admission rejection configuredMaximum must be positive."
            );
        }
        if (observedValue < 0L) {
            throw new IllegalArgumentException(
                    "Admission rejection observedValue must not be negative."
            );
        }
    }

    public enum Limit {
        MAX_ACTIVE_JOBS,
        MAX_ACTIVE_TASKS,
        MAX_TASKS_PER_JOB,
        MAX_INLINE_MESSAGE_BYTES,
        MAX_REFERENCED_PAYLOAD_BYTES,
        MAX_PENDING_OUTBOX_ROWS,
        MAX_SCHEDULER_MAILBOX_DEPTH
    }
}
