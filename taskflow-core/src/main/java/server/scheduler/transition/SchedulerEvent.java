package server.scheduler.transition;

import java.util.Objects;

/**
 * Closed set of scheduler facts understood by the pure task transition model.
 * Infrastructure adapters are responsible for validating protocol messages and
 * turning them into one of these events.
 */
public sealed interface SchedulerEvent permits SchedulerEvent.JobSubmitted,
        SchedulerEvent.AssignmentRequested,
        SchedulerEvent.TaskResultReceived,
        SchedulerEvent.TaskExecutionFailed,
        SchedulerEvent.LeaseExpired,
        SchedulerEvent.TaskTimedOut,
        SchedulerEvent.WorkerUnavailable,
        SchedulerEvent.CoordinatorRecovered {

    long occurredAtMillis();

    record JobSubmitted(long occurredAtMillis) implements SchedulerEvent {
        public JobSubmitted {
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }
    }

    record AssignmentRequested(TaskState.Assignment assignment,
                               long occurredAtMillis) implements SchedulerEvent {
        public AssignmentRequested {
            Objects.requireNonNull(assignment, "assignment");
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }
    }

    record TaskResultReceived(TaskState.AssignmentIdentity assignment,
                              long occurredAtMillis) implements SchedulerEvent {
        public TaskResultReceived {
            Objects.requireNonNull(assignment, "assignment");
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }
    }

    record TaskExecutionFailed(TaskState.AssignmentIdentity assignment,
                               int maxRetries,
                               boolean retryable,
                               long occurredAtMillis) implements SchedulerEvent {
        public TaskExecutionFailed {
            Objects.requireNonNull(assignment, "assignment");
            requirePositive(maxRetries, "maxRetries");
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }

        public TaskExecutionFailed(TaskState.AssignmentIdentity assignment,
                                   int maxRetries,
                                   long occurredAtMillis) {
            this(assignment, maxRetries, true, occurredAtMillis);
        }
    }

    record LeaseExpired(TaskState.AssignmentIdentity assignment,
                        int maxRetries,
                        long occurredAtMillis) implements SchedulerEvent {
        public LeaseExpired {
            Objects.requireNonNull(assignment, "assignment");
            requirePositive(maxRetries, "maxRetries");
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }
    }

    record TaskTimedOut(TaskState.AssignmentIdentity assignment,
                        int maxRetries,
                        long timeoutMillis,
                        long occurredAtMillis) implements SchedulerEvent {
        public TaskTimedOut {
            Objects.requireNonNull(assignment, "assignment");
            requirePositive(maxRetries, "maxRetries");
            requirePositive(timeoutMillis, "timeoutMillis");
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }
    }

    /**
     * The mandated event name uses "worker". Each affected task receives the
     * exact assignment snapshot owned by that executor participant, so peer
     * identity alone never substitutes for generation identity.
     */
    record WorkerUnavailable(TaskState.AssignmentIdentity assignment,
                             int maxRetries,
                             long occurredAtMillis) implements SchedulerEvent {
        public WorkerUnavailable {
            Objects.requireNonNull(assignment, "assignment");
            requirePositive(maxRetries, "maxRetries");
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }
    }

    record CoordinatorRecovered(long occurredAtMillis) implements SchedulerEvent {
        public CoordinatorRecovered {
            requireNonNegative(occurredAtMillis, "occurredAtMillis");
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must not be negative.");
        }
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " must be positive.");
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank.");
        }
        return value;
    }
}
