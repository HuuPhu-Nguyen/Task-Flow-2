package server.scheduler.transition;

/**
 * Immutable input and output projection for one task and its containing job.
 * The latest assignment is retained after closure so replay and stale
 * generations can be classified without consulting infrastructure. The last
 * accepted event is an in-process replay marker for the reducer; it is not a
 * substitute for the store's conditional transition predicate.
 */
public record TaskState(JobStatus jobStatus,
                        TaskStatus taskStatus,
                        int retryCount,
                        int lastAttemptNumber,
                        Assignment latestAssignment,
                        long startedAtMillis,
                        long pendingSinceMillis,
                        SchedulerEvent lastAcceptedEvent) {

    public enum JobStatus {
        NOT_CREATED,
        RUNNING,
        COMPLETED,
        FAILED
    }

    public enum TaskStatus {
        NOT_CREATED,
        PENDING,
        ASSIGNED,
        COMPLETED,
        FAILED
    }

    /**
     * Fencing tuple carried by result and failure events.
     */
    public record AssignmentIdentity(int attemptNumber,
                                     String assignmentId,
                                     String workerId) {
        public AssignmentIdentity {
            if (attemptNumber <= 0) {
                throw new IllegalArgumentException("attemptNumber must be positive.");
            }
            assignmentId = requireText(assignmentId, "assignmentId");
            workerId = requireText(workerId, "workerId");
        }

        private static String requireText(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank.");
            }
            return value;
        }
    }

    /**
     * Complete coordinator assignment state, including lease metadata that is
     * intentionally absent from result/failure protocol events.
     */
    public record Assignment(AssignmentIdentity identity,
                             String leaseOwnerId,
                             long leaseExpiresAtMillis) {
        public Assignment {
            if (identity == null) {
                throw new IllegalArgumentException("identity is required.");
            }
            leaseOwnerId = requireText(leaseOwnerId, "leaseOwnerId");
            if (leaseExpiresAtMillis < 0L) {
                throw new IllegalArgumentException("leaseExpiresAtMillis must not be negative.");
            }
        }

        public int attemptNumber() {
            return identity.attemptNumber();
        }

        public String assignmentId() {
            return identity.assignmentId();
        }

        public String workerId() {
            return identity.workerId();
        }

        private static String requireText(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank.");
            }
            return value;
        }
    }

    public TaskState {
        if (jobStatus == null) {
            throw new IllegalArgumentException("jobStatus is required.");
        }
        if (taskStatus == null) {
            throw new IllegalArgumentException("taskStatus is required.");
        }
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount must not be negative.");
        }
        if (lastAttemptNumber < 0) {
            throw new IllegalArgumentException("lastAttemptNumber must not be negative.");
        }
        if (startedAtMillis < 0L) {
            throw new IllegalArgumentException("startedAtMillis must not be negative.");
        }
        if (pendingSinceMillis < -1L) {
            throw new IllegalArgumentException("pendingSinceMillis must be -1 or non-negative.");
        }
        if (latestAssignment != null && latestAssignment.attemptNumber() != lastAttemptNumber) {
            throw new IllegalArgumentException(
                    "latestAssignment attempt must equal lastAttemptNumber."
            );
        }

        validateLifecycle(jobStatus, taskStatus);
        validateProjection(taskStatus, latestAssignment, startedAtMillis, pendingSinceMillis);
        if (taskStatus == TaskStatus.NOT_CREATED
                && (retryCount != 0 || lastAttemptNumber != 0)) {
            throw new IllegalArgumentException("An absent task cannot have counters.");
        }
    }

    public static TaskState absent() {
        return new TaskState(
                JobStatus.NOT_CREATED,
                TaskStatus.NOT_CREATED,
                0,
                0,
                null,
                0L,
                -1L,
                null
        );
    }

    public static TaskState pending(int retryCount,
                                    int lastAttemptNumber,
                                    Assignment latestAssignment,
                                    long pendingSinceMillis) {
        return new TaskState(
                JobStatus.RUNNING,
                TaskStatus.PENDING,
                retryCount,
                lastAttemptNumber,
                latestAssignment,
                0L,
                pendingSinceMillis,
                null
        );
    }

    public static TaskState assigned(int retryCount,
                                     Assignment assignment,
                                     long startedAtMillis) {
        if (assignment == null) {
            throw new IllegalArgumentException("assignment is required.");
        }
        return new TaskState(
                JobStatus.RUNNING,
                TaskStatus.ASSIGNED,
                retryCount,
                assignment.attemptNumber(),
                assignment,
                startedAtMillis,
                -1L,
                null
        );
    }

    /**
     * Represents a legacy persisted ASSIGNED row whose complete fencing tuple
     * cannot be reconstructed. Recovery must release it before rescheduling.
     */
    public static TaskState incompleteAssigned(int retryCount,
                                               int lastAttemptNumber,
                                               long startedAtMillis) {
        return new TaskState(
                JobStatus.RUNNING,
                TaskStatus.ASSIGNED,
                retryCount,
                lastAttemptNumber,
                null,
                startedAtMillis,
                -1L,
                null
        );
    }

    public static TaskState completed(int retryCount, Assignment latestAssignment) {
        int attemptNumber = latestAssignment == null ? 0 : latestAssignment.attemptNumber();
        return new TaskState(
                JobStatus.RUNNING,
                TaskStatus.COMPLETED,
                retryCount,
                attemptNumber,
                latestAssignment,
                0L,
                -1L,
                null
        );
    }

    public static TaskState failed(int retryCount, Assignment latestAssignment) {
        int attemptNumber = latestAssignment == null ? 0 : latestAssignment.attemptNumber();
        return new TaskState(
                JobStatus.RUNNING,
                TaskStatus.FAILED,
                retryCount,
                attemptNumber,
                latestAssignment,
                0L,
                -1L,
                null
        );
    }

    private static void validateLifecycle(JobStatus jobStatus, TaskStatus taskStatus) {
        if (jobStatus == JobStatus.NOT_CREATED || taskStatus == TaskStatus.NOT_CREATED) {
            if (jobStatus != JobStatus.NOT_CREATED || taskStatus != TaskStatus.NOT_CREATED) {
                throw new IllegalArgumentException(
                        "Job and task must both be NOT_CREATED at the pre-submission boundary."
                );
            }
            return;
        }
        if (jobStatus == JobStatus.COMPLETED && taskStatus != TaskStatus.COMPLETED) {
            throw new IllegalArgumentException("A completed job may contain only completed tasks.");
        }
        if (jobStatus == JobStatus.FAILED
                && taskStatus != TaskStatus.COMPLETED
                && taskStatus != TaskStatus.FAILED) {
            throw new IllegalArgumentException("A failed job may contain only terminal tasks.");
        }
    }

    private static void validateProjection(TaskStatus taskStatus,
                                           Assignment latestAssignment,
                                           long startedAtMillis,
                                           long pendingSinceMillis) {
        switch (taskStatus) {
            case NOT_CREATED -> {
                if (latestAssignment != null || startedAtMillis != 0L || pendingSinceMillis != -1L) {
                    throw new IllegalArgumentException("An absent task cannot have projection data.");
                }
            }
            case PENDING -> {
                if (startedAtMillis != 0L || pendingSinceMillis < 0L) {
                    throw new IllegalArgumentException(
                            "A pending task requires pendingSinceMillis and no start time."
                    );
                }
            }
            case ASSIGNED -> {
                if (pendingSinceMillis != -1L) {
                    throw new IllegalArgumentException("An assigned task cannot have pending time.");
                }
            }
            case COMPLETED, FAILED -> {
                if (startedAtMillis != 0L || pendingSinceMillis != -1L) {
                    throw new IllegalArgumentException("A terminal task cannot have live timing data.");
                }
            }
        }
    }
}
