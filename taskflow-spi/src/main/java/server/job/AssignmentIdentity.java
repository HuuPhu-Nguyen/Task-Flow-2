package server.job;

import java.util.UUID;

/**
 * Immutable identity for one task-assignment generation.
 *
 * <p>The attempt number is a monotonic assignment-generation sequence. It is
 * deliberately independent from {@link TaskUnit#getRetryCount()}, which is a
 * retry-policy counter for failed processor attempts.</p>
 */
public record AssignmentIdentity(
        String taskId,
        int attemptNumber,
        String assignmentId,
        String workerId,
        long leaseExpiresAtEpochMillis
) {
    public AssignmentIdentity {
        taskId = requireText(taskId, "taskId");
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be positive.");
        }
        assignmentId = canonicalUuid(assignmentId);
        workerId = requireText(workerId, "workerId");
        if (leaseExpiresAtEpochMillis < 0L) {
            throw new IllegalArgumentException("leaseExpiresAtEpochMillis must not be negative.");
        }
    }

    public static AssignmentIdentity create(String taskId,
                                             int attemptNumber,
                                             String workerId,
                                             long leaseExpiresAtEpochMillis) {
        return new AssignmentIdentity(
                taskId,
                attemptNumber,
                UUID.randomUUID().toString(),
                workerId,
                leaseExpiresAtEpochMillis
        );
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required.");
        }
        return value.trim();
    }

    private static String canonicalUuid(String value) {
        String candidate = requireText(value, "assignmentId");
        try {
            String canonical = UUID.fromString(candidate).toString();
            if (!canonical.equalsIgnoreCase(candidate)) {
                throw new IllegalArgumentException("assignmentId must use canonical UUID syntax.");
            }
            return canonical;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("assignmentId must be a UUID.", e);
        }
    }
}
