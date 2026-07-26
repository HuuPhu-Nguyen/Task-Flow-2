package server.registry;

import protocol.PeerIdentity;

import java.util.Locale;

/**
 * Exact coordinator-side capacity claim for one durable assignment generation.
 */
public record AssignmentCapacityReservation(
        String jobId,
        String taskId,
        int attemptNumber,
        String assignmentId,
        String workerId,
        String taskType,
        int capacityUnitCost
) {
    public AssignmentCapacityReservation {
        jobId = requireText(jobId, "jobId");
        taskId = requireText(taskId, "taskId");
        if (attemptNumber <= 0) {
            throw new IllegalArgumentException("attemptNumber must be positive.");
        }
        assignmentId = requireText(assignmentId, "assignmentId");
        workerId = PeerIdentity.require(workerId);
        taskType = requireText(taskType, "taskType").toUpperCase(Locale.ROOT);
        if (capacityUnitCost <= 0) {
            throw new IllegalArgumentException("capacityUnitCost must be positive.");
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required.");
        }
        return value.trim();
    }
}
