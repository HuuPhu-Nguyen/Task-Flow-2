package peer.engine;

public record LocalExecutionCapacityReservation(
        String assignmentId,
        String taskId,
        int attemptNumber,
        String taskType,
        int capacityUnitCost
) {
    public LocalExecutionCapacityReservation {
        if (assignmentId == null || assignmentId.isBlank()) {
            throw new IllegalArgumentException("assignmentId is required.");
        }
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId is required.");
        }
        if (attemptNumber <= 0) {
            throw new IllegalArgumentException("attemptNumber must be positive.");
        }
        taskType = plugin.TaskResourceCatalog.normalizeTaskType(taskType);
        if (capacityUnitCost <= 0) {
            throw new IllegalArgumentException("capacityUnitCost must be positive.");
        }
    }
}
