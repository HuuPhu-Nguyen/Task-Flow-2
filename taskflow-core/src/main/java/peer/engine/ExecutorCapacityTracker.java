package peer.engine;

import plugin.TaskResourceCatalog;
import protocol.TaskAssignMessage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Atomic executor-local accepted-work accounting and heartbeat snapshots.
 */
public final class ExecutorCapacityTracker {
    public enum ReserveOutcome {
        RESERVED,
        ALREADY_RESERVED,
        IDENTITY_MISMATCH
    }

    public enum ReleaseOutcome {
        RELEASED,
        ALREADY_RELEASED,
        IDENTITY_MISMATCH
    }

    private final ExecutorCapacityConfig config;
    private final TaskResourceCatalog resources;
    private final String executorInstanceId;
    private final Map<String, LocalExecutionCapacityReservation> reservations =
            new LinkedHashMap<>();
    private final Map<String, Integer> reservedTypeCounts = new LinkedHashMap<>();
    private long sequence;
    private long reservedUnits;

    public ExecutorCapacityTracker(ExecutorCapacityConfig config,
                                   TaskResourceCatalog resources) {
        this(config, resources, UUID.randomUUID().toString());
    }

    ExecutorCapacityTracker(ExecutorCapacityConfig config,
                            TaskResourceCatalog resources,
                            String executorInstanceId) {
        this.config = Objects.requireNonNull(config, "config");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.executorInstanceId = Objects.requireNonNull(
                executorInstanceId,
                "executorInstanceId"
        );
        UUID.fromString(executorInstanceId);
        if (!config.maxConcurrencyByTaskType().keySet().equals(resources.taskTypes())) {
            throw new IllegalArgumentException(
                    "Executor capacity types must match the resource catalog."
            );
        }
    }

    public synchronized ReserveOutcome reserve(TaskAssignMessage task) {
        Objects.requireNonNull(task, "task");
        int cost = resources.require(task.getTaskType()).capacityUnitCost();
        LocalExecutionCapacityReservation requested = new LocalExecutionCapacityReservation(
                task.getAssignmentId(),
                task.getTaskId(),
                task.getAttemptNumber(),
                task.getTaskType(),
                cost
        );
        LocalExecutionCapacityReservation current = reservations.get(requested.assignmentId());
        if (current != null) {
            return current.equals(requested)
                    ? ReserveOutcome.ALREADY_RESERVED
                    : ReserveOutcome.IDENTITY_MISMATCH;
        }
        long nextReservedUnits = Math.addExact(reservedUnits, cost);
        int nextTypeCount = Math.addExact(
                reservedTypeCounts.getOrDefault(requested.taskType(), 0),
                1
        );
        reservations.put(requested.assignmentId(), requested);
        reservedTypeCounts.put(requested.taskType(), nextTypeCount);
        reservedUnits = nextReservedUnits;
        return ReserveOutcome.RESERVED;
    }

    public synchronized ReleaseOutcome release(TaskAssignMessage task) {
        Objects.requireNonNull(task, "task");
        LocalExecutionCapacityReservation current = reservations.get(task.getAssignmentId());
        if (current == null) {
            return ReleaseOutcome.ALREADY_RELEASED;
        }
        int cost = resources.require(task.getTaskType()).capacityUnitCost();
        LocalExecutionCapacityReservation expected = new LocalExecutionCapacityReservation(
                task.getAssignmentId(),
                task.getTaskId(),
                task.getAttemptNumber(),
                task.getTaskType(),
                cost
        );
        if (!current.equals(expected)) {
            return ReleaseOutcome.IDENTITY_MISMATCH;
        }
        reservations.remove(expected.assignmentId());
        int remainingTypeCount = reservedTypeCounts.get(expected.taskType()) - 1;
        if (remainingTypeCount == 0) {
            reservedTypeCounts.remove(expected.taskType());
        } else {
            reservedTypeCounts.put(expected.taskType(), remainingTypeCount);
        }
        reservedUnits -= expected.capacityUnitCost();
        return ReleaseOutcome.RELEASED;
    }

    public synchronized ExecutorCapacitySnapshot snapshot() {
        if (sequence == Long.MAX_VALUE) {
            throw new IllegalStateException("Capacity snapshot sequence exhausted.");
        }
        sequence++;
        int available = reservedUnits >= config.totalCapacityUnits()
                ? 0
                : (int) (config.totalCapacityUnits() - reservedUnits);
        return new ExecutorCapacitySnapshot(
                executorInstanceId,
                sequence,
                config.totalCapacityUnits(),
                available,
                config.maxConcurrencyByTaskType()
        );
    }

    public synchronized int reservationCount() {
        return reservations.size();
    }

    public synchronized long reservedUnits() {
        return reservedUnits;
    }

    public synchronized boolean overcommitted() {
        if (reservedUnits > config.totalCapacityUnits()) {
            return true;
        }
        for (Map.Entry<String, Integer> entry : reservedTypeCounts.entrySet()) {
            if (entry.getValue() > config.maxConcurrencyByTaskType().get(entry.getKey())) {
                return true;
            }
        }
        return false;
    }

    public synchronized boolean clear() {
        if (reservations.isEmpty()) {
            return false;
        }
        reservations.clear();
        reservedTypeCounts.clear();
        reservedUnits = 0L;
        return true;
    }
}
