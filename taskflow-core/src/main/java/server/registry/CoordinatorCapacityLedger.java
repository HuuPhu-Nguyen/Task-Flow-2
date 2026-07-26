package server.registry;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Bounded by the number of durable, currently assigned task generations. */
final class CoordinatorCapacityLedger {
    enum ReserveOutcome {
        RESERVED,
        ALREADY_RESERVED,
        IDENTITY_MISMATCH
    }

    enum ReleaseOutcome {
        RELEASED,
        ALREADY_RELEASED,
        IDENTITY_MISMATCH
    }

    private final Map<String, AssignmentCapacityReservation> byAssignmentId =
            new LinkedHashMap<>();
    private final Map<String, Long> reservedUnitsByWorker = new HashMap<>();
    private final Map<String, Map<String, Integer>> reservedTypeCountsByWorker =
            new HashMap<>();

    ReserveOutcome reserve(AssignmentCapacityReservation reservation) {
        AssignmentCapacityReservation current =
                byAssignmentId.get(reservation.assignmentId());
        if (current != null) {
            return current.equals(reservation)
                    ? ReserveOutcome.ALREADY_RESERVED
                    : ReserveOutcome.IDENTITY_MISMATCH;
        }
        long nextReservedUnits = Math.addExact(
                reservedUnits(reservation.workerId()),
                reservation.capacityUnitCost()
        );
        Map<String, Integer> currentTypeCounts =
                reservedTypeCountsByWorker.get(reservation.workerId());
        int nextTypeCount = Math.addExact(
                currentTypeCounts == null
                        ? 0
                        : currentTypeCounts.getOrDefault(reservation.taskType(), 0),
                1
        );
        byAssignmentId.put(reservation.assignmentId(), reservation);
        reservedUnitsByWorker.put(reservation.workerId(), nextReservedUnits);
        reservedTypeCountsByWorker
                .computeIfAbsent(reservation.workerId(), ignored -> new HashMap<>())
                .put(reservation.taskType(), nextTypeCount);
        return ReserveOutcome.RESERVED;
    }

    ReleaseOutcome release(AssignmentCapacityReservation reservation) {
        AssignmentCapacityReservation current =
                byAssignmentId.get(reservation.assignmentId());
        if (current == null) {
            return ReleaseOutcome.ALREADY_RELEASED;
        }
        if (!current.equals(reservation)) {
            return ReleaseOutcome.IDENTITY_MISMATCH;
        }

        Long workerUnits = reservedUnitsByWorker.get(reservation.workerId());
        Map<String, Integer> typeCounts =
                reservedTypeCountsByWorker.get(reservation.workerId());
        Integer typeCount = typeCounts == null
                ? null
                : typeCounts.get(reservation.taskType());
        if (workerUnits == null
                || workerUnits < reservation.capacityUnitCost()
                || typeCount == null
                || typeCount <= 0) {
            throw new IllegalStateException(
                    "Capacity reservation aggregates do not match assignment "
                            + reservation.assignmentId() + "."
            );
        }
        long remainingUnits = Math.subtractExact(
                workerUnits,
                reservation.capacityUnitCost()
        );
        int remainingTypeCount = Math.subtractExact(typeCount, 1);

        byAssignmentId.remove(reservation.assignmentId());
        if (remainingUnits == 0L) {
            reservedUnitsByWorker.remove(reservation.workerId());
        } else {
            reservedUnitsByWorker.put(reservation.workerId(), remainingUnits);
        }
        if (remainingTypeCount == 0) {
            typeCounts.remove(reservation.taskType());
        } else {
            typeCounts.put(reservation.taskType(), remainingTypeCount);
        }
        if (typeCounts.isEmpty()) {
            reservedTypeCountsByWorker.remove(reservation.workerId());
        }
        return ReleaseOutcome.RELEASED;
    }

    long reservedUnits(String workerId) {
        return reservedUnitsByWorker.getOrDefault(workerId, 0L);
    }

    int reservedTypeCount(String workerId, String taskType) {
        return reservedTypeCountsByWorker
                .getOrDefault(workerId, Map.of())
                .getOrDefault(taskType, 0);
    }

    int reservationCount() {
        return byAssignmentId.size();
    }

    long reservedUnitsTotal() {
        long total = 0L;
        for (long reservedUnits : reservedUnitsByWorker.values()) {
            total = Math.addExact(total, reservedUnits);
        }
        return total;
    }

}
