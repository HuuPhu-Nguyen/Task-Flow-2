package server.registry;

/**
 * Process-local capacity counters and gauges exposed through scheduler metrics.
 */
public record CapacityMetricsSnapshot(
        long acceptedSnapshots,
        long staleSnapshots,
        long incompatibleSnapshots,
        long reservationsCreated,
        long reservationsReleased,
        long projectionFailures,
        long activeReservations,
        long reservedCapacityUnits
) {
    public static final CapacityMetricsSnapshot EMPTY =
            new CapacityMetricsSnapshot(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
}
