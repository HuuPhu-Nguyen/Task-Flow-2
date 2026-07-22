package peer.engine;

public record AssignmentCacheSnapshot(
        int size,
        int runningEntries,
        int completedEntries,
        long runningDuplicateCount,
        long completedDuplicateCount,
        long evictionCount,
        long capacityEvictionCount,
        long ttlEvictionCount
) {
    public static AssignmentCacheSnapshot empty() {
        return new AssignmentCacheSnapshot(0, 0, 0, 0L, 0L, 0L, 0L, 0L);
    }
}
