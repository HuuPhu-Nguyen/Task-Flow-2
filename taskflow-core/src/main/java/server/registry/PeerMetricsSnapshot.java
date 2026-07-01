package server.registry;

public record PeerMetricsSnapshot(long completedTasks,
                                  long failedTasks,
                                  long latencyEwmaMs,
                                  long taskDurationEwmaMs) {

    public PeerMetricsSnapshot {
        completedTasks = Math.max(0L, completedTasks);
        failedTasks = Math.max(0L, failedTasks);
        latencyEwmaMs = Math.max(0L, latencyEwmaMs);
        taskDurationEwmaMs = Math.max(0L, taskDurationEwmaMs);
    }

    public static PeerMetricsSnapshot empty() {
        return new PeerMetricsSnapshot(0L, 0L, 0L, 0L);
    }
}
