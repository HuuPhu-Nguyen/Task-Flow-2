package server.registry;

import transport.TransportConnection;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PeerInfo {

    private static final int MAX_ACTIVE_TASKS = 3;
    private static final double EWMA_ALPHA = 0.2;

    private final String nodeId;
    private final TransportConnection connection;

    private final AtomicLong lastHeartbeatReceivedAtMillis;
    private final AtomicInteger activeTasks = new AtomicInteger(0);

    private volatile long latencyEwmaMs;         // Smoothed RTT
    private volatile long taskDurationEwmaMs;    // Smoothed task runtime
    private final AtomicLong completedTasks = new AtomicLong(0);
    private final AtomicLong failedTasks = new AtomicLong(0);

    public PeerInfo(String nodeId) {
        this(nodeId, new NoopTransportConnection(nodeId));
    }

    public PeerInfo(String nodeId, TransportConnection connection) {
        this.nodeId = nodeId;
        this.connection = connection;
        this.lastHeartbeatReceivedAtMillis = new AtomicLong(System.currentTimeMillis());
    }

    public void send(protocol.Message message) {
        if (connection != null && connection.isOpen()) {
            connection.send(message);
        } else {
            System.err.println("No sender attached for " + nodeId);
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isConnected() {
        return connection != null && connection.isOpen();
    }

    public void closeConnection() {
        if (connection != null) {
            connection.close();
        }
    }

    public long getLastHeartbeatReceivedAtMillis() {return lastHeartbeatReceivedAtMillis.get();}

    public void updateHeartbeatReceivedNow() {lastHeartbeatReceivedAtMillis.set(System.currentTimeMillis());}

    public int getActiveTasks() {return activeTasks.get();}

    public int incrementTasks() {return activeTasks.incrementAndGet();}

    public int decrementTasks() {
        return activeTasks.updateAndGet(current -> Math.max(0, current - 1));
    }

    public synchronized void updateLatency(long rtt) {
        if (rtt <= 0) {
            return;
        }
        latencyEwmaMs = applyEwma(latencyEwmaMs, rtt);
    }

    public synchronized void recordTaskSuccess(long durationMs) {
        completedTasks.incrementAndGet();
        if (durationMs <= 0) {
            return;
        }
        taskDurationEwmaMs = applyEwma(taskDurationEwmaMs, durationMs);
    }

    public void recordTaskFailure() {
        failedTasks.incrementAndGet();
    }

    public long getLatency() { return latencyEwmaMs; }
    public long getAvgTaskDuration() { return taskDurationEwmaMs; }
    public long getFailedTasks() { return failedTasks.get(); }
    public long getCompletedTasks() { return completedTasks.get(); }

    public double getSelectionScore() {
        double loadScore = activeTasks.get() / (double) MAX_ACTIVE_TASKS;
        double latencyScore = normalize(latencyEwmaMs, 200.0);
        double durationScore = normalize(taskDurationEwmaMs, 5_000.0);

        long totalObserved = completedTasks.get() + failedTasks.get();
        double failureRate = totalObserved == 0 ? 0.0 : failedTasks.get() / (double) totalObserved;

        // lower score means better candidate
        return (loadScore * 6.0)
                + (latencyScore * 2.0)
                + (durationScore * 1.5)
                + (failureRate * 4.0);
    }

    private long applyEwma(long previous, long sample) {
        if (previous <= 0) {
            return sample;
        }
        return Math.round((1.0 - EWMA_ALPHA) * previous + EWMA_ALPHA * sample);
    }

    private double normalize(long value, double reference) {
        if (value <= 0) {
            return 0.0;
        }
        return Math.min(5.0, value / reference);
    }

    private static class NoopTransportConnection implements TransportConnection {
        private final String nodeId;

        NoopTransportConnection(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public String nodeId() {
            return nodeId;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void send(protocol.Message message) {
        }

        @Override
        public void close() {
        }
    }
}
