package server.registry;

import protocol.PeerIdentity;
import protocol.MessageValidator;
import protocol.PongMessage;
import protocol.ProtocolVersions;
import server.scheduler.SchedulerConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PeerInfo {
    public enum CapacitySnapshotOutcome {
        ACCEPTED,
        STALE,
        INCOMPATIBLE,
        INSTANCE_CONFLICT
    }

    private final String nodeId;
    private final SchedulerConfig config;
    private final PeerTransport transport;
    private final String runtimeType;
    private volatile Set<String> supportedTaskTypes;
    private volatile CapacitySnapshot capacitySnapshot;
    private volatile String capacityIneligibilityReason = "CAPACITY_NOT_ADVERTISED";
    private String acceptedExecutorInstanceId;
    private long acceptedCapacitySnapshotSequence;

    private final long firstSeenAtMillis;
    private final AtomicLong lastHeartbeatReceivedAtMillis;
    private final AtomicInteger activeTasks = new AtomicInteger(0);

    private volatile long latencyEwmaMs;         // Smoothed RTT
    private volatile long taskDurationEwmaMs;    // Smoothed task runtime
    private final AtomicLong completedTasks = new AtomicLong(0);
    private final AtomicLong failedTasks = new AtomicLong(0);

    public PeerInfo(String nodeId) {
        this(nodeId, SchedulerConfig.defaults(), Set.of());
    }

    public PeerInfo(String nodeId, SchedulerConfig config) {
        this(nodeId, config, Set.of());
    }

    public PeerInfo(String nodeId, SchedulerConfig config, Collection<String> supportedTaskTypes) {
        this(nodeId, config, supportedTaskTypes, PeerTransport.UNKNOWN);
    }

    public PeerInfo(String nodeId,
                    SchedulerConfig config,
                    Collection<String> supportedTaskTypes,
                    PeerTransport transport) {
        this(nodeId, config, supportedTaskTypes, transport, null);
    }

    public PeerInfo(String nodeId,
                    SchedulerConfig config,
                    Collection<String> supportedTaskTypes,
                    PeerTransport transport,
                    String runtimeType) {
        this.nodeId = PeerIdentity.require(nodeId);
        this.config = config == null ? SchedulerConfig.defaults() : config;
        this.transport = transport == null ? PeerTransport.UNKNOWN : transport;
        this.runtimeType = normalizeRuntimeType(runtimeType, this.transport);
        this.supportedTaskTypes = normalizeTaskTypes(supportedTaskTypes);
        // Direct registration remains available to in-process scheduler users.
        // RabbitMQ participants start empty and require a validated v3 PONG.
        if (!this.supportedTaskTypes.isEmpty()) {
            this.capacitySnapshot = programmaticCapacity(this.supportedTaskTypes);
            this.capacityIneligibilityReason = "";
        }
        this.firstSeenAtMillis = System.currentTimeMillis();
        this.lastHeartbeatReceivedAtMillis = new AtomicLong(firstSeenAtMillis);
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getLastHeartbeatReceivedAtMillis() {return lastHeartbeatReceivedAtMillis.get();}

    public void updateHeartbeatReceivedNow() {lastHeartbeatReceivedAtMillis.set(System.currentTimeMillis());}

    public long getFirstSeenAtMillis() {return firstSeenAtMillis;}

    public PeerTransport getTransport() {return transport;}

    public String getRuntimeType() {return runtimeType;}

    public int getActiveTasks() {return activeTasks.get();}

    public Set<String> getSupportedTaskTypes() {
        return supportedTaskTypes;
    }

    public void setSupportedTaskTypes(Collection<String> supportedTaskTypes) {
        this.supportedTaskTypes = normalizeTaskTypes(supportedTaskTypes);
    }

    public synchronized CapacitySnapshotOutcome applyCapacityHeartbeat(PongMessage pong) {
        MessageValidator.validate(pong);
        if (pong.getProtocolVersion() < ProtocolVersions.CAPACITY_ADVERTISEMENT) {
            updateHeartbeatReceivedNow();
            setSupportedTaskTypes(pong.getSupportedTaskTypes());
            capacitySnapshot = null;
            capacityIneligibilityReason = "CAPACITY_PROTOCOL_UNSUPPORTED";
            return CapacitySnapshotOutcome.INCOMPATIBLE;
        }

        if (acceptedExecutorInstanceId != null
                && !acceptedExecutorInstanceId.equals(pong.getExecutorInstanceId())) {
            return CapacitySnapshotOutcome.INSTANCE_CONFLICT;
        }
        if (acceptedExecutorInstanceId != null
                && pong.getCapacitySnapshotSequence() <= acceptedCapacitySnapshotSequence) {
            updateHeartbeatReceivedNow();
            return CapacitySnapshotOutcome.STALE;
        }

        updateHeartbeatReceivedNow();
        setSupportedTaskTypes(pong.getSupportedTaskTypes());
        acceptedExecutorInstanceId = pong.getExecutorInstanceId();
        acceptedCapacitySnapshotSequence = pong.getCapacitySnapshotSequence();
        capacitySnapshot = new CapacitySnapshot(
                pong.getTotalCapacityUnits(),
                pong.getAvailableCapacityUnits(),
                pong.getMaxConcurrencyByTaskType()
        );
        capacityIneligibilityReason = "";
        return CapacitySnapshotOutcome.ACCEPTED;
    }

    public synchronized void applyLegacyHeartbeat(Collection<String> taskTypes) {
        updateHeartbeatReceivedNow();
        setSupportedTaskTypes(taskTypes);
        capacitySnapshot = null;
        capacityIneligibilityReason = "CAPACITY_PROTOCOL_UNSUPPORTED";
    }

    public boolean hasCapacityAdvertisement() {
        return capacitySnapshot != null;
    }

    public String getCapacityIneligibilityReason() {
        return capacityIneligibilityReason;
    }

    public long getAdvertisedTotalCapacityUnits() {
        CapacitySnapshot snapshot = capacitySnapshot;
        return snapshot == null ? 0L : snapshot.totalCapacityUnits();
    }

    public long getAdvertisedAvailableCapacityUnits() {
        CapacitySnapshot snapshot = capacitySnapshot;
        return snapshot == null ? 0L : snapshot.availableCapacityUnits();
    }

    public boolean hasCapacityFor(String taskType,
                                  int capacityUnitCost,
                                  long reservedUnits,
                                  int reservedTypeCount) {
        CapacitySnapshot snapshot = capacitySnapshot;
        if (snapshot == null || capacityUnitCost <= 0) {
            return false;
        }
        String normalizedType = normalizeTaskType(taskType);
        Integer typeLimit = snapshot.maxConcurrencyByTaskType().get(normalizedType);
        if (typeLimit == null || reservedTypeCount >= typeLimit) {
            return false;
        }
        return effectiveFreeUnits(snapshot, reservedUnits) >= capacityUnitCost;
    }

    public boolean supportsTaskType(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            return false;
        }
        return supportedTaskTypes.contains(normalizeTaskType(taskType));
    }

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

    public PeerMetricsSnapshot metricsSnapshot() {
        return new PeerMetricsSnapshot(
                completedTasks.get(),
                failedTasks.get(),
                latencyEwmaMs,
                taskDurationEwmaMs
        );
    }

    public double getSelectionScore() {
        return getSelectionScore(0L);
    }

    public double getSelectionScore(long reservedUnits) {
        CapacitySnapshot snapshot = capacitySnapshot;
        double loadScore = snapshot == null || snapshot.totalCapacityUnits() <= 0
                ? 1.0
                : (snapshot.totalCapacityUnits()
                        - effectiveFreeUnits(snapshot, reservedUnits))
                        / (double) snapshot.totalCapacityUnits();
        double latencyScore = normalize(latencyEwmaMs, config.peerScoreLatencyBaselineMillis());
        double durationScore = normalize(taskDurationEwmaMs, config.peerScoreDurationBaselineMillis());

        long totalObserved = completedTasks.get() + failedTasks.get();
        double failureRate = totalObserved == 0 ? 0.0 : failedTasks.get() / (double) totalObserved;

        // lower score means better candidate
        return (loadScore * config.peerScoreLoadWeight())
                + (latencyScore * config.peerScoreLatencyWeight())
                + (durationScore * config.peerScoreDurationWeight())
                + (failureRate * config.peerScoreFailureWeight());
    }

    private long applyEwma(long previous, long sample) {
        if (previous <= 0) {
            return sample;
        }
        return Math.round((1.0 - config.peerScoreEwmaAlpha()) * previous
                + config.peerScoreEwmaAlpha() * sample);
    }

    private double normalize(long value, double reference) {
        if (value <= 0) {
            return 0.0;
        }
        return Math.min(5.0, value / reference);
    }

    private Set<String> normalizeTaskTypes(Collection<String> taskTypes) {
        if (taskTypes == null) {
            return Set.of();
        }
        return taskTypes.stream()
                .filter(taskType -> taskType != null && !taskType.isBlank())
                .map(this::normalizeTaskType)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private String normalizeTaskType(String taskType) {
        return taskType.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeRuntimeType(String runtimeType, PeerTransport transport) {
        if (runtimeType != null && !runtimeType.isBlank()) {
            return runtimeType.trim();
        }
        return switch (transport == null ? PeerTransport.UNKNOWN : transport) {
            case RABBITMQ -> "RABBITMQ_PEER";
            case UNKNOWN -> "PEER";
        };
    }

    private static CapacitySnapshot programmaticCapacity(Set<String> taskTypes) {
        Map<String, Integer> typeLimits = taskTypes.stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        taskType -> taskType,
                        ignored -> Integer.MAX_VALUE
                ));
        return new CapacitySnapshot(
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                typeLimits
        );
    }

    private static long effectiveFreeUnits(CapacitySnapshot snapshot,
                                           long reservedUnits) {
        long totalAfterReservations = Math.max(
                0L,
                (long) snapshot.totalCapacityUnits() - reservedUnits
        );
        return Math.max(
                0L,
                Math.min(snapshot.availableCapacityUnits(), totalAfterReservations)
        );
    }

    private record CapacitySnapshot(
            int totalCapacityUnits,
            int availableCapacityUnits,
            Map<String, Integer> maxConcurrencyByTaskType
    ) {
        private CapacitySnapshot {
            maxConcurrencyByTaskType = Map.copyOf(maxConcurrencyByTaskType);
        }
    }
}
