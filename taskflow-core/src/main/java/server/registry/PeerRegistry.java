package server.registry;

import java.util.Collection;
import java.util.List;

import protocol.PongMessage;

public interface PeerRegistry {
    void register(String nodeId, PeerInfo peer);

    boolean registerIfAbsent(String nodeId, PeerInfo peer);

    void remove(String nodeId);

    void updateHeartbeat(String nodeId);

    default void updateHeartbeat(String nodeId, Collection<String> supportedTaskTypes) {
        updateHeartbeat(nodeId);
        PeerInfo peer = get(nodeId);
        if (peer != null) {
            peer.applyLegacyHeartbeat(supportedTaskTypes);
        }
    }

    PeerInfo.CapacitySnapshotOutcome updateHeartbeat(String nodeId, PongMessage pong);

    default void updateMetricsSnapshot(String nodeId) {
    }

    Collection<PeerInfo> getAllPeers();

    PeerInfo get(String nodeId);

    /**
     * Returns only live peers that support the requested task type and have
     * enough advertised and locally projected capacity for the task cost.
     */
    List<PeerInfo> getAvailablePeers(String taskType, int capacityUnitCost);

    /**
     * Monotonic process-local signal changed whenever compatible scheduling
     * capacity may have increased.
     */
    default long capacityAvailabilityVersion() {
        return 0L;
    }

    /**
     * Projects one already-authoritative assignment into worker capacity.
     */
    boolean reserveTaskCapacity(AssignmentCapacityReservation reservation);

    /**
     * Releases one capacity slot using the exact peer object selected at T1.
     */
    boolean releaseTaskCapacity(
            AssignmentCapacityReservation reservation,
            String releaseReason
    );

    boolean capacityProjectionValid();

    default CapacityMetricsSnapshot capacityMetricsSnapshot() {
        return CapacityMetricsSnapshot.EMPTY;
    }
}
