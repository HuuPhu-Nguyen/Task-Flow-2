package server.registry;

import java.util.Collection;
import java.util.List;

public interface PeerRegistry {
    void register(String nodeId, PeerInfo peer);

    boolean registerIfAbsent(String nodeId, PeerInfo peer);

    void remove(String nodeId);

    void updateHeartbeat(String nodeId);

    default void updateHeartbeat(String nodeId, Collection<String> supportedTaskTypes) {
        updateHeartbeat(nodeId);
        PeerInfo peer = get(nodeId);
        if (peer != null) {
            peer.setSupportedTaskTypes(supportedTaskTypes);
        }
    }

    default void updateMetricsSnapshot(String nodeId) {
    }

    Collection<PeerInfo> getAllPeers();

    PeerInfo get(String nodeId);

    /**
     * Returns only live peers that support the requested task type and have
     * scheduler capacity under the supplied limit.
     */
    List<PeerInfo> getAvailablePeers(String taskType, int maxTasksPerPeer);

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
    void reserveTaskCapacity(PeerInfo peer);

    /**
     * Releases one capacity slot using the exact peer object selected at T1.
     */
    void releaseTaskCapacity(PeerInfo peer);

    /**
     * Releases one capacity slot after a result/failure identified its worker.
     */
    void releaseTaskCapacity(String nodeId);
}
