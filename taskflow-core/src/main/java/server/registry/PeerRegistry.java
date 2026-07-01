package server.registry;

import java.util.Collection;

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
}
