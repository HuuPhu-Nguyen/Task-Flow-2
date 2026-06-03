package server.registry;

import java.util.Collection;

public interface PeerRegistry {
    void register(String nodeId, PeerInfo peer);

    void remove(String nodeId);

    void updateHeartbeat(String nodeId);

    Collection<PeerInfo> getAllPeers();

    PeerInfo get(String nodeId);
}
