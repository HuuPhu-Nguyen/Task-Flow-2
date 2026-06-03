package server.registry;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPeerRegistry implements PeerRegistry {

    private final ConcurrentHashMap<String, PeerInfo> peers = new ConcurrentHashMap<>();

    @Override
    public void register(String nodeId, PeerInfo peer) {
        peers.put(nodeId, peer);
        System.out.println("Registered peer: " + nodeId);
    }

    @Override
    public void remove(String nodeId) {
        peers.remove(nodeId);
        System.out.println("Removed peer: " + nodeId);
    }

    @Override
    public void updateHeartbeat(String nodeId) {
        PeerInfo peer = peers.get(nodeId);
        if (peer != null) {
            peer.updateHeartbeatReceivedNow();
        }
    }

    @Override
    public Collection<PeerInfo> getAllPeers() {
        return peers.values();
    }

    @Override
    public PeerInfo get(String nodeId) {
        return peers.get(nodeId);
    }
}
