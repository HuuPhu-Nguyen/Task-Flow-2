package server.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPeerRegistry implements PeerRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryPeerRegistry.class);

    private final ConcurrentHashMap<String, PeerInfo> peers = new ConcurrentHashMap<>();

    @Override
    public void register(String nodeId, PeerInfo peer) {
        peers.put(nodeId, peer);
        LOGGER.info("event=peer_registered peer_id={}", nodeId);
    }

    @Override
    public void remove(String nodeId) {
        peers.remove(nodeId);
        LOGGER.info("event=peer_removed peer_id={}", nodeId);
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
