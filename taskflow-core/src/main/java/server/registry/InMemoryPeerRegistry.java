package server.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.PeerIdentity;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPeerRegistry implements PeerRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryPeerRegistry.class);

    private final ConcurrentHashMap<String, PeerInfo> peers = new ConcurrentHashMap<>();
    private final PeerRegistryStore store;

    public InMemoryPeerRegistry() {
        this(null);
    }

    public InMemoryPeerRegistry(PeerRegistryStore store) {
        this.store = store;
    }

    @Override
    public void register(String nodeId, PeerInfo peer) {
        String peerId = PeerIdentity.require(nodeId);
        verifyPeerInfo(peerId, peer);
        peers.put(peerId, peer);
        persistPeer(peer, PeerStatus.CONNECTED, 0L);
        LOGGER.info("event=peer_registered peer_id={}", peerId);
    }

    @Override
    public boolean registerIfAbsent(String nodeId, PeerInfo peer) {
        String peerId = PeerIdentity.require(nodeId);
        verifyPeerInfo(peerId, peer);
        PeerInfo existing = peers.putIfAbsent(peerId, peer);
        if (existing == null) {
            persistPeer(peer, PeerStatus.CONNECTED, 0L);
            LOGGER.info("event=peer_registered peer_id={}", peerId);
            return true;
        }
        LOGGER.warn("event=peer_duplicate_rejected peer_id={}", peerId);
        return false;
    }

    @Override
    public void remove(String nodeId) {
        String peerId = PeerIdentity.require(nodeId);
        PeerInfo removed = peers.remove(peerId);
        if (removed != null) {
            persistPeer(removed, PeerStatus.DISCONNECTED, System.currentTimeMillis());
        }
        LOGGER.info("event=peer_removed peer_id={}", peerId);
    }

    @Override
    public void updateHeartbeat(String nodeId) {
        PeerInfo peer = peers.get(PeerIdentity.require(nodeId));
        if (peer != null) {
            peer.updateHeartbeatReceivedNow();
            persistPeer(peer, PeerStatus.CONNECTED, 0L);
        }
    }

    @Override
    public void updateHeartbeat(String nodeId, Collection<String> supportedTaskTypes) {
        PeerInfo peer = peers.get(PeerIdentity.require(nodeId));
        if (peer != null) {
            peer.updateHeartbeatReceivedNow();
            peer.setSupportedTaskTypes(supportedTaskTypes);
            persistPeer(peer, PeerStatus.CONNECTED, 0L);
        }
    }

    @Override
    public void updateMetricsSnapshot(String nodeId) {
        PeerInfo peer = peers.get(PeerIdentity.require(nodeId));
        if (peer != null) {
            persistPeer(peer, PeerStatus.CONNECTED, 0L);
        }
    }

    @Override
    public Collection<PeerInfo> getAllPeers() {
        return peers.values();
    }

    @Override
    public PeerInfo get(String nodeId) {
        return peers.get(PeerIdentity.require(nodeId));
    }

    private void verifyPeerInfo(String nodeId, PeerInfo peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer info is required.");
        }
        if (!nodeId.equals(peer.getNodeId())) {
            throw new IllegalArgumentException("Peer registry key must match PeerInfo node id.");
        }
    }

    private void persistPeer(PeerInfo peer, PeerStatus status, long lastDisconnectedAtMillis) {
        if (store == null) {
            return;
        }
        boolean stored = store.upsertPeerRecord(PeerRegistryRecord.fromPeer(
                peer,
                status,
                lastDisconnectedAtMillis
        ));
        if (!stored) {
            LOGGER.warn("event=peer_registry_persist_failed peer_id={} status={}",
                    peer.getNodeId(), status);
        }
    }
}
