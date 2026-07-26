package server.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.PeerIdentity;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPeerRegistry implements PeerRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryPeerRegistry.class);

    private final ConcurrentHashMap<String, PeerInfo> peers = new ConcurrentHashMap<>();
    private final Map<String, LinkedHashSet<String>> capablePeerIdsByTaskType = new HashMap<>();
    private final Map<String, LinkedHashSet<String>> availablePeerIdsByTaskType = new HashMap<>();
    private final PeerRegistryStore store;
    private int capacityIndexLimit = -1;
    private long capacityAvailabilityVersion;

    public InMemoryPeerRegistry() {
        this(null);
    }

    public InMemoryPeerRegistry(PeerRegistryStore store) {
        this.store = store;
    }

    @Override
    public synchronized void register(String nodeId, PeerInfo peer) {
        String peerId = PeerIdentity.require(nodeId);
        verifyPeerInfo(peerId, peer);
        PeerInfo previous = peers.put(peerId, peer);
        if (previous != null) {
            removeFromIndexes(previous);
        }
        addToIndexes(peer);
        capacityAvailabilityVersion++;
        persistPeer(peer, PeerStatus.CONNECTED, 0L);
        LOGGER.info("event=peer_registered peer_id={}", peerId);
    }

    @Override
    public synchronized boolean registerIfAbsent(String nodeId, PeerInfo peer) {
        String peerId = PeerIdentity.require(nodeId);
        verifyPeerInfo(peerId, peer);
        PeerInfo existing = peers.putIfAbsent(peerId, peer);
        if (existing == null) {
            addToIndexes(peer);
            capacityAvailabilityVersion++;
            persistPeer(peer, PeerStatus.CONNECTED, 0L);
            LOGGER.info("event=peer_registered peer_id={}", peerId);
            return true;
        }
        LOGGER.warn("event=peer_duplicate_rejected peer_id={}", peerId);
        return false;
    }

    @Override
    public synchronized void remove(String nodeId) {
        String peerId = PeerIdentity.require(nodeId);
        PeerInfo removed = peers.remove(peerId);
        if (removed != null) {
            removeFromIndexes(removed);
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
    public synchronized void updateHeartbeat(String nodeId, Collection<String> supportedTaskTypes) {
        PeerInfo peer = peers.get(PeerIdentity.require(nodeId));
        if (peer != null) {
            LinkedHashSet<String> previousTaskTypes =
                    new LinkedHashSet<>(peer.getSupportedTaskTypes());
            removeFromIndexes(peer);
            peer.updateHeartbeatReceivedNow();
            peer.setSupportedTaskTypes(supportedTaskTypes);
            addToIndexes(peer);
            if (!previousTaskTypes.equals(new LinkedHashSet<>(peer.getSupportedTaskTypes()))) {
                capacityAvailabilityVersion++;
            }
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
        return List.copyOf(peers.values());
    }

    @Override
    public PeerInfo get(String nodeId) {
        return peers.get(PeerIdentity.require(nodeId));
    }

    @Override
    public synchronized List<PeerInfo> getAvailablePeers(String taskType, int maxTasksPerPeer) {
        if (maxTasksPerPeer <= 0) {
            throw new IllegalArgumentException("maxTasksPerPeer must be positive.");
        }
        if (capacityIndexLimit != maxTasksPerPeer) {
            rebuildAvailableCapacityIndex(maxTasksPerPeer);
        }
        LinkedHashSet<String> peerIds = availablePeerIdsByTaskType.get(normalizeTaskType(taskType));
        if (peerIds == null || peerIds.isEmpty()) {
            return List.of();
        }
        return peerIds.stream()
                .map(peers::get)
                .filter(peer -> peer != null)
                .sorted(Comparator.comparingDouble(PeerInfo::getSelectionScore))
                .toList();
    }

    @Override
    public synchronized long capacityAvailabilityVersion() {
        return capacityAvailabilityVersion;
    }

    @Override
    public synchronized void reserveTaskCapacity(PeerInfo peer) {
        if (peer == null) {
            return;
        }
        peer.incrementTasks();
        refreshAvailableCapacity(peer);
    }

    @Override
    public synchronized void releaseTaskCapacity(PeerInfo peer) {
        if (peer == null) {
            return;
        }
        int activeTasks = peer.getActiveTasks();
        peer.decrementTasks();
        refreshAvailableCapacity(peer);
        if (peer.getActiveTasks() < activeTasks) {
            capacityAvailabilityVersion++;
        }
    }

    @Override
    public synchronized void releaseTaskCapacity(String nodeId) {
        PeerInfo peer = peers.get(PeerIdentity.require(nodeId));
        if (peer == null) {
            return;
        }
        int activeTasks = peer.getActiveTasks();
        peer.decrementTasks();
        refreshAvailableCapacity(peer);
        if (peer.getActiveTasks() < activeTasks) {
            capacityAvailabilityVersion++;
        }
    }

    private void verifyPeerInfo(String nodeId, PeerInfo peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer info is required.");
        }
        if (!nodeId.equals(peer.getNodeId())) {
            throw new IllegalArgumentException("Peer registry key must match PeerInfo node id.");
        }
    }

    private void addToIndexes(PeerInfo peer) {
        for (String taskType : peer.getSupportedTaskTypes()) {
            capablePeerIdsByTaskType
                    .computeIfAbsent(taskType, ignored -> new LinkedHashSet<>())
                    .add(peer.getNodeId());
        }
        refreshAvailableCapacity(peer);
    }

    private void removeFromIndexes(PeerInfo peer) {
        for (String taskType : peer.getSupportedTaskTypes()) {
            removePeerId(capablePeerIdsByTaskType, taskType, peer.getNodeId());
            removePeerId(availablePeerIdsByTaskType, taskType, peer.getNodeId());
        }
    }

    private void refreshAvailableCapacity(PeerInfo peer) {
        if (capacityIndexLimit <= 0 || peers.get(peer.getNodeId()) != peer) {
            return;
        }
        boolean available = peer.getActiveTasks() < capacityIndexLimit;
        for (String taskType : peer.getSupportedTaskTypes()) {
            if (available) {
                availablePeerIdsByTaskType
                        .computeIfAbsent(taskType, ignored -> new LinkedHashSet<>())
                        .add(peer.getNodeId());
            } else {
                removePeerId(availablePeerIdsByTaskType, taskType, peer.getNodeId());
            }
        }
    }

    private void rebuildAvailableCapacityIndex(int maxTasksPerPeer) {
        capacityIndexLimit = maxTasksPerPeer;
        availablePeerIdsByTaskType.clear();
        for (Map.Entry<String, LinkedHashSet<String>> entry : capablePeerIdsByTaskType.entrySet()) {
            for (String peerId : entry.getValue()) {
                PeerInfo peer = peers.get(peerId);
                if (peer != null && peer.getActiveTasks() < maxTasksPerPeer) {
                    availablePeerIdsByTaskType
                            .computeIfAbsent(entry.getKey(), ignored -> new LinkedHashSet<>())
                            .add(peerId);
                }
            }
        }
    }

    private static void removePeerId(Map<String, LinkedHashSet<String>> index,
                                     String taskType,
                                     String peerId) {
        LinkedHashSet<String> peerIds = index.get(taskType);
        if (peerIds == null) {
            return;
        }
        peerIds.remove(peerId);
        if (peerIds.isEmpty()) {
            index.remove(taskType);
        }
    }

    private static String normalizeTaskType(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            return "";
        }
        return taskType.trim().toUpperCase(Locale.ROOT);
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
