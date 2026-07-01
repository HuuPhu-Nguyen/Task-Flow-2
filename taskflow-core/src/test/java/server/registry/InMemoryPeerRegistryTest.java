package server.registry;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryPeerRegistryTest {

    @Test
    void registerIfAbsentRejectsDuplicateActivePeerId() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo first = new PeerInfo("peer-1");
        PeerInfo duplicate = new PeerInfo("peer-1");

        assertTrue(registry.registerIfAbsent("peer-1", first));
        assertFalse(registry.registerIfAbsent("peer-1", duplicate));

        assertSame(first, registry.get("peer-1"));
        assertEquals(1, registry.getAllPeers().size());
    }

    @Test
    void registryKeysUseSanitizedPeerIds() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo("peer with spaces");

        assertTrue(registry.registerIfAbsent("peer with spaces", peer));

        assertSame(peer, registry.get("peer_with_spaces"));
    }

    @Test
    void registrationPersistsConnectedPeerMetadata() {
        RecordingPeerRegistryStore store = new RecordingPeerRegistryStore();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry(store);
        PeerInfo peer = new PeerInfo(
                "peer-1",
                server.scheduler.SchedulerConfig.defaults(),
                List.of("image_conversion"),
                PeerTransport.TCP
        );

        assertTrue(registry.registerIfAbsent("peer-1", peer));

        PeerRegistryRecord record = store.last("peer-1");
        assertNotNull(record);
        assertEquals("peer-1", record.peerId());
        assertEquals("TCP_PEER", record.runtimeType());
        assertEquals(PeerTransport.TCP, record.transport());
        assertEquals(PeerStatus.CONNECTED, record.status());
        assertEquals(java.util.Set.of("IMAGE_CONVERSION"), record.supportedTaskTypes());
        assertTrue(record.firstSeenAtMillis() > 0L);
        assertTrue(record.lastHeartbeatAtMillis() >= record.firstSeenAtMillis());
        assertEquals(0L, record.lastDisconnectedAtMillis());
    }

    @Test
    void heartbeatPersistsUpdatedCapabilities() {
        RecordingPeerRegistryStore store = new RecordingPeerRegistryStore();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry(store);
        PeerInfo peer = new PeerInfo("peer-1");
        registry.registerIfAbsent("peer-1", peer);
        PeerRegistryRecord registered = store.last("peer-1");

        registry.updateHeartbeat("peer-1", List.of("text_analysis", " image_conversion "));

        PeerRegistryRecord heartbeat = store.last("peer-1");
        assertNotNull(heartbeat);
        assertEquals(PeerStatus.CONNECTED, heartbeat.status());
        assertEquals(java.util.Set.of("IMAGE_CONVERSION", "TEXT_ANALYSIS"), heartbeat.supportedTaskTypes());
        assertTrue(heartbeat.lastHeartbeatAtMillis() >= registered.lastHeartbeatAtMillis());
    }

    @Test
    void removalPersistsDisconnectedStatus() {
        RecordingPeerRegistryStore store = new RecordingPeerRegistryStore();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry(store);
        registry.registerIfAbsent("peer-1", new PeerInfo("peer-1"));

        registry.remove("peer-1");

        PeerRegistryRecord disconnected = store.last("peer-1");
        assertNotNull(disconnected);
        assertEquals(PeerStatus.DISCONNECTED, disconnected.status());
        assertTrue(disconnected.lastDisconnectedAtMillis() > 0L);
        assertNull(registry.get("peer-1"));
    }

    @Test
    void duplicateActivePeerDoesNotOverwritePersistedRecord() {
        RecordingPeerRegistryStore store = new RecordingPeerRegistryStore();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry(store);
        PeerInfo first = new PeerInfo(
                "peer-1",
                server.scheduler.SchedulerConfig.defaults(),
                List.of("image_conversion"),
                PeerTransport.TCP
        );
        PeerInfo duplicate = new PeerInfo(
                "peer-1",
                server.scheduler.SchedulerConfig.defaults(),
                List.of("text_analysis"),
                PeerTransport.RABBITMQ
        );

        assertTrue(registry.registerIfAbsent("peer-1", first));
        assertFalse(registry.registerIfAbsent("peer-1", duplicate));

        assertEquals(1, store.records.size());
        PeerRegistryRecord record = store.last("peer-1");
        assertEquals(PeerTransport.TCP, record.transport());
        assertEquals(java.util.Set.of("IMAGE_CONVERSION"), record.supportedTaskTypes());
    }

    @Test
    void metricsSnapshotPersistsCurrentSchedulerCounters() {
        RecordingPeerRegistryStore store = new RecordingPeerRegistryStore();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry(store);
        PeerInfo peer = new PeerInfo("peer-1");
        registry.registerIfAbsent("peer-1", peer);
        peer.recordTaskSuccess(250L);
        peer.recordTaskFailure();
        peer.updateLatency(30L);

        registry.updateMetricsSnapshot("peer-1");

        PeerMetricsSnapshot metrics = store.last("peer-1").metricsSnapshot();
        assertEquals(1L, metrics.completedTasks());
        assertEquals(1L, metrics.failedTasks());
        assertEquals(30L, metrics.latencyEwmaMs());
        assertEquals(250L, metrics.taskDurationEwmaMs());
    }

    private static final class RecordingPeerRegistryStore implements PeerRegistryStore {
        private final List<PeerRegistryRecord> records = new ArrayList<>();

        @Override
        public boolean upsertPeerRecord(PeerRegistryRecord record) {
            records.add(record);
            return true;
        }

        @Override
        public List<PeerRegistryRecord> loadPeerRecords() {
            return List.copyOf(records);
        }

        private PeerRegistryRecord last(String peerId) {
            for (int index = records.size() - 1; index >= 0; index--) {
                PeerRegistryRecord record = records.get(index);
                if (record.peerId().equals(peerId)) {
                    return record;
                }
            }
            return null;
        }
    }
}
