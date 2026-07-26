package server.registry;

import org.junit.jupiter.api.Test;
import protocol.PongMessage;

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
                PeerTransport.RABBITMQ
        );

        assertTrue(registry.registerIfAbsent("peer-1", peer));

        PeerRegistryRecord record = store.last("peer-1");
        assertNotNull(record);
        assertEquals("peer-1", record.peerId());
        assertEquals("RABBITMQ_PEER", record.runtimeType());
        assertEquals(PeerTransport.RABBITMQ, record.transport());
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
                PeerTransport.RABBITMQ
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
        assertEquals(PeerTransport.RABBITMQ, record.transport());
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

    @Test
    void capacityEligibilityIsKeyedByTaskTypeAndTracksExactReservations() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo textPeer = capacityPeer("text-peer", "TEXT_ANALYSIS");
        PeerInfo imagePeer = capacityPeer("image-peer", "IMAGE_CONVERSION");
        registry.register(textPeer.getNodeId(), textPeer);
        registry.register(imagePeer.getNodeId(), imagePeer);

        assertEquals(List.of(textPeer), registry.getAvailablePeers("TEXT_ANALYSIS", 1));
        assertEquals(List.of(imagePeer), registry.getAvailablePeers("image_conversion", 1));

        AssignmentCapacityReservation reservation = reservation(
                "assignment-text",
                textPeer.getNodeId(),
                "TEXT_ANALYSIS"
        );
        registry.reserveTaskCapacity(reservation);

        assertEquals(List.of(), registry.getAvailablePeers("text_analysis", 1));
        assertEquals(List.of(imagePeer), registry.getAvailablePeers("image_conversion", 1));

        registry.releaseTaskCapacity(reservation, "TEST_COMPLETE");

        assertEquals(List.of(textPeer), registry.getAvailablePeers("text_analysis", 1));

        registry.updateHeartbeat(
                textPeer.getNodeId(),
                capacityPong("text-peer", "VIDEO_TRANSCODE", 2L)
        );

        assertEquals(List.of(), registry.getAvailablePeers("text_analysis", 1));
        assertEquals(List.of(textPeer), registry.getAvailablePeers("video_transcode", 1));
    }

    @Test
    void availabilityVersionChangesOnlyWhenCapacityMayIncrease() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = capacityPeer("peer-1", "TEXT_ANALYSIS");

        assertEquals(0L, registry.capacityAvailabilityVersion());
        registry.register(peer.getNodeId(), peer);
        assertEquals(1L, registry.capacityAvailabilityVersion());

        AssignmentCapacityReservation reservation = reservation(
                "assignment-1",
                peer.getNodeId(),
                "TEXT_ANALYSIS"
        );
        registry.reserveTaskCapacity(reservation);
        assertEquals(1L, registry.capacityAvailabilityVersion());

        registry.releaseTaskCapacity(reservation, "TEST_COMPLETE");
        assertEquals(2L, registry.capacityAvailabilityVersion());

        registry.updateHeartbeat(
                peer.getNodeId(),
                capacityPong(peer.getNodeId(), "TEXT_ANALYSIS", 2L)
        );
        assertEquals(3L, registry.capacityAvailabilityVersion());

        registry.updateHeartbeat(
                peer.getNodeId(),
                capacityPong(peer.getNodeId(), "VIDEO_TRANSCODE", 3L)
        );
        assertEquals(4L, registry.capacityAvailabilityVersion());
    }

    @Test
    void legacyPongPreservesLivenessButClearsSchedulingCapacity() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo("peer-legacy");
        registry.register(peer.getNodeId(), peer);
        registry.updateHeartbeat(
                peer.getNodeId(),
                capacityPong(peer.getNodeId(), "TEXT_ANALYSIS", 1L)
        );

        PeerInfo.CapacitySnapshotOutcome outcome = registry.updateHeartbeat(
                peer.getNodeId(),
                new PongMessage(
                        peer.getNodeId(),
                        "2026-07-26T00:00:01Z",
                        List.of("TEXT_ANALYSIS")
                )
        );

        assertEquals(PeerInfo.CapacitySnapshotOutcome.INCOMPATIBLE, outcome);
        assertEquals(
                "CAPACITY_PROTOCOL_UNSUPPORTED",
                peer.getCapacityIneligibilityReason()
        );
        assertEquals(
                PeerInfo.CapacitySnapshotOutcome.INSTANCE_CONFLICT,
                registry.updateHeartbeat(
                        peer.getNodeId(),
                        capacityPongWith(
                                peer.getNodeId(),
                                "550e8400-e29b-41d4-a716-446655440100",
                                "TEXT_ANALYSIS",
                                2L,
                                1,
                                1,
                                1
                        )
                )
        );
        assertEquals(List.of(), registry.getAvailablePeers("TEXT_ANALYSIS", 1));
    }

    @Test
    void staleSnapshotAndConflictingLiveInstanceCannotOverwriteAcceptedCapacity() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo("peer-sequence");
        registry.register(peer.getNodeId(), peer);
        registry.updateHeartbeat(
                peer.getNodeId(),
                capacityPong(peer.getNodeId(), "TEXT_ANALYSIS", 2L)
        );

        PongMessage stale = capacityPongWith(
                peer.getNodeId(),
                "550e8400-e29b-41d4-a716-446655440099",
                "TEXT_ANALYSIS",
                1L,
                1,
                0,
                1
        );
        PongMessage conflict = capacityPongWith(
                peer.getNodeId(),
                "550e8400-e29b-41d4-a716-446655440100",
                "TEXT_ANALYSIS",
                3L,
                1,
                0,
                1
        );

        assertEquals(
                PeerInfo.CapacitySnapshotOutcome.STALE,
                registry.updateHeartbeat(peer.getNodeId(), stale)
        );
        assertEquals(
                PeerInfo.CapacitySnapshotOutcome.INSTANCE_CONFLICT,
                registry.updateHeartbeat(peer.getNodeId(), conflict)
        );
        assertEquals(
                List.of(peer),
                registry.getAvailablePeers("TEXT_ANALYSIS", 1)
        );
    }

    @Test
    void weightedUnitsAndTypeConcurrencyAreBothHardEligibilityFilters() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo("peer-weighted");
        registry.register(peer.getNodeId(), peer);
        registry.updateHeartbeat(
                peer.getNodeId(),
                new PongMessage(
                        peer.getNodeId(),
                        "2026-07-26T00:00:00Z",
                        List.of("TEXT_ANALYSIS", "VIDEO_TRANSCODING"),
                        "550e8400-e29b-41d4-a716-446655440099",
                        1L,
                        8,
                        8,
                        java.util.Map.of(
                                "TEXT_ANALYSIS", 8,
                                "VIDEO_TRANSCODING", 2
                        )
                )
        );
        AssignmentCapacityReservation video = new AssignmentCapacityReservation(
                "job-video",
                "task-video",
                1,
                "assignment-video",
                peer.getNodeId(),
                "VIDEO_TRANSCODING",
                8
        );
        AssignmentCapacityReservation text = new AssignmentCapacityReservation(
                "job-text",
                "task-text",
                1,
                "assignment-text",
                peer.getNodeId(),
                "TEXT_ANALYSIS",
                1
        );

        assertEquals(
                List.of(peer),
                registry.getAvailablePeers("VIDEO_TRANSCODING", 8)
        );
        assertTrue(registry.reserveTaskCapacity(video));
        assertEquals(
                List.of(),
                registry.getAvailablePeers("VIDEO_TRANSCODING", 8)
        );
        assertEquals(
                List.of(),
                registry.getAvailablePeers("TEXT_ANALYSIS", 1)
        );
        assertTrue(registry.releaseTaskCapacity(video, "TEST_COMPLETE"));

        assertTrue(registry.reserveTaskCapacity(text));
        assertEquals(List.of(peer), registry.getAvailablePeers("TEXT_ANALYSIS", 1));
        assertEquals(
                List.of(),
                registry.getAvailablePeers("VIDEO_TRANSCODING", 8)
        );
        assertTrue(registry.releaseTaskCapacity(text, "TEST_COMPLETE"));
        assertEquals(
                List.of(peer),
                registry.getAvailablePeers("VIDEO_TRANSCODING", 8)
        );
    }

    @Test
    void capacityMetricsCountLifecycleWithoutDoubleCountingExactReserveReplay() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo("peer-capacity-metrics");
        registry.register(peer.getNodeId(), peer);
        PongMessage capacity = capacityPong(peer.getNodeId(), "TEXT_ANALYSIS", 1L);

        assertEquals(
                PeerInfo.CapacitySnapshotOutcome.ACCEPTED,
                registry.updateHeartbeat(peer.getNodeId(), capacity)
        );
        assertEquals(
                PeerInfo.CapacitySnapshotOutcome.STALE,
                registry.updateHeartbeat(peer.getNodeId(), capacity)
        );

        AssignmentCapacityReservation reservation = reservation(
                "assignment-capacity-metrics",
                peer.getNodeId(),
                "TEXT_ANALYSIS"
        );
        assertTrue(registry.reserveTaskCapacity(reservation));
        assertTrue(registry.reserveTaskCapacity(reservation));
        assertEquals(
                new CapacityMetricsSnapshot(1L, 1L, 0L, 1L, 0L, 0L, 1L, 1L),
                registry.capacityMetricsSnapshot()
        );

        assertTrue(registry.releaseTaskCapacity(reservation, "TEST_COMPLETE"));
        assertEquals(
                PeerInfo.CapacitySnapshotOutcome.INCOMPATIBLE,
                registry.updateHeartbeat(
                        peer.getNodeId(),
                        new PongMessage(
                                peer.getNodeId(),
                                "2026-07-26T00:00:01Z",
                                List.of("TEXT_ANALYSIS")
                        )
                )
        );
        assertEquals(
                new CapacityMetricsSnapshot(1L, 1L, 1L, 1L, 1L, 0L, 0L, 0L),
                registry.capacityMetricsSnapshot()
        );
    }

    @Test
    void reservationIdentityMismatchDisablesFurtherDispatch() {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = capacityPeer("peer-invalid", "TEXT_ANALYSIS");
        registry.register(peer.getNodeId(), peer);
        AssignmentCapacityReservation current = reservation(
                "assignment-invalid",
                peer.getNodeId(),
                "TEXT_ANALYSIS"
        );
        AssignmentCapacityReservation mismatched =
                new AssignmentCapacityReservation(
                        current.jobId(),
                        "different-task",
                        current.attemptNumber(),
                        current.assignmentId(),
                        current.workerId(),
                        current.taskType(),
                        current.capacityUnitCost()
                );

        assertTrue(registry.reserveTaskCapacity(current));
        assertFalse(registry.releaseTaskCapacity(mismatched, "TEST_MISMATCH"));
        assertFalse(registry.capacityProjectionValid());
        assertEquals(1L, registry.capacityMetricsSnapshot().projectionFailures());
        assertEquals(
                List.of(),
                registry.getAvailablePeers("TEXT_ANALYSIS", 1)
        );
    }

    private static PeerInfo capacityPeer(String peerId, String taskType) {
        PeerInfo peer = new PeerInfo(peerId);
        peer.applyCapacityHeartbeat(capacityPong(peerId, taskType, 1L));
        return peer;
    }

    private static PongMessage capacityPong(String peerId,
                                            String taskType,
                                            long sequence) {
        return capacityPongWith(
                peerId,
                "550e8400-e29b-41d4-a716-446655440099",
                taskType,
                sequence,
                1,
                1,
                1
        );
    }

    private static PongMessage capacityPongWith(String peerId,
                                                String instanceId,
                                                String taskType,
                                                long sequence,
                                                int total,
                                                int available,
                                                int typeLimit) {
        return new PongMessage(
                peerId,
                "2026-07-26T00:00:00Z",
                List.of(taskType),
                instanceId,
                sequence,
                total,
                available,
                java.util.Map.of(taskType, typeLimit)
        );
    }

    private static AssignmentCapacityReservation reservation(
            String assignmentId,
            String workerId,
            String taskType
    ) {
        return new AssignmentCapacityReservation(
                "job-1",
                "task-1",
                1,
                assignmentId,
                workerId,
                taskType,
                1
        );
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
