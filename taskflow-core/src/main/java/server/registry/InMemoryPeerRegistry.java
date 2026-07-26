package server.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.PeerIdentity;
import protocol.PongMessage;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPeerRegistry implements PeerRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryPeerRegistry.class);

    private final ConcurrentHashMap<String, PeerInfo> peers = new ConcurrentHashMap<>();
    private final Map<String, LinkedHashSet<String>> capablePeerIdsByTaskType = new HashMap<>();
    private final CoordinatorCapacityLedger capacityLedger = new CoordinatorCapacityLedger();
    private final PeerRegistryStore store;
    private long capacityAvailabilityVersion;
    private boolean capacityProjectionValid = true;
    private long acceptedCapacitySnapshots;
    private long staleCapacitySnapshots;
    private long incompatibleCapacitySnapshots;
    private long capacityReservationsCreated;
    private long capacityReservationsReleased;
    private long capacityProjectionFailures;

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
            peer.applyLegacyHeartbeat(supportedTaskTypes);
            incompatibleCapacitySnapshots++;
            addToIndexes(peer);
            if (!previousTaskTypes.equals(new LinkedHashSet<>(peer.getSupportedTaskTypes()))) {
                capacityAvailabilityVersion++;
            }
            persistPeer(peer, PeerStatus.CONNECTED, 0L);
        }
    }

    @Override
    public synchronized PeerInfo.CapacitySnapshotOutcome updateHeartbeat(
            String nodeId,
            PongMessage pong
    ) {
        Objects.requireNonNull(pong, "pong");
        PeerInfo peer = peers.get(PeerIdentity.require(nodeId));
        if (peer == null) {
            return PeerInfo.CapacitySnapshotOutcome.INCOMPATIBLE;
        }
        if (!peer.getNodeId().equals(PeerIdentity.require(pong.getNodeId()))) {
            throw new IllegalArgumentException(
                    "Capacity heartbeat node id must match the registry peer."
            );
        }
        LinkedHashSet<String> previousTaskTypes =
                new LinkedHashSet<>(peer.getSupportedTaskTypes());
        boolean previouslyEligible = peer.hasCapacityAdvertisement();
        PeerInfo.CapacitySnapshotOutcome outcome = peer.applyCapacityHeartbeat(pong);
        if (outcome == PeerInfo.CapacitySnapshotOutcome.INSTANCE_CONFLICT) {
            staleCapacitySnapshots++;
            LOGGER.warn(
                    "event=executor_capacity_snapshot_stale_ignored peer_id={} "
                            + "executor_instance_id={} capacity_snapshot_sequence={} "
                            + "reason=executor_instance_conflict",
                    nodeId,
                    pong.getExecutorInstanceId(),
                    pong.getCapacitySnapshotSequence()
            );
            return outcome;
        }
        removeFromIndexes(peer);
        addToIndexes(peer);
        boolean eligibilityChanged = previouslyEligible != peer.hasCapacityAdvertisement()
                || !previousTaskTypes.equals(new LinkedHashSet<>(peer.getSupportedTaskTypes()))
                || outcome == PeerInfo.CapacitySnapshotOutcome.ACCEPTED;
        if (eligibilityChanged) {
            capacityAvailabilityVersion++;
        }
        persistPeer(peer, PeerStatus.CONNECTED, 0L);
        String event = switch (outcome) {
            case ACCEPTED -> {
                acceptedCapacitySnapshots++;
                yield "executor_capacity_snapshot_accepted";
            }
            case STALE -> {
                staleCapacitySnapshots++;
                yield "executor_capacity_snapshot_stale_ignored";
            }
            case INCOMPATIBLE -> {
                incompatibleCapacitySnapshots++;
                yield "executor_capacity_protocol_incompatible";
            }
            case INSTANCE_CONFLICT -> throw new IllegalStateException("Handled above.");
        };
        LOGGER.info(
                "event={} peer_id={} executor_instance_id={} capacity_snapshot_sequence={} "
                        + "total_capacity_units={} available_capacity_units={} reason={}",
                event,
                nodeId,
                pong.getExecutorInstanceId(),
                pong.getCapacitySnapshotSequence(),
                pong.getTotalCapacityUnits(),
                pong.getAvailableCapacityUnits(),
                peer.getCapacityIneligibilityReason()
        );
        return outcome;
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
    public synchronized List<PeerInfo> getAvailablePeers(String taskType, int capacityUnitCost) {
        if (capacityUnitCost <= 0) {
            throw new IllegalArgumentException("capacityUnitCost must be positive.");
        }
        if (!capacityProjectionValid) {
            return List.of();
        }
        String normalizedType = normalizeTaskType(taskType);
        LinkedHashSet<String> peerIds = capablePeerIdsByTaskType.get(normalizedType);
        if (peerIds == null || peerIds.isEmpty()) {
            return List.of();
        }
        return peerIds.stream()
                .map(peers::get)
                .filter(peer -> peer != null && peer.hasCapacityFor(
                        normalizedType,
                        capacityUnitCost,
                        capacityLedger.reservedUnits(peer.getNodeId()),
                        capacityLedger.reservedTypeCount(peer.getNodeId(), normalizedType)
                ))
                .sorted(Comparator.comparingDouble(peer -> peer.getSelectionScore(
                        capacityLedger.reservedUnits(peer.getNodeId())
                )))
                .toList();
    }

    @Override
    public synchronized long capacityAvailabilityVersion() {
        return capacityAvailabilityVersion;
    }

    @Override
    public synchronized boolean reserveTaskCapacity(
            AssignmentCapacityReservation reservation
    ) {
        Objects.requireNonNull(reservation, "reservation");
        if (!capacityProjectionValid) {
            return false;
        }
        CoordinatorCapacityLedger.ReserveOutcome outcome;
        try {
            outcome = capacityLedger.reserve(reservation);
        } catch (RuntimeException failure) {
            invalidateCapacityProjection(
                    "reserve_exception_" + failure.getClass().getSimpleName(),
                    reservation,
                    failure
            );
            return false;
        }
        if (outcome == CoordinatorCapacityLedger.ReserveOutcome.ALREADY_RESERVED) {
            return true;
        }
        if (outcome == CoordinatorCapacityLedger.ReserveOutcome.IDENTITY_MISMATCH) {
            invalidateCapacityProjection("reserve_" + outcome, reservation);
            return false;
        }
        capacityReservationsCreated++;
        PeerInfo peer = peers.get(reservation.workerId());
        if (peer != null) {
            peer.incrementTasks();
        }
        LOGGER.info(
                "event=assignment_capacity_reserved job_id={} task_id={} attempt_number={} "
                        + "assignment_id={} worker_id={} task_type={} capacity_unit_cost={}",
                reservation.jobId(),
                reservation.taskId(),
                reservation.attemptNumber(),
                reservation.assignmentId(),
                reservation.workerId(),
                reservation.taskType(),
                reservation.capacityUnitCost()
        );
        return true;
    }

    @Override
    public synchronized boolean releaseTaskCapacity(
            AssignmentCapacityReservation reservation,
            String releaseReason
    ) {
        Objects.requireNonNull(reservation, "reservation");
        String checkedReleaseReason = requireReleaseReason(releaseReason);
        CoordinatorCapacityLedger.ReleaseOutcome outcome;
        try {
            outcome = capacityLedger.release(reservation);
        } catch (RuntimeException failure) {
            invalidateCapacityProjection(
                    "release_exception_" + failure.getClass().getSimpleName(),
                    reservation,
                    failure
            );
            return false;
        }
        if (outcome != CoordinatorCapacityLedger.ReleaseOutcome.RELEASED) {
            invalidateCapacityProjection("release_" + outcome, reservation);
            return false;
        }
        capacityReservationsReleased++;
        PeerInfo peer = peers.get(reservation.workerId());
        if (peer != null) {
            peer.decrementTasks();
        }
        capacityAvailabilityVersion++;
        LOGGER.info(
                "event=assignment_capacity_released job_id={} task_id={} attempt_number={} "
                        + "assignment_id={} worker_id={} task_type={} capacity_unit_cost={} "
                        + "release_reason={} release_outcome=RELEASED",
                reservation.jobId(),
                reservation.taskId(),
                reservation.attemptNumber(),
                reservation.assignmentId(),
                reservation.workerId(),
                reservation.taskType(),
                reservation.capacityUnitCost(),
                checkedReleaseReason
        );
        return true;
    }

    @Override
    public synchronized boolean capacityProjectionValid() {
        return capacityProjectionValid;
    }

    @Override
    public synchronized CapacityMetricsSnapshot capacityMetricsSnapshot() {
        return new CapacityMetricsSnapshot(
                acceptedCapacitySnapshots,
                staleCapacitySnapshots,
                incompatibleCapacitySnapshots,
                capacityReservationsCreated,
                capacityReservationsReleased,
                capacityProjectionFailures,
                capacityLedger.reservationCount(),
                capacityLedger.reservedUnitsTotal()
        );
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
    }

    private void removeFromIndexes(PeerInfo peer) {
        for (String taskType : peer.getSupportedTaskTypes()) {
            removePeerId(capablePeerIdsByTaskType, taskType, peer.getNodeId());
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

    private static String requireReleaseReason(String releaseReason) {
        if (releaseReason == null || releaseReason.isBlank()) {
            throw new IllegalArgumentException("releaseReason is required.");
        }
        return releaseReason.trim();
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

    private void invalidateCapacityProjection(
            String reason,
            AssignmentCapacityReservation reservation
    ) {
        invalidateCapacityProjection(reason, reservation, null);
    }

    private void invalidateCapacityProjection(
            String reason,
            AssignmentCapacityReservation reservation,
            RuntimeException failure
    ) {
        capacityProjectionValid = false;
        capacityProjectionFailures++;
        LOGGER.error(
                "event=coordinator_capacity_projection_invalid reason={} job_id={} "
                        + "task_id={} attempt_number={} assignment_id={} worker_id={} "
                        + "task_type={} capacity_unit_cost={} error={}",
                reason,
                reservation.jobId(),
                reservation.taskId(),
                reservation.attemptNumber(),
                reservation.assignmentId(),
                reservation.workerId(),
                reservation.taskType(),
                reservation.capacityUnitCost(),
                failure == null ? "" : failure.getMessage(),
                failure
        );
    }
}
