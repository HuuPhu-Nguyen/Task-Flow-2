package server.registry;

import protocol.PeerIdentity;

import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

public record PeerRegistryRecord(String peerId,
                                 String runtimeType,
                                 PeerTransport transport,
                                 Set<String> supportedTaskTypes,
                                 long firstSeenAtMillis,
                                 long lastHeartbeatAtMillis,
                                 long lastDisconnectedAtMillis,
                                 PeerStatus status,
                                 PeerMetricsSnapshot metricsSnapshot) {

    public PeerRegistryRecord {
        peerId = PeerIdentity.require(peerId);
        runtimeType = normalizeRuntimeType(runtimeType, transport);
        transport = transport == null ? PeerTransport.UNKNOWN : transport;
        supportedTaskTypes = normalizeTaskTypes(supportedTaskTypes);
        firstSeenAtMillis = Math.max(0L, firstSeenAtMillis);
        lastHeartbeatAtMillis = Math.max(0L, lastHeartbeatAtMillis);
        lastDisconnectedAtMillis = Math.max(0L, lastDisconnectedAtMillis);
        status = status == null ? PeerStatus.DISCONNECTED : status;
        metricsSnapshot = metricsSnapshot == null ? PeerMetricsSnapshot.empty() : metricsSnapshot;
    }

    public static PeerRegistryRecord fromPeer(PeerInfo peer,
                                              PeerStatus status,
                                              long lastDisconnectedAtMillis) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer info is required.");
        }
        return new PeerRegistryRecord(
                peer.getNodeId(),
                peer.getRuntimeType(),
                peer.getTransport(),
                peer.getSupportedTaskTypes(),
                peer.getFirstSeenAtMillis(),
                peer.getLastHeartbeatReceivedAtMillis(),
                status == PeerStatus.DISCONNECTED ? lastDisconnectedAtMillis : 0L,
                status,
                peer.metricsSnapshot()
        );
    }

    public static Set<String> normalizeTaskTypes(Collection<String> taskTypes) {
        if (taskTypes == null) {
            return Set.of();
        }
        TreeSet<String> normalized = new TreeSet<>();
        for (String taskType : taskTypes) {
            if (taskType != null && !taskType.isBlank()) {
                normalized.add(taskType.trim().toUpperCase(Locale.ROOT));
            }
        }
        return Set.copyOf(normalized);
    }

    private static String normalizeRuntimeType(String runtimeType, PeerTransport transport) {
        if (runtimeType != null && !runtimeType.isBlank()) {
            return runtimeType.trim();
        }
        PeerTransport normalizedTransport = transport == null ? PeerTransport.UNKNOWN : transport;
        return switch (normalizedTransport) {
            case TCP -> "TCP_PEER";
            case RABBITMQ -> "RABBITMQ_PEER";
            case UNKNOWN -> "PEER";
        };
    }
}
