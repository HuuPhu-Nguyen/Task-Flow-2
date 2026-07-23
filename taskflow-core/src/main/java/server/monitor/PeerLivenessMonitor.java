package server.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;

import java.util.function.Consumer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PeerLivenessMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeerLivenessMonitor.class);

    private final PeerRegistry registry;
    private final ScheduledExecutorService scheduler;
    private final long timeoutMillis;
    private final Consumer<PeerInfo> onTimeout;

    public PeerLivenessMonitor(PeerRegistry registry, long timeoutMillis) {
        this(registry, timeoutMillis, null);
    }

    public PeerLivenessMonitor(PeerRegistry registry, long timeoutMillis, Consumer<PeerInfo> onTimeout) {
        this.registry = registry;
        this.timeoutMillis = timeoutMillis;
        this.onTimeout = onTimeout;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::checkPeers, 5, 5, TimeUnit.SECONDS);
    }

    private void checkPeers() {
        long now = System.currentTimeMillis();
        for (PeerInfo peer : registry.getAllPeers()) {
            long lastSeen = peer.getLastHeartbeatReceivedAtMillis();
            if (now - lastSeen > timeoutMillis) {
                LOGGER.warn("event=peer_timeout peer_id={} timeout_ms={}", peer.getNodeId(), timeoutMillis);
                if (onTimeout != null) {
                    onTimeout.accept(peer);
                }
                registry.remove(peer.getNodeId());
            }
        }
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
