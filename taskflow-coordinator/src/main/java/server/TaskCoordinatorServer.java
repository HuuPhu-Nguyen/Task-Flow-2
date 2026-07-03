package server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.PeerDisconnectedMessage;
import server.db.DatabaseManager;
import server.handler.PeerHandler;
import server.job.EmbarrassinglyParallelJob;
import server.model.MessageEnvelope;
import server.monitor.PeerLivenessMonitor;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.scheduler.PeerRegistrySchedulerOutput;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.TaskScheduler;

public class TaskCoordinatorServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskCoordinatorServer.class);

    private static final int PORT = 6789;
    private static final int IO_POOL_SIZE = 100;
    private static final long HEARTBEAT_TIMEOUT_MILLIS = 90_000;
    private static final String TRANSPORT_ENV = "TASKFLOW_TRANSPORT";

    public static void main(String[] args) throws Exception {
        if (isHelpRequested(args)) {
            System.out.println(usage());
            return;
        }

        if (isRabbitMqTransportSelected()) {
            RabbitMqTaskCoordinatorServer.main(args);
            return;
        }

        SchedulerConfig schedulerConfig = SchedulerConfig.fromRuntime();
        BlockingQueue<MessageEnvelope> inboundMailbox = SchedulerMailbox.create(schedulerConfig);

        DatabaseManager db = null;
        List<EmbarrassinglyParallelJob<?, ?>> resumedJobs = List.of();
        Map<String, String> resumedJobTokenHashes = Map.of();
        Map<String, String> resumedJobIdentityKeys = Map.of();
        try {
            db = new DatabaseManager();
            LOGGER.info("event=database_initialized path={}", DatabaseManager.DB_PATH);
            CoordinatorStartupRecovery.RecoveryResult recovery = CoordinatorStartupRecovery.recoverPersistedJobs(db);
            if (!recovery.successful()) {
                db.close();
                db = null;
                LOGGER.warn("event=database_disabled path={} reason=startup_reconciliation_failed",
                        DatabaseManager.DB_PATH);
            } else {
                resumedJobs = recovery.resumedJobs();
                resumedJobTokenHashes = recovery.requesterTokenHashes();
                resumedJobIdentityKeys = recovery.requesterIdentityKeys();
            }
        } catch (Exception e) {
            if (db != null) {
                db.close();
                db = null;
            }
            LOGGER.warn("event=database_unavailable path={} error={}",
                    DatabaseManager.DB_PATH, e.getMessage(), e);
        }

        PeerRegistry registry = new InMemoryPeerRegistry(db);
        TaskScheduler schedulerLogic = new TaskScheduler(
                inboundMailbox,
                registry,
                db,
                new PeerRegistrySchedulerOutput(registry),
                schedulerConfig
        );
        schedulerLogic.restoreJobs(resumedJobs, resumedJobTokenHashes, resumedJobIdentityKeys);
        Thread schedulerThread = new Thread(schedulerLogic, "task-scheduler");

        //Monitoring and Networking
        ExecutorService ioPool = Executors.newFixedThreadPool(IO_POOL_SIZE);
        PeerLivenessMonitor monitor = new PeerLivenessMonitor(
                registry,
                HEARTBEAT_TIMEOUT_MILLIS,
                peer -> enqueuePeerUnavailable(inboundMailbox, peer, "heartbeat_timeout")
        );

        //Status Printer (Modified to include new performance metrics)
        Thread statusPrinter = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(3000);
                    LOGGER.info("event=peer_status{}", buildStatusReport(registry, schedulerLogic));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "status-printer");

        // Start everything
        monitor.start();
        schedulerThread.start();
        statusPrinter.start();

        final DatabaseManager finalDb = db;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("event=coordinator_shutdown");
            schedulerThread.interrupt();
            monitor.shutdown();
            ioPool.shutdownNow();
            if (finalDb != null) finalDb.close();
        }));

        //Server Loop
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            LOGGER.info("event=coordinator_started transport=tcp port={}", PORT);
            while (true) {
                Socket socket = serverSocket.accept();
                // PeerHandler will drop messages into 'inboundMailbox', which schedulerThread pulls from
                ioPool.submit(new PeerHandler(socket, registry, inboundMailbox, schedulerConfig));
            }
        } catch (IOException e) {
            LOGGER.error("event=coordinator_accept_loop_failed error={}", e.getMessage(), e);
        }
    }

    private static boolean isRabbitMqTransportSelected() {
        return "rabbitmq".equalsIgnoreCase(System.getenv().getOrDefault(TRANSPORT_ENV, "tcp"));
    }

    static boolean isHelpRequested(String[] args) {
        return args != null
                && args.length > 0
                && ("--help".equals(args[0]) || "-h".equals(args[0]));
    }

    static String usage() {
        return String.join(System.lineSeparator(),
                "TASKFLOW_TRANSPORT=tcp java -jar taskflow-coordinator-<version>-coordinator-runtime.jar",
                "TASKFLOW_TRANSPORT=rabbitmq java -jar taskflow-coordinator-<version>-coordinator-runtime.jar");
    }

    private static void enqueuePeerUnavailable(BlockingQueue<MessageEnvelope> inboundMailbox,
                                               PeerInfo peer,
                                               String reason) {
        if (peer == null) {
            return;
        }
        boolean queued = SchedulerMailbox.offer(inboundMailbox, new MessageEnvelope(
                new PeerDisconnectedMessage(peer.getNodeId(), Instant.now().toString(), reason),
                peer.getNodeId()
        ));
        if (!queued) {
            LOGGER.error("event=peer_unavailable_event_dropped peer_id={} reason={}", peer.getNodeId(), reason);
        }
    }

    private static String buildStatusReport(PeerRegistry registry, TaskScheduler schedulerLogic) {
        StringBuilder report = new StringBuilder(System.lineSeparator())
                .append("========== PEER STATUS ==========").append(System.lineSeparator());
        if (registry.getAllPeers().isEmpty()) {
            report.append("Peers: none connected").append(System.lineSeparator());
        } else {
            report.append(String.format(Locale.ROOT, "%-20s %-10s %-10s %-10s %-10s%n",
                    "Node ID", "Status", "Tasks", "Latency", "Avg Dur"));

            long now = System.currentTimeMillis();
            for (var peer : registry.getAllPeers()) {
                long delta = now - peer.getLastHeartbeatReceivedAtMillis();
                String status = (delta < HEARTBEAT_TIMEOUT_MILLIS) ? "ALIVE" : "STALE";

                report.append(String.format(Locale.ROOT, "%-20s %-10s %-10d %-10d %-10d%n",
                        peer.getNodeId(),
                        status,
                        peer.getActiveTasks(),
                        peer.getLatency(),
                        peer.getAvgTaskDuration()));
            }
        }

        var schedulerMetrics = schedulerLogic.getMetricsSnapshot();
        report.append(String.format(Locale.ROOT,
                "Scheduler: queue=%d activeJobs=%d dispatchLatencyMs=%.2f retries=%d successRate=%.4f%n",
                schedulerMetrics.queueDepth(),
                schedulerMetrics.activeJobs(),
                schedulerMetrics.avgDispatchLatencyMs(),
                schedulerMetrics.retryCount(),
                schedulerMetrics.taskSuccessRate()));
        report.append("=================================");
        return report.toString();
    }
}
