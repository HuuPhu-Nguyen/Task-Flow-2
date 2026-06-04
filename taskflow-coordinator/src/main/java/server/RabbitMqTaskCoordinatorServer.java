package server;

import protocol.Message;
import protocol.PongMessage;
import server.db.DatabaseManager;
import server.model.MessageEnvelope;
import server.monitor.PeerLivenessMonitor;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.scheduler.SchedulerConfig;
import server.scheduler.TaskScheduler;
import transport.InboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RabbitMqTaskCoordinatorServer {
    private static final long HEARTBEAT_TIMEOUT_MILLIS = 90_000;

    public static void main(String[] args) throws Exception {
        RabbitMqTransport transport = new RabbitMqTransport(RabbitMqTransportConfig.fromEnvironment());
        transport.declareTopology();

        BlockingQueue<MessageEnvelope> inboundMailbox = new LinkedBlockingQueue<>();
        PeerRegistry registry = new InMemoryPeerRegistry();
        SchedulerConfig schedulerConfig = SchedulerConfig.fromRuntime();

        DatabaseManager db = null;
        try {
            db = new DatabaseManager();
            System.out.println("Database initialized: " + DatabaseManager.DB_PATH);
        } catch (Exception e) {
            System.err.println("Warning: could not open database, history will not be persisted: " + e.getMessage());
        }

        TaskScheduler schedulerLogic = new TaskScheduler(
                inboundMailbox,
                registry,
                db,
                new RabbitMqSchedulerOutput(transport),
                schedulerConfig
        );
        Thread schedulerThread = new Thread(schedulerLogic, "rabbitmq-task-scheduler");
        PeerLivenessMonitor monitor = new PeerLivenessMonitor(registry, HEARTBEAT_TIMEOUT_MILLIS);

        transport.subscribe(TransportRoute.JOB_SUBMIT,
                delivery -> enqueueForScheduler(inboundMailbox, delivery));
        transport.subscribe(TransportRoute.TASK_RESULT,
                delivery -> enqueueForScheduler(inboundMailbox, delivery));
        transport.subscribe(TransportRoute.HEARTBEAT,
                delivery -> handleHeartbeat(registry, schedulerConfig, delivery));

        DatabaseManager finalDb = db;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down RabbitMQ coordinator...");
            schedulerThread.interrupt();
            monitor.shutdown();
            if (finalDb != null) {
                finalDb.close();
            }
            try {
                transport.close();
            } catch (Exception ignored) {
            }
        }));

        monitor.start();
        schedulerThread.start();
        System.out.println("RabbitMqTaskCoordinatorServer listening on RabbitMQ routes.");
        Thread.currentThread().join();
    }

    private static void enqueueForScheduler(BlockingQueue<MessageEnvelope> inboundMailbox,
                                            InboundTransportMessage delivery) throws InterruptedException {
        inboundMailbox.put(new MessageEnvelope(delivery.message(), delivery.fromNodeId()));
    }

    private static void handleHeartbeat(PeerRegistry registry,
                                        SchedulerConfig schedulerConfig,
                                        InboundTransportMessage delivery) {
        Message message = delivery.message();
        if (!(message instanceof PongMessage pong)) {
            return;
        }
        String peerNodeId = delivery.fromNodeId();
        if (peerNodeId == null || peerNodeId.isBlank()) {
            peerNodeId = message.getNodeId();
        }
        if (peerNodeId == null || peerNodeId.isBlank()) {
            return;
        }

        PeerInfo peer = registry.get(peerNodeId);
        if (peer == null) {
            registry.register(peerNodeId, new PeerInfo(peerNodeId, schedulerConfig, pong.getSupportedTaskTypes()));
        } else {
            registry.updateHeartbeat(peerNodeId);
            peer.setSupportedTaskTypes(pong.getSupportedTaskTypes());
        }
    }
}
