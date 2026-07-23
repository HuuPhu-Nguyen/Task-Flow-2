package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.Message;
import protocol.PeerIdentity;
import protocol.PeerDisconnectedMessage;
import protocol.PongMessage;
import server.db.DatabaseManager;
import server.job.EmbarrassinglyParallelJob;
import server.model.MessageEnvelope;
import server.monitor.PeerLivenessMonitor;
import server.rabbitmq.RabbitMqOutboxReplayer;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.registry.PeerTransport;
import server.runtime.AssignmentIdGenerator;
import server.runtime.SystemTaskFlowClock;
import server.runtime.TaskFlowClock;
import server.runtime.UuidAssignmentIdGenerator;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.TaskScheduler;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class RabbitMqTaskCoordinatorServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqTaskCoordinatorServer.class);

    private static final long HEARTBEAT_TIMEOUT_MILLIS = 90_000;

    public static void main(String[] args) throws Exception {
        RabbitMqTransport transport = new RabbitMqTransport(RabbitMqTransportConfig.fromEnvironment());
        transport.declareTopology();

        SchedulerConfig schedulerConfig = SchedulerConfig.fromRuntime();
        TaskFlowClock clock = SystemTaskFlowClock.INSTANCE;
        AssignmentIdGenerator assignmentIdGenerator = UuidAssignmentIdGenerator.INSTANCE;
        BlockingQueue<MessageEnvelope> inboundMailbox = SchedulerMailbox.create(schedulerConfig);

        DatabaseManager db = null;
        List<EmbarrassinglyParallelJob<?, ?>> resumedJobs = List.of();
        Map<String, String> resumedJobTokenHashes = Map.of();
        Map<String, String> resumedJobIdentityKeys = Map.of();
        try {
            db = new DatabaseManager();
            LOGGER.info("event=database_initialized path={}", DatabaseManager.DB_PATH);
            CoordinatorStartupRecovery.RecoveryResult recovery = CoordinatorStartupRecovery.recoverPersistedJobs(
                    db,
                    clock,
                    assignmentIdGenerator
            );
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
        RabbitMqSchedulerOutput schedulerOutput = new RabbitMqSchedulerOutput(transport);
        RabbitMqOutboxReplayer outboxReplayer = db == null
                ? null
                : new RabbitMqOutboxReplayer(db, schedulerOutput);
        TaskScheduler schedulerLogic = new TaskScheduler(
                inboundMailbox,
                registry,
                db,
                schedulerOutput,
                schedulerConfig,
                clock,
                assignmentIdGenerator
        );
        schedulerLogic.restoreJobs(resumedJobs, resumedJobTokenHashes, resumedJobIdentityKeys);
        Thread schedulerThread = new Thread(schedulerLogic, "rabbitmq-task-scheduler");
        PeerLivenessMonitor monitor = new PeerLivenessMonitor(
                registry,
                HEARTBEAT_TIMEOUT_MILLIS,
                peer -> enqueuePeerUnavailable(inboundMailbox, peer, "heartbeat_timeout")
        );

        transport.subscribe(TransportRoute.JOB_SUBMIT,
                delivery -> enqueueForScheduler(inboundMailbox, delivery));
        transport.subscribe(TransportRoute.TASK_RESULT,
                delivery -> enqueueForScheduler(inboundMailbox, delivery));
        transport.subscribe(TransportRoute.HEARTBEAT,
                delivery -> handleHeartbeat(registry, schedulerConfig, delivery));

        DatabaseManager finalDb = db;
        RabbitMqOutboxReplayer finalOutboxReplayer = outboxReplayer;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("event=rabbitmq_coordinator_shutdown");
            schedulerThread.interrupt();
            monitor.shutdown();
            if (finalOutboxReplayer != null) {
                finalOutboxReplayer.close();
            }
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
        if (outboxReplayer != null) {
            outboxReplayer.start();
        }
        LOGGER.info("event=coordinator_started transport=rabbitmq");
        Thread.currentThread().join();
    }

    private static void enqueueForScheduler(BlockingQueue<MessageEnvelope> inboundMailbox,
                                            InboundTransportMessage delivery) throws Exception {
        boolean queued = SchedulerMailbox.offerBrokerDelivery(inboundMailbox, delivery);
        if (!queued) {
            LOGGER.warn("event=scheduler_ingress_retry_requested reason=mailbox_full route={} from_node_id={} queue_depth={}",
                    delivery.route(),
                    delivery.fromNodeId(),
                    inboundMailbox.size());
        }
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

    private static void handleHeartbeat(PeerRegistry registry,
                                        SchedulerConfig schedulerConfig,
                                        InboundTransportMessage delivery) throws Exception {
        Message message = delivery.message();
        if (!(message instanceof PongMessage pong)) {
            settle(delivery, DeliveryDisposition.REJECT_INVALID, "heartbeat_message_type_invalid");
            return;
        }
        String envelopePeerId = delivery.fromNodeId();
        String messagePeerId = message.getNodeId();
        if (envelopePeerId != null
                && !envelopePeerId.isBlank()
                && messagePeerId != null
                && !messagePeerId.isBlank()
                && !PeerIdentity.require(envelopePeerId).equals(PeerIdentity.require(messagePeerId))) {
            LOGGER.warn("event=rabbitmq_heartbeat_identity_mismatch envelope_peer_id={} message_peer_id={}",
                    envelopePeerId, messagePeerId);
            settle(delivery, DeliveryDisposition.REJECT_INVALID, "heartbeat_identity_mismatch");
            return;
        }
        String peerNodeId = envelopePeerId == null || envelopePeerId.isBlank()
                ? messagePeerId
                : envelopePeerId;
        if (peerNodeId == null || peerNodeId.isBlank()) {
            settle(delivery, DeliveryDisposition.REJECT_INVALID, "heartbeat_peer_id_missing");
            return;
        }
        peerNodeId = PeerIdentity.require(peerNodeId);

        PeerInfo peer = registry.get(peerNodeId);
        if (peer == null) {
            registry.registerIfAbsent(
                    peerNodeId,
                    new PeerInfo(peerNodeId, schedulerConfig, pong.getSupportedTaskTypes(), PeerTransport.RABBITMQ));
        } else {
            registry.updateHeartbeat(peerNodeId, pong.getSupportedTaskTypes());
        }
        settle(delivery, DeliveryDisposition.ACK_SUCCESS, "heartbeat_registered");
    }

    private static void settle(InboundTransportMessage delivery,
                               DeliveryDisposition disposition,
                               String reasonCode) throws Exception {
        if (delivery.acknowledgement() != null) {
            delivery.acknowledgement().settle(disposition, reasonCode);
        }
    }
}
