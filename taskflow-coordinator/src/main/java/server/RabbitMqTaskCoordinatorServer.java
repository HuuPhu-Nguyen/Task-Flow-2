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
import transport.rabbitmq.RabbitMqRecoveryPolicy;
import transport.rabbitmq.RabbitMqTransportConnector;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class RabbitMqTaskCoordinatorServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqTaskCoordinatorServer.class);

    private static final long HEARTBEAT_TIMEOUT_MILLIS = 90_000;

    public static void main(String[] args) throws Exception {
        RabbitMqTransportConfig transportConfig = RabbitMqTransportConfig.fromEnvironment();
        RabbitMqRecoveryPolicy recoveryPolicy = RabbitMqRecoveryPolicy.fromEnvironment();
        RabbitMqTransportConnector startupConnector =
                new RabbitMqTransportConnector(transportConfig, recoveryPolicy);
        AtomicReference<Runnable> shutdownAction =
                new AtomicReference<>(startupConnector::close);
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> shutdownAction.get().run(), "rabbitmq-coordinator-shutdown")
        );

        LOGGER.info(
                "event=coordinator_broker_startup_waiting host={} port={} "
                        + "connection_timeout_ms={} recovery_initial_delay_ms={} recovery_max_delay_ms={}",
                transportConfig.host(),
                transportConfig.port(),
                recoveryPolicy.connectionTimeoutMillis(),
                recoveryPolicy.initialRetryDelayMillis(),
                recoveryPolicy.maxRetryDelayMillis()
        );
        RabbitMqTransport transport = startupConnector.connect();
        transport.declareTopology();

        SchedulerConfig schedulerConfig = SchedulerConfig.fromRuntime();
        TaskFlowClock clock = SystemTaskFlowClock.INSTANCE;
        AssignmentIdGenerator assignmentIdGenerator = UuidAssignmentIdGenerator.INSTANCE;
        BlockingQueue<MessageEnvelope> inboundMailbox = SchedulerMailbox.create(schedulerConfig);
        SchedulerMailbox.BrokerIngress brokerIngress = SchedulerMailbox.brokerIngress(inboundMailbox);

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

        String jobSubmitConsumerTag = transport.subscribe(TransportRoute.JOB_SUBMIT,
                delivery -> enqueueForScheduler(brokerIngress, delivery));
        String taskResultConsumerTag = transport.subscribe(TransportRoute.TASK_RESULT,
                delivery -> enqueueForScheduler(brokerIngress, delivery));
        String heartbeatConsumerTag = transport.subscribe(TransportRoute.HEARTBEAT,
                delivery -> handleHeartbeat(registry, schedulerConfig, delivery));

        DatabaseManager finalDb = db;
        RabbitMqOutboxReplayer finalOutboxReplayer = outboxReplayer;
        RabbitMqCoordinatorShutdown shutdown = new RabbitMqCoordinatorShutdown(
                brokerIngress::stopIntake,
                transport,
                List.of(jobSubmitConsumerTag, taskResultConsumerTag, heartbeatConsumerTag),
                monitor::shutdown,
                finalOutboxReplayer,
                schedulerLogic::requestShutdownAfterDrain,
                schedulerThread,
                finalDb
        );
        shutdownAction.set(shutdown);
        RabbitMqTransport releasedTransport = startupConnector.releaseTransportOwnership();
        if (releasedTransport != transport) {
            throw new IllegalStateException("startup connector released an unexpected transport");
        }

        monitor.start();
        schedulerThread.start();
        if (outboxReplayer != null) {
            outboxReplayer.start();
        }
        LOGGER.info("event=coordinator_started transport=rabbitmq");
        Thread.currentThread().join();
    }

    private static void enqueueForScheduler(SchedulerMailbox.BrokerIngress brokerIngress,
                                            InboundTransportMessage delivery) throws Exception {
        SchedulerMailbox.BrokerOfferOutcome outcome = brokerIngress.offer(delivery);
        if (outcome == SchedulerMailbox.BrokerOfferOutcome.MAILBOX_FULL_RETRY) {
            LOGGER.warn("event=scheduler_ingress_retry_requested reason=mailbox_full route={} from_node_id={} queue_depth={}",
                    delivery.route(),
                    delivery.fromNodeId(),
                    brokerIngress.queueDepth());
        } else if (outcome == SchedulerMailbox.BrokerOfferOutcome.INTAKE_STOPPED_UNACKNOWLEDGED) {
            LOGGER.info("event=scheduler_ingress_stopped route={} from_node_id={} action=broker_requeue_on_transport_close",
                    delivery.route(),
                    delivery.fromNodeId());
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
