package gui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peer.engine.AssignmentCacheConflictException;
import peer.engine.AssignmentCacheSnapshot;
import peer.engine.AssignmentExecution;
import protocol.JobResultMessage;
import protocol.Message;
import protocol.PeerIdentity;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.BrokerTransport;
import transport.ClassifiedDeliveryFailure;
import transport.DeliveryDisposition;
import transport.DeliveryFailureClassifier;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;
import transport.TransientDeliveryException;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class RabbitMqCoordinatorConnection implements StartableCoordinatorConnection, RabbitMqBrokerConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqCoordinatorConnection.class);
    private static final long HEARTBEAT_INTERVAL_MILLIS = 30_000;

    private final String peerId;
    private final String host;
    private final int port;
    private final GuiWorkerRuntime workerRuntime;
    private final CoordinatorConnectionListener listener;
    private final TransportFactory transportFactory;
    private final long heartbeatIntervalMillis;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<String> consumerTags = new CopyOnWriteArrayList<>();

    private volatile BrokerTransport transport;
    private volatile ScheduledExecutorService heartbeats;
    private volatile Thread startupThread;
    private volatile boolean connected;

    RabbitMqCoordinatorConnection(String peerId,
                                  String host,
                                  int port,
                                  GuiWorkerRuntime workerRuntime,
                                  CoordinatorConnectionListener listener) {
        this(peerId, host, port, workerRuntime, listener, RabbitMqTransport::new, HEARTBEAT_INTERVAL_MILLIS);
    }

    RabbitMqCoordinatorConnection(String peerId,
                                  String host,
                                  int port,
                                  GuiWorkerRuntime workerRuntime,
                                  CoordinatorConnectionListener listener,
                                  TransportFactory transportFactory,
                                  long heartbeatIntervalMillis) {
        if (peerId == null || peerId.isBlank()) {
            throw new IllegalArgumentException("peerId is required.");
        }
        this.peerId = PeerIdentity.require(peerId);
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.workerRuntime = Objects.requireNonNull(workerRuntime, "workerRuntime");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.transportFactory = Objects.requireNonNull(transportFactory, "transportFactory");
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    @Override
    public void start() {
        Thread thread = new Thread(this::connect, "gui-rabbitmq-connection");
        thread.setDaemon(true);
        startupThread = thread;
        thread.start();
    }

    @Override
    public boolean isOpen() {
        return connected && !closed.get() && transport != null;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ScheduledExecutorService heartbeatExecutor = heartbeats;
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        BrokerTransport currentTransport = transport;
        if (currentTransport != null) {
            for (String consumerTag : consumerTags) {
                try {
                    currentTransport.cancel(consumerTag);
                } catch (Exception cancelFailure) {
                    LOGGER.warn("event=gui_rabbitmq_consumer_cancel_failed peer_id={} consumer_tag={} error={}",
                            peerId, consumerTag, cancelFailure.getMessage(), cancelFailure);
                }
            }
            try {
                currentTransport.close();
            } catch (Exception closeFailure) {
                LOGGER.warn("event=gui_rabbitmq_close_failed peer_id={} error={}",
                        peerId, closeFailure.getMessage(), closeFailure);
            }
        }
        Thread currentThread = startupThread;
        if (currentThread != null) {
            currentThread.interrupt();
        }
        connected = false;
    }

    @Override
    public BrokerTransport transport() {
        BrokerTransport currentTransport = transport;
        if (currentTransport == null) {
            throw new IllegalStateException("RabbitMQ transport is not connected.");
        }
        return currentTransport;
    }

    @Override
    public String peerId() {
        return peerId;
    }

    private void connect() {
        try {
            RabbitMqTransportConfig config = configFor(RabbitMqTransportConfig.fromEnvironment(), host, port);
            BrokerTransport openedTransport = transportFactory.create(config);
            if (closed.get()) {
                openedTransport.close();
                return;
            }
            transport = openedTransport;
            openedTransport.declareTopology();
            consumerTags.add(openedTransport.subscribePeer(
                    TransportRoute.TASK_ASSIGN,
                    peerId,
                    this::handleTaskAssignment));
            consumerTags.add(openedTransport.subscribePeer(
                    TransportRoute.JOB_RESULT,
                    peerId,
                    this::handleJobResult));
            startHeartbeats(openedTransport);
            connected = true;
            listener.onConnected(this);
        } catch (Exception e) {
            if (!closed.get()) {
                close();
                listener.onConnectionFailed(this, e.getMessage());
            }
        }
    }

    private void handleTaskAssignment(InboundTransportMessage delivery) throws Exception {
        Message message = delivery.message();
        TransportAcknowledgement acknowledgement = delivery.acknowledgement();
        if (!(message instanceof TaskAssignMessage task)) {
            settle(acknowledgement, DeliveryDisposition.REJECT_INVALID);
            return;
        }
        if (!peerId.equals(task.getNodeId())) {
            LOGGER.warn("event=gui_rabbitmq_task_assignment_ignored peer_id={} assigned_peer_id={}",
                    peerId, task.getNodeId());
            settle(acknowledgement, DeliveryDisposition.ACK_DUPLICATE_OR_STALE);
            return;
        }

        try {
            if (acknowledgement != null) {
                acknowledgement.defer();
            }
            AssignmentExecution execution = workerRuntime.executeAssignment(task);
            logAssignmentCacheDecision(task, execution, workerRuntime.assignmentCacheSnapshot());
            if (execution.disposition() == AssignmentExecution.Disposition.DUPLICATE_RUNNING) {
                settle(acknowledgement, DeliveryDisposition.ACK_DUPLICATE_OR_STALE);
                return;
            }
            execution.resultFuture().whenComplete((result, failure) ->
                    completeTaskAssignment(
                            task,
                            result,
                            failure,
                            acknowledgement,
                            execution.disposition()
                    ));
        } catch (AssignmentCacheConflictException conflict) {
            LOGGER.warn(
                    "event=gui_rabbitmq_task_assignment_cache_conflict peer_id={} task_id={} "
                            + "assignment_id={} error={}",
                    peerId,
                    task.getTaskId(),
                    task.getAssignmentId(),
                    conflict.getMessage()
            );
            settle(acknowledgement, DeliveryDisposition.QUARANTINE_POISON);
        } catch (Exception e) {
            ClassifiedDeliveryFailure classified = DeliveryFailureClassifier.classify(e);
            LOGGER.warn("event=gui_rabbitmq_task_assignment_handler_failed peer_id={} task_id={} reason_code={} disposition={} error={}",
                    peerId,
                    task.getTaskId(),
                    classified.reasonCode(),
                    classified.disposition(),
                    e.getMessage(),
                    e);
            settle(acknowledgement, classified.disposition());
        }
    }

    private void logAssignmentCacheDecision(TaskAssignMessage task,
                                            AssignmentExecution execution,
                                            AssignmentCacheSnapshot snapshot) {
        if (execution.disposition() == AssignmentExecution.Disposition.STARTED) {
            return;
        }
        LOGGER.info(
                "event=gui_rabbitmq_task_assignment_duplicate peer_id={} task_id={} assignment_id={} "
                        + "disposition={} cache_size={} cache_evictions_total={}",
                peerId,
                task.getTaskId(),
                task.getAssignmentId(),
                execution.disposition(),
                snapshot.size(),
                snapshot.evictionCount()
        );
    }

    private void completeTaskAssignment(TaskAssignMessage task,
                                        TaskResultMessage result,
                                        Throwable failure,
                                        TransportAcknowledgement acknowledgement,
                                        AssignmentExecution.Disposition executionDisposition) {
        if (failure != null) {
            ClassifiedDeliveryFailure classified = DeliveryFailureClassifier.classify(failure);
            LOGGER.warn("event=gui_rabbitmq_task_execution_failed peer_id={} task_id={} reason_code={} disposition={} error={}",
                    peerId,
                    task.getTaskId(),
                    classified.reasonCode(),
                    classified.disposition(),
                    failure.getMessage(),
                    failure);
            settleQuietly(acknowledgement, classified.disposition());
            return;
        }
        try {
            boolean published = transport().publish(new OutboundTransportMessage(
                    TransportRoute.TASK_RESULT,
                    result.getNodeId(),
                    result
            ));
            if (!published) {
                throw new TransientDeliveryException(
                        "broker_publish_not_confirmed",
                        "Task result publish was not confirmed for task " + task.getTaskId(),
                        null
                );
            }
            DeliveryDisposition disposition =
                    executionDisposition == AssignmentExecution.Disposition.DUPLICATE_COMPLETED
                            ? DeliveryDisposition.ACK_DUPLICATE_OR_STALE
                            : DeliveryDisposition.ACK_SUCCESS;
            settle(acknowledgement, disposition);
        } catch (Exception publishFailure) {
            ClassifiedDeliveryFailure classified = DeliveryFailureClassifier.classify(publishFailure);
            LOGGER.warn("event=gui_rabbitmq_task_result_publish_failed peer_id={} task_id={} reason_code={} disposition={} error={}",
                    peerId,
                    task.getTaskId(),
                    classified.reasonCode(),
                    classified.disposition(),
                    publishFailure.getMessage(),
                    publishFailure);
            settleQuietly(acknowledgement, classified.disposition());
        }
    }

    private void handleJobResult(InboundTransportMessage delivery) throws Exception {
        Message message = delivery.message();
        TransportAcknowledgement acknowledgement = delivery.acknowledgement();
        if (!(message instanceof JobResultMessage result)) {
            settle(acknowledgement, DeliveryDisposition.REJECT_INVALID);
            return;
        }
        listener.onJobResult(this, result);
        settle(acknowledgement, DeliveryDisposition.ACK_SUCCESS);
        LOGGER.info("event=gui_rabbitmq_job_result_received peer_id={} job_id={} success={}",
                peerId, result.getJobId(), result.isSuccessful());
    }

    private void startHeartbeats(BrokerTransport openedTransport) throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "gui-rabbitmq-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        Runnable heartbeat = () -> {
            try {
                publishHeartbeat(openedTransport);
            } catch (Exception e) {
                LOGGER.warn("event=gui_rabbitmq_heartbeat_failed peer_id={} error={}",
                        peerId, e.getMessage(), e);
            }
        };
        publishHeartbeat(openedTransport);
        scheduler.scheduleAtFixedRate(
                heartbeat,
                heartbeatIntervalMillis,
                heartbeatIntervalMillis,
                TimeUnit.MILLISECONDS
        );
        heartbeats = scheduler;
    }

    private void publishHeartbeat(BrokerTransport openedTransport) throws Exception {
        boolean published = openedTransport.publish(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                peerId,
                new PongMessage(peerId, Instant.now().toString(), workerRuntime.supportedTaskTypes())
        ));
        if (!published) {
            throw new TransientDeliveryException(
                    "broker_publish_not_confirmed",
                    "Heartbeat publish was not confirmed.",
                    null
            );
        }
    }

    private static RabbitMqTransportConfig configFor(RabbitMqTransportConfig base, String host, int port) {
        return new RabbitMqTransportConfig(
                host,
                port,
                base.username(),
                base.password(),
                base.virtualHost(),
                base.exchangeName(),
                base.queuePrefix(),
                base.durable(),
                base.prefetchCount(),
                base.publisherConfirmTimeoutMillis(),
                base.deadLetterEnabled(),
                base.deadLetterExchangeName(),
                base.deadLetterQueueName(),
                base.deadLetterRoutingKey()
        );
    }

    private static void settle(TransportAcknowledgement acknowledgement,
                               DeliveryDisposition disposition) throws Exception {
        if (acknowledgement != null) {
            acknowledgement.settle(disposition);
        }
    }

    private static void settleQuietly(TransportAcknowledgement acknowledgement,
                                      DeliveryDisposition disposition) {
        try {
            settle(acknowledgement, disposition);
        } catch (Exception settlementError) {
            if (settlementError instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOGGER.warn("event=gui_rabbitmq_delivery_settlement_failed disposition={} error={}",
                    disposition, settlementError.getMessage(), settlementError);
        }
    }

    @FunctionalInterface
    interface TransportFactory {
        BrokerTransport create(RabbitMqTransportConfig config) throws Exception;
    }
}
