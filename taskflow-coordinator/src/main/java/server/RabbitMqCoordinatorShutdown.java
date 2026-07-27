package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transport.BrokerTransport;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the coordinator's bounded shutdown order so accepted broker deliveries
 * are either drained to a scheduler disposition or returned to RabbitMQ by
 * closing their channel.
 */
final class RabbitMqCoordinatorShutdown implements Runnable {
    static final long DEFAULT_DRAIN_TIMEOUT_MILLIS = 10_000L;
    private static final long INTERRUPT_JOIN_TIMEOUT_MILLIS = 1_000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqCoordinatorShutdown.class);

    private final Runnable stopIntake;
    private final BrokerTransport transport;
    private final List<String> consumerTags;
    private final Runnable stopMonitor;
    private final AutoCloseable orphanOutputGc;
    private final AutoCloseable outboxReplayer;
    private final Runnable requestSchedulerDrain;
    private final Thread schedulerThread;
    private final AutoCloseable database;
    private final long drainTimeoutMillis;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();

    RabbitMqCoordinatorShutdown(Runnable stopIntake,
                                BrokerTransport transport,
                                List<String> consumerTags,
                                Runnable stopMonitor,
                                AutoCloseable outboxReplayer,
                                Runnable requestSchedulerDrain,
                                Thread schedulerThread,
                                AutoCloseable database) {
        this(
                stopIntake,
                transport,
                consumerTags,
                stopMonitor,
                null,
                outboxReplayer,
                requestSchedulerDrain,
                schedulerThread,
                database,
                DEFAULT_DRAIN_TIMEOUT_MILLIS
        );
    }

    RabbitMqCoordinatorShutdown(Runnable stopIntake,
                                BrokerTransport transport,
                                List<String> consumerTags,
                                Runnable stopMonitor,
                                AutoCloseable orphanOutputGc,
                                AutoCloseable outboxReplayer,
                                Runnable requestSchedulerDrain,
                                Thread schedulerThread,
                                AutoCloseable database) {
        this(
                stopIntake,
                transport,
                consumerTags,
                stopMonitor,
                orphanOutputGc,
                outboxReplayer,
                requestSchedulerDrain,
                schedulerThread,
                database,
                DEFAULT_DRAIN_TIMEOUT_MILLIS
        );
    }

    RabbitMqCoordinatorShutdown(Runnable stopIntake,
                                BrokerTransport transport,
                                List<String> consumerTags,
                                Runnable stopMonitor,
                                AutoCloseable outboxReplayer,
                                Runnable requestSchedulerDrain,
                                Thread schedulerThread,
                                AutoCloseable database,
                                long drainTimeoutMillis) {
        this(
                stopIntake,
                transport,
                consumerTags,
                stopMonitor,
                null,
                outboxReplayer,
                requestSchedulerDrain,
                schedulerThread,
                database,
                drainTimeoutMillis
        );
    }

    RabbitMqCoordinatorShutdown(Runnable stopIntake,
                                BrokerTransport transport,
                                List<String> consumerTags,
                                Runnable stopMonitor,
                                AutoCloseable orphanOutputGc,
                                AutoCloseable outboxReplayer,
                                Runnable requestSchedulerDrain,
                                Thread schedulerThread,
                                AutoCloseable database,
                                long drainTimeoutMillis) {
        this.stopIntake = Objects.requireNonNull(stopIntake, "stopIntake");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.consumerTags = List.copyOf(Objects.requireNonNull(consumerTags, "consumerTags"));
        this.stopMonitor = Objects.requireNonNull(stopMonitor, "stopMonitor");
        this.orphanOutputGc = orphanOutputGc;
        this.outboxReplayer = outboxReplayer;
        this.requestSchedulerDrain = Objects.requireNonNull(
                requestSchedulerDrain,
                "requestSchedulerDrain"
        );
        this.schedulerThread = Objects.requireNonNull(schedulerThread, "schedulerThread");
        this.database = database;
        if (drainTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("drainTimeoutMillis must be positive");
        }
        this.drainTimeoutMillis = drainTimeoutMillis;
    }

    @Override
    public void run() {
        shutdown();
    }

    void shutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }

        LOGGER.info("event=rabbitmq_coordinator_shutdown_started consumer_count={} drain_timeout_ms={}",
                consumerTags.size(),
                drainTimeoutMillis);
        runAction("stop_intake", stopIntake);
        cancelConsumers();
        boolean monitorStopped = runAction("stop_peer_monitor", stopMonitor);
        boolean orphanOutputGcStopped =
                closeResource("orphan_output_gc", orphanOutputGc);
        boolean outboxReplayerStopped = closeResource("outbox_replayer", outboxReplayer);
        runAction("request_scheduler_drain", requestSchedulerDrain);

        boolean drained = awaitScheduler(drainTimeoutMillis);
        if (!drained) {
            LOGGER.warn("event=rabbitmq_coordinator_shutdown_drain_timeout timeout_ms={} queue_action=broker_requeue_on_transport_close",
                    drainTimeoutMillis);
            schedulerThread.interrupt();
        }

        boolean transportStopped = closeResource("rabbitmq_transport", transport);
        if (!drained) {
            drained = awaitScheduler(INTERRUPT_JOIN_TIMEOUT_MILLIS);
        }

        if (drained
                && monitorStopped
                && orphanOutputGcStopped
                && outboxReplayerStopped
                && transportStopped) {
            closeResource("database", database);
        } else if (database != null) {
            LOGGER.error("event=rabbitmq_coordinator_shutdown_database_close_deferred scheduler_stopped={} peer_monitor_stopped={} orphan_output_gc_stopped={} outbox_replayer_stopped={} transport_stopped={}",
                    drained,
                    monitorStopped,
                    orphanOutputGcStopped,
                    outboxReplayerStopped,
                    transportStopped);
        }
        LOGGER.info("event=rabbitmq_coordinator_shutdown_completed scheduler_drained={} peer_monitor_stopped={} orphan_output_gc_stopped={} outbox_replayer_stopped={} transport_stopped={}",
                drained,
                monitorStopped,
                orphanOutputGcStopped,
                outboxReplayerStopped,
                transportStopped);
    }

    private void cancelConsumers() {
        for (String consumerTag : consumerTags) {
            try {
                transport.cancel(consumerTag);
            } catch (Exception e) {
                LOGGER.warn("event=rabbitmq_coordinator_consumer_cancel_failed consumer_tag={} error={}",
                        consumerTag,
                        e.getMessage(),
                        e);
            }
        }
    }

    private boolean awaitScheduler(long timeoutMillis) {
        try {
            schedulerThread.join(timeoutMillis);
            return !schedulerThread.isAlive();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("event=rabbitmq_coordinator_shutdown_wait_interrupted", e);
            return false;
        }
    }

    private static boolean runAction(String resource, Runnable action) {
        try {
            action.run();
            return true;
        } catch (RuntimeException e) {
            LOGGER.warn("event=rabbitmq_coordinator_shutdown_action_failed resource={} error={}",
                    resource,
                    e.getMessage(),
                    e);
            return false;
        }
    }

    private static boolean closeResource(String resource, AutoCloseable closeable) {
        if (closeable == null) {
            return true;
        }
        try {
            closeable.close();
            return true;
        } catch (Exception e) {
            LOGGER.warn("event=rabbitmq_coordinator_shutdown_close_failed resource={} error={}",
                    resource,
                    e.getMessage(),
                    e);
            return false;
        }
    }
}
