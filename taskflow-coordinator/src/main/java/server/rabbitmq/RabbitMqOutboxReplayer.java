package server.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.db.BrokerOutboxStore;
import server.scheduler.BrokerOutboxPublisher;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class RabbitMqOutboxReplayer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqOutboxReplayer.class);
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final long DEFAULT_REPLAY_INTERVAL_MILLIS = 1_000L;
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 2_000L;

    private final BrokerOutboxStore outboxStore;
    private final BrokerOutboxPublisher publisher;
    private final int batchSize;
    private final long replayIntervalMillis;
    private final ScheduledExecutorService executor;

    public RabbitMqOutboxReplayer(BrokerOutboxStore outboxStore, BrokerOutboxPublisher publisher) {
        this(outboxStore, publisher, DEFAULT_BATCH_SIZE, DEFAULT_REPLAY_INTERVAL_MILLIS);
    }

    RabbitMqOutboxReplayer(BrokerOutboxStore outboxStore,
                           BrokerOutboxPublisher publisher,
                           int batchSize,
                           long replayIntervalMillis) {
        this.outboxStore = outboxStore;
        this.publisher = publisher;
        this.batchSize = batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
        this.replayIntervalMillis = replayIntervalMillis <= 0
                ? DEFAULT_REPLAY_INTERVAL_MILLIS
                : replayIntervalMillis;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "rabbitmq-outbox-replayer");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        Future<Integer> initialReplay = executor.submit(this::replayOnce);
        try {
            initialReplay.get();
        } catch (InterruptedException e) {
            initialReplay.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during initial RabbitMQ outbox replay.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Initial RabbitMQ outbox replay failed.", cause);
        }

        try {
            executor.scheduleWithFixedDelay(
                    this::replayOnceSafely,
                    replayIntervalMillis,
                    replayIntervalMillis,
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException e) {
            if (!executor.isShutdown()) {
                throw e;
            }
        }
    }

    int replayOnce() {
        List<BrokerOutboxStore.OutboxRecord> records = outboxStore.loadPendingBrokerOutbox(batchSize);
        int published = 0;
        for (BrokerOutboxStore.OutboxRecord record : records) {
            if (publish(record)) {
                published++;
            }
        }
        if (!records.isEmpty()) {
            LOGGER.info("event=rabbitmq_outbox_replay batch_size={} published={}",
                    records.size(), published);
        }
        return published;
    }

    private void replayOnceSafely() {
        try {
            replayOnce();
        } catch (RuntimeException e) {
            LOGGER.warn("event=rabbitmq_outbox_replay_failed error={}", e.getMessage(), e);
        }
    }

    private boolean publish(BrokerOutboxStore.OutboxRecord record) {
        long attemptedAt = System.currentTimeMillis();
        try {
            boolean published = publisher.publishOutbox(record);
            if (!published) {
                outboxStore.markBrokerOutboxPublishFailed(
                        record.outboxId(),
                        "publish_unconfirmed_or_unroutable",
                        attemptedAt
                );
                LOGGER.warn("event=rabbitmq_outbox_publish_deferred outbox_id={} route={} peer_id={} attempt={}",
                        record.outboxId(),
                        record.message().route(),
                        record.message().peerNodeId(),
                        record.attemptCount() + 1);
                return false;
            }
            if (!outboxStore.markBrokerOutboxPublished(record.outboxId(), attemptedAt)) {
                LOGGER.warn("event=rabbitmq_outbox_publish_mark_failed outbox_id={}", record.outboxId());
                return false;
            }
            LOGGER.info("event=rabbitmq_outbox_published outbox_id={} route={} peer_id={}",
                    record.outboxId(),
                    record.message().route(),
                    record.message().peerNodeId());
            return true;
        } catch (Exception e) {
            outboxStore.markBrokerOutboxPublishFailed(record.outboxId(), e.getMessage(), attemptedAt);
            LOGGER.warn("event=rabbitmq_outbox_publish_failed outbox_id={} route={} peer_id={} error={}",
                    record.outboxId(),
                    record.message().route(),
                    record.message().peerNodeId(),
                    e.getMessage(),
                    e);
            return false;
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                        "RabbitMQ outbox replayer did not stop within "
                                + SHUTDOWN_TIMEOUT_MILLIS
                                + " ms."
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while stopping the RabbitMQ outbox replayer.",
                    e
            );
        }
    }
}
