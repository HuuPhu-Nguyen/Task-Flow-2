package server.scheduler;

import server.db.BrokerOutboxStore;
import server.runtime.TaskFlowClock;

final class SchedulerOutboxService {
    private final SchedulerPersistence persistence;
    private final SchedulerOutput output;
    private final TaskFlowClock clock;
    private final SchedulerOverloadStatus overloadStatus;
    private final SchedulerEventLog events;

    SchedulerOutboxService(SchedulerPersistence persistence,
                           SchedulerOutput output,
                           TaskFlowClock clock,
                           SchedulerEventLog events) {
        this(
                persistence,
                output,
                clock,
                new SchedulerOverloadStatus(SchedulerConfig.defaults(), clock, events),
                events
        );
    }

    SchedulerOutboxService(SchedulerPersistence persistence,
                           SchedulerOutput output,
                           TaskFlowClock clock,
                           SchedulerOverloadStatus overloadStatus,
                           SchedulerEventLog events) {
        this.persistence = persistence;
        this.output = output;
        this.clock = clock;
        this.overloadStatus = overloadStatus;
        this.events = events;
    }

    BrokerOutboxStore store() {
        return persistence.store() instanceof BrokerOutboxStore store ? store : null;
    }

    BrokerOutboxPublisher publisher() {
        return output instanceof BrokerOutboxPublisher publisher ? publisher : null;
    }

    boolean available() {
        return store() != null && publisher() != null;
    }

    boolean publish(BrokerOutboxStore.OutboxRecord record) {
        BrokerOutboxStore outboxStore = store();
        BrokerOutboxPublisher outboxPublisher = publisher();
        if (outboxStore == null || outboxPublisher == null) {
            throw new IllegalStateException("Broker outbox publication is not configured.");
        }

        long attemptedAt = clock.nowEpochMillis();
        try {
            boolean published = outboxPublisher.publishOutbox(record);
            if (!published) {
                outboxStore.markBrokerOutboxPublishFailed(
                        record.outboxId(),
                        "publish_unconfirmed_or_unroutable",
                        attemptedAt
                );
                events.error("broker_outbox_publish_deferred", events.fields(
                        "outbox_id", record.outboxId(),
                        "route", record.message().route(),
                        "peer_id", record.message().peerNodeId(),
                        "reason", "publish_unconfirmed_or_unroutable"
                ));
                return false;
            }
            if (!outboxStore.markBrokerOutboxPublished(record.outboxId(), attemptedAt)) {
                events.error("broker_outbox_publish_mark_failed", events.fields(
                        "outbox_id", record.outboxId(),
                        "route", record.message().route(),
                        "peer_id", record.message().peerNodeId()
                ));
                return false;
            }
            return true;
        } catch (Exception e) {
            outboxStore.markBrokerOutboxPublishFailed(record.outboxId(), e.getMessage(), attemptedAt);
            events.error("broker_outbox_publish_failed", events.fields(
                    "outbox_id", record.outboxId(),
                    "route", record.message().route(),
                    "peer_id", record.message().peerNodeId(),
                    "error", e.getMessage()
            ));
            return false;
        } finally {
            refreshPendingOutbox(outboxStore);
        }
    }

    private void refreshPendingOutbox(BrokerOutboxStore outboxStore) {
        try {
            overloadStatus.refreshPendingOutbox(outboxStore.countPendingBrokerOutbox());
        } catch (RuntimeException e) {
            overloadStatus.refreshPendingOutbox(0L, false);
            events.error("scheduler_overload_outbox_count_failed", events.fields(
                    "error", e.getMessage()
            ));
        }
    }
}
