package server.metrics;

import server.db.BrokerOutboxStore;
import server.objectstore.OrphanOutputGc;
import server.runtime.TaskFlowClock;
import server.scheduler.SchedulerMetrics;
import server.scheduler.TaskScheduler;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportMetrics;

import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class CoordinatorMetricsCollector {
    private final Supplier<SchedulerMetrics.Snapshot> schedulerSnapshot;
    private final Supplier<RabbitMqTransportMetrics.Snapshot> transportSnapshot;
    private final Supplier<BrokerOutboxStore.PendingOutboxMetrics> outboxMetrics;
    private final LongSupplier orphanOutputsTotal;
    private final TaskFlowClock clock;

    public CoordinatorMetricsCollector(
            TaskScheduler scheduler,
            RabbitMqTransport transport,
            BrokerOutboxStore outboxStore,
            OrphanOutputGc orphanOutputGc,
            TaskFlowClock clock
    ) {
        this(
                Objects.requireNonNull(scheduler, "scheduler")::getMetricsSnapshot,
                Objects.requireNonNull(transport, "transport")::metricsSnapshot,
                outboxStore == null
                        ? BrokerOutboxStore.PendingOutboxMetrics::storageFailure
                        : outboxStore::observePendingBrokerOutbox,
                orphanOutputGc == null
                        ? () -> 0L
                        : () -> orphanOutputGc.metricsSnapshot().orphanOutputsTotal(),
                clock
        );
    }

    CoordinatorMetricsCollector(
            Supplier<SchedulerMetrics.Snapshot> schedulerSnapshot,
            Supplier<RabbitMqTransportMetrics.Snapshot> transportSnapshot,
            Supplier<BrokerOutboxStore.PendingOutboxMetrics> outboxMetrics,
            LongSupplier orphanOutputsTotal,
            TaskFlowClock clock
    ) {
        this.schedulerSnapshot = Objects.requireNonNull(
                schedulerSnapshot,
                "schedulerSnapshot"
        );
        this.transportSnapshot = Objects.requireNonNull(
                transportSnapshot,
                "transportSnapshot"
        );
        this.outboxMetrics = Objects.requireNonNull(outboxMetrics, "outboxMetrics");
        this.orphanOutputsTotal = Objects.requireNonNull(
                orphanOutputsTotal,
                "orphanOutputsTotal"
        );
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    CoordinatorMetricsSnapshot snapshot() {
        SchedulerMetrics.Snapshot schedulerMetrics = schedulerSnapshot.get();
        RabbitMqTransportMetrics.Snapshot brokerSnapshot = transportSnapshot.get();
        OutboxSnapshot outboxSnapshot = observeOutbox();
        long orphanOutputs = orphanOutputsTotal.getAsLong();
        return new CoordinatorMetricsSnapshot(
                schedulerMetrics,
                outboxSnapshot.pending(),
                outboxSnapshot.oldestAgeSeconds(),
                brokerSnapshot.redeliveriesTotal(),
                brokerSnapshot.quarantinedTotal(),
                orphanOutputs
        );
    }

    private OutboxSnapshot observeOutbox() {
        BrokerOutboxStore.PendingOutboxMetrics observation =
                outboxMetrics.get();
        if (observation == null || !observation.observed()) {
            return OutboxSnapshot.unavailable();
        }
        if (observation.count() == 0L) {
            return new OutboxSnapshot(0.0, 0.0);
        }
        long now = clock.nowEpochMillis();
        long oldest = observation.oldestCreatedAt();
        double ageSeconds = now <= oldest ? 0.0 : (now - oldest) / 1_000.0;
        return new OutboxSnapshot(observation.count(), ageSeconds);
    }

    record CoordinatorMetricsSnapshot(
            SchedulerMetrics.Snapshot scheduler,
            double outboxPending,
            double outboxOldestAgeSeconds,
            long brokerRedeliveriesTotal,
            long brokerQuarantinedTotal,
            long orphanOutputsTotal
    ) {
        CoordinatorMetricsSnapshot {
            Objects.requireNonNull(scheduler, "scheduler");
            if (brokerRedeliveriesTotal < 0L
                    || brokerQuarantinedTotal < 0L
                    || orphanOutputsTotal < 0L) {
                throw new IllegalArgumentException(
                        "Coordinator metric counters must not be negative"
                );
            }
        }
    }

    private record OutboxSnapshot(double pending, double oldestAgeSeconds) {
        private static OutboxSnapshot unavailable() {
            return new OutboxSnapshot(Double.NaN, Double.NaN);
        }
    }
}
