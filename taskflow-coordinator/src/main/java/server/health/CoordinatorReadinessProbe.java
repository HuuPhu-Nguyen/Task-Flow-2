package server.health;

import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.registry.PeerRegistry;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerOverloadSnapshot;
import server.scheduler.TaskScheduler;
import transport.rabbitmq.RabbitMqTransport;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Reads the current authorities that decide whether a new job can be accepted. */
public final class CoordinatorReadinessProbe
        implements Supplier<CoordinatorHealth.ReadinessInputs> {
    private final DatabaseManager database;
    private final BooleanSupplier brokerUsable;
    private final Supplier<SchedulerOverloadSnapshot> overloadSnapshot;
    private final BooleanSupplier capacityProjectionValid;
    private final long maxPendingOutboxRows;

    public CoordinatorReadinessProbe(
            DatabaseManager database,
            RabbitMqTransport transport,
            TaskScheduler scheduler,
            PeerRegistry peerRegistry,
            SchedulerConfig schedulerConfig
    ) {
        this(
                database,
                Objects.requireNonNull(transport, "transport")::connectionUsable,
                Objects.requireNonNull(scheduler, "scheduler")::getOverloadSnapshot,
                Objects.requireNonNull(peerRegistry, "peerRegistry")::capacityProjectionValid,
                schedulerConfig
        );
    }

    CoordinatorReadinessProbe(
            DatabaseManager database,
            BooleanSupplier brokerUsable,
            Supplier<SchedulerOverloadSnapshot> overloadSnapshot,
            BooleanSupplier capacityProjectionValid,
            SchedulerConfig schedulerConfig
    ) {
        this.database = database;
        this.brokerUsable = Objects.requireNonNull(brokerUsable, "brokerUsable");
        this.overloadSnapshot = Objects.requireNonNull(
                overloadSnapshot,
                "overloadSnapshot"
        );
        this.capacityProjectionValid = Objects.requireNonNull(
                capacityProjectionValid,
                "capacityProjectionValid"
        );
        this.maxPendingOutboxRows = Objects.requireNonNull(
                schedulerConfig,
                "schedulerConfig"
        ).maxPendingOutboxRows();
    }

    @Override
    public CoordinatorHealth.ReadinessInputs get() {
        boolean sqliteWritable = database != null && database.isWritable();
        BrokerOutboxStore.PendingOutboxMetrics outbox = database == null
                ? BrokerOutboxStore.PendingOutboxMetrics.storageFailure()
                : database.observePendingBrokerOutbox();
        SchedulerOverloadSnapshot overload = Objects.requireNonNull(
                overloadSnapshot.get(),
                "overload snapshot"
        );
        boolean admissionBlocked = overload.reasons().stream()
                .map(SchedulerOverloadSnapshot.Pressure::reason)
                .anyMatch(CoordinatorReadinessProbe::blocksNewJobAdmission);
        return new CoordinatorHealth.ReadinessInputs(
                sqliteWritable,
                brokerUsable.getAsBoolean(),
                outbox.observed(),
                outbox.observed() ? outbox.count() : -1L,
                maxPendingOutboxRows,
                admissionBlocked,
                !capacityProjectionValid.getAsBoolean()
        );
    }

    private static boolean blocksNewJobAdmission(SchedulerOverloadSnapshot.Reason reason) {
        return reason == SchedulerOverloadSnapshot.Reason.MAX_ACTIVE_JOBS
                || reason == SchedulerOverloadSnapshot.Reason.MAX_ACTIVE_TASKS;
    }
}
