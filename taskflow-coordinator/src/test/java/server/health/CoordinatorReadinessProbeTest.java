package server.health;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerOverloadSnapshot;
import transport.TransportRoute;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorReadinessProbeTest {
    @TempDir
    Path tempDir;

    @Test
    void actualSqliteOutboxAndSchedulerSignalsRecoverWithoutRestart() throws Exception {
        Path dbPath = tempDir.resolve("coordinator-readiness.db");
        AtomicBoolean brokerUsable = new AtomicBoolean(true);
        AtomicBoolean capacityProjectionValid = new AtomicBoolean(true);
        AtomicReference<SchedulerOverloadSnapshot> overload = new AtomicReference<>(
                overload()
        );
        SchedulerConfig config = SchedulerConfig.fromEnvironment(
                java.util.Map.of("TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "1")
        );
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            CoordinatorHealth health = new CoordinatorHealth();
            health.activate(
                    () -> true,
                    new CoordinatorReadinessProbe(
                            db,
                            brokerUsable::get,
                            overload::get,
                            capacityProjectionValid::get,
                            config
                    )
            );

            assertTrue(health.readiness().ready());

            BrokerOutboxStore.OutboxRecord pending = db.enqueueBrokerOutbox(
                    new BrokerOutboxStore.OutboxMessage(
                            TransportRoute.JOB_RESULT,
                            "requester-health",
                            "COORDINATOR",
                            new JobResultMessage(
                                    "COORDINATOR",
                                    "2026-07-27T00:00:00Z",
                                    "job-health",
                                    "TEST_TASK",
                                    false,
                                    List.of(),
                                    "health test"
                            )
                    )
            ).orElseThrow();
            assertReason(
                    health,
                    CoordinatorHealth.Reason.OUTBOX_THRESHOLD_REACHED
            );
            assertTrue(db.markBrokerOutboxPublished(pending.outboxId(), 100L));
            assertTrue(health.readiness().ready());

            installWriteFailure(dbPath);
            assertReason(health, CoordinatorHealth.Reason.SQLITE_NOT_WRITABLE);
            removeWriteFailure(dbPath);
            assertTrue(health.readiness().ready());

            brokerUsable.set(false);
            assertReason(health, CoordinatorHealth.Reason.BROKER_NOT_USABLE);
            brokerUsable.set(true);

            overload.set(overload(new SchedulerOverloadSnapshot.Pressure(
                    SchedulerOverloadSnapshot.Reason.MAX_ACTIVE_JOBS,
                    1L,
                    1L,
                    10L
            )));
            assertReason(
                    health,
                    CoordinatorHealth.Reason.SCHEDULER_ADMISSION_BLOCKED
            );
            overload.set(overload());

            capacityProjectionValid.set(false);
            assertReason(
                    health,
                    CoordinatorHealth.Reason.SCHEDULER_TERMINAL_OVERLOAD
            );
            capacityProjectionValid.set(true);
            assertTrue(health.readiness().ready());
        }
    }

    private static void assertReason(
            CoordinatorHealth health,
            CoordinatorHealth.Reason reason
    ) {
        CoordinatorHealth.ReadinessSnapshot snapshot = health.readiness();
        assertFalse(snapshot.ready());
        assertTrue(snapshot.degraded());
        assertTrue(snapshot.reasons().contains(reason));
    }

    private static SchedulerOverloadSnapshot overload(
            SchedulerOverloadSnapshot.Pressure... pressures
    ) {
        List<SchedulerOverloadSnapshot.Pressure> reasons = List.of(pressures);
        return new SchedulerOverloadSnapshot(
                !reasons.isEmpty(),
                reasons,
                reasons.isEmpty() ? null : reasons.getFirst().reason(),
                1,
                true,
                10L
        );
    }

    private static void installWriteFailure(Path dbPath) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TRIGGER fail_coordinator_readiness_write
                    BEFORE UPDATE OF applied_at ON schema_version
                    BEGIN
                        SELECT RAISE(ABORT, 'injected readiness write failure');
                    END
                    """);
        }
    }

    private static void removeWriteFailure(Path dbPath) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER fail_coordinator_readiness_write");
        }
    }
}
