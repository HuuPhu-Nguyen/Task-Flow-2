package server.db;

import protocol.RequesterTokens;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

/**
 * SQLite binding for the reusable state-store contract.
 */
class SqlitePersistenceContractTest extends PersistenceContractTest {
    @Override
    protected StoreHandle openStore(Path location) throws Exception {
        DatabaseManager database = new DatabaseManager(location.toString());
        return storeHandle(database, database, database);
    }

    @Override
    protected int currentSchemaVersion() {
        return DatabaseManager.CURRENT_SCHEMA_VERSION;
    }

    @Override
    protected int schemaVersion(StoreHandle store) throws Exception {
        return ((DatabaseManager) store.state()).getSchemaVersion();
    }

    @Override
    protected MigrationSeed preparePreviousSchema(Path location)
            throws Exception {
        String jobId = "job-contract-v13-migration";
        String taskId = "task-job-contract-v13-migration-0";
        String payload = "migration-payload";
        String tokenHash = RequesterTokens.hashToken("migration-owner-token");
        String ownerKey = "migration-owner-key";
        String requestHash = "v1:migration-request";

        try (DatabaseManager database =
                     new DatabaseManager(location.toString())) {
            JobStateStore.JobSubmissionDecision committed =
                    database.commitJobSubmission(
                            jobId,
                            "TEST_TASK",
                            "requester-contract",
                            tokenHash,
                            ownerKey,
                            requestHash,
                            "",
                            List.of(new JobStateStore.TaskStartupState(
                                    taskId,
                                    payload
                            ))
                    );
            if (committed.outcome()
                    != JobStateStore.JobSubmissionOutcome.COMMITTED) {
                throw new IllegalStateException(
                        "Could not prepare the migration contract fixture"
                );
            }
        }

        try (Connection connection = DriverManager.getConnection(
                "jdbc:sqlite:" + location
        ); Statement statement = connection.createStatement()) {
            statement.execute("DROP INDEX idx_tasks_job_id");
            statement.execute("DROP INDEX idx_task_attempts_job_id");
            statement.execute("DROP INDEX idx_broker_outbox_pending");
            statement.execute(
                    "UPDATE schema_version SET version=13 WHERE id=1"
            );
        }

        return new MigrationSeed(
                13,
                jobId,
                taskId,
                payload,
                tokenHash,
                ownerKey,
                requestHash
        );
    }

    @Override
    protected AutoCloseable failOutboxWrites(Path location) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:sqlite:" + location
        ); Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TRIGGER contract_fail_broker_outbox_insert
                    BEFORE INSERT ON broker_outbox
                    BEGIN
                        SELECT RAISE(ABORT, 'contract injected outbox failure');
                    END
                    """);
        }
        return () -> {
            try (Connection connection = DriverManager.getConnection(
                    "jdbc:sqlite:" + location
            ); Statement statement = connection.createStatement()) {
                statement.execute(
                        "DROP TRIGGER contract_fail_broker_outbox_insert"
                );
            }
        };
    }
}
