package server.db;

import com.google.gson.Gson;
import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.MessageType;
import protocol.MessageValidator;
import protocol.PayloadLimits;
import protocol.PingMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.job.AssignmentIdentity;
import server.registry.PeerMetricsSnapshot;
import server.registry.PeerRegistryRecord;
import server.registry.PeerRegistryStore;
import server.registry.PeerStatus;
import server.registry.PeerTransport;
import server.runtime.UuidAssignmentIdGenerator;
import transport.TransportRoute;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class DatabaseManager implements JobStateStore, PeerRegistryStore, BrokerOutboxStore, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseManager.class);

    public static final String DB_PATH = "taskflow.db";
    public static final int CURRENT_SCHEMA_VERSION = 12;
    private static final int ASSIGNMENT_IDENTITY_SCHEMA_VERSION = 10;
    private static final int FINALIZATION_INTENT_SCHEMA_VERSION = 11;
    private static final Gson GSON = new Gson();

    private final Connection conn;

    public DatabaseManager() throws SQLException {
        this(DB_PATH);
    }

    public DatabaseManager(String dbPath) throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite:" + Objects.requireNonNull(dbPath, "dbPath"));
        try {
            configureConnection();
            initSchema();
        } catch (SQLException e) {
            try {
                conn.close();
            } catch (SQLException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    private void configureConnection() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA foreign_keys=ON");
            stmt.execute("PRAGMA journal_mode=WAL");
            try (ResultSet rs = stmt.executeQuery("PRAGMA foreign_keys")) {
                if (!rs.next() || rs.getInt(1) != 1) {
                    throw new SQLException("SQLite foreign-key enforcement could not be enabled.");
                }
            }
        }
    }

    private void initSchema() throws SQLException {
        boolean originalAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            createSchemaVersionTable();
            int version = readSchemaVersion();
            if (version > CURRENT_SCHEMA_VERSION) {
                throw new SQLException("Database schema version " + version
                        + " is newer than supported version " + CURRENT_SCHEMA_VERSION);
            }

            createJobsTable();
            ensureJobsTableColumns();
            if (tableExists("tasks")) {
                if (!tasksHaveJobForeignKey()) {
                    migrateTasksTableToForeignKey();
                }
                ensureTasksTableColumns();
            } else {
                createTasksTable();
            }
            createTaskAttemptsTable();
            if (version < ASSIGNMENT_IDENTITY_SCHEMA_VERSION) {
                migrateAssignmentIdentitySchema();
            }
            createPeerRegistryTable();
            createBrokerOutboxTable();
            if (version < FINALIZATION_INTENT_SCHEMA_VERSION) {
                migrateFinalizationIntentSchema();
            }

            writeSchemaVersion(CURRENT_SCHEMA_VERSION);
            validateCurrentSchema();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(originalAutoCommit);
        }
    }

    private void createSchemaVersionTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    id         INTEGER PRIMARY KEY CHECK (id = 1),
                    version    INTEGER NOT NULL CHECK (version >= 0),
                    applied_at INTEGER NOT NULL
                )
            """);
        }
    }

    private void createJobsTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id           TEXT    PRIMARY KEY,
                    task_type        TEXT    NOT NULL,
                    requester_node_id TEXT   NOT NULL,
                    requester_token_hash TEXT NOT NULL DEFAULT '',
                    requester_identity_key TEXT NOT NULL DEFAULT '',
                    status           TEXT    NOT NULL DEFAULT 'RUNNING',
                    submitted_at     INTEGER NOT NULL,
                    completed_at     INTEGER,
                    file_count       INTEGER NOT NULL,
                    parameter        TEXT    NOT NULL DEFAULT '',
                    request_hash     TEXT    NOT NULL DEFAULT '',
                    result_payload_json TEXT
                )
            """);
        }
    }

    private void createTasksTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE tasks (
                    task_id          TEXT    PRIMARY KEY,
                    job_id           TEXT    NOT NULL,
                    assigned_peer_id TEXT,
                    status           TEXT    NOT NULL DEFAULT 'PENDING',
                    started_at       INTEGER,
                    completed_at     INTEGER,
                    duration_ms      INTEGER,
                    retry_count      INTEGER NOT NULL DEFAULT 0,
                    attempt_number   INTEGER NOT NULL DEFAULT 0,
                    assignment_id    TEXT,
                    payload_json     TEXT,
                    result_payload_json TEXT,
                    lease_owner_id   TEXT    NOT NULL DEFAULT '',
                    lease_expires_at INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                )
            """);
        }
    }

    private void createPeerRegistryTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS peer_registry (
                    peer_id                   TEXT    PRIMARY KEY,
                    runtime_type              TEXT    NOT NULL DEFAULT 'PEER',
                    transport                 TEXT    NOT NULL DEFAULT 'UNKNOWN',
                    supported_task_types_json TEXT    NOT NULL DEFAULT '[]',
                    first_seen_at             INTEGER NOT NULL,
                    last_heartbeat_at         INTEGER NOT NULL DEFAULT 0,
                    last_disconnected_at      INTEGER NOT NULL DEFAULT 0,
                    status                    TEXT    NOT NULL,
                    completed_tasks           INTEGER NOT NULL DEFAULT 0,
                    failed_tasks              INTEGER NOT NULL DEFAULT 0,
                    latency_ewma_ms           INTEGER NOT NULL DEFAULT 0,
                    task_duration_ewma_ms     INTEGER NOT NULL DEFAULT 0
                )
            """);
        }
    }

    private void createTaskAttemptsTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS task_attempts (
                    attempt_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id         TEXT    NOT NULL,
                    task_id        TEXT    NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    assignment_id  TEXT,
                    peer_id        TEXT    NOT NULL,
                    started_at     INTEGER NOT NULL,
                    lease_expires_at INTEGER NOT NULL DEFAULT 0,
                    finished_at    INTEGER,
                    duration_ms    INTEGER,
                    outcome        TEXT    NOT NULL DEFAULT 'RUNNING',
                    failure_reason TEXT    NOT NULL DEFAULT '',
                    UNIQUE(task_id, attempt_number),
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
                )
            """);
        }
    }

    private void createBrokerOutboxTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS broker_outbox (
                    outbox_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    route           TEXT    NOT NULL,
                    peer_node_id    TEXT    NOT NULL DEFAULT '',
                    from_node_id    TEXT    NOT NULL,
                    message_type    TEXT    NOT NULL,
                    message_json    TEXT    NOT NULL,
                    created_at      INTEGER NOT NULL,
                    published_at    INTEGER,
                    attempt_count   INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at INTEGER NOT NULL DEFAULT 0,
                    last_error      TEXT    NOT NULL DEFAULT ''
                )
            """);
        }
    }

    private void ensureJobsTableColumns() throws SQLException {
        if (!columnExists("jobs", "parameter")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE jobs ADD COLUMN parameter TEXT NOT NULL DEFAULT ''");
            }
        }
        if (!columnExists("jobs", "requester_token_hash")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE jobs ADD COLUMN requester_token_hash TEXT NOT NULL DEFAULT ''");
            }
        }
        if (!columnExists("jobs", "requester_identity_key")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE jobs ADD COLUMN requester_identity_key TEXT NOT NULL DEFAULT ''");
            }
        }
        if (!columnExists("jobs", "result_payload_json")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE jobs ADD COLUMN result_payload_json TEXT");
            }
        }
        if (!columnExists("jobs", "request_hash")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE jobs ADD COLUMN request_hash TEXT NOT NULL DEFAULT ''");
            }
        }
    }

    private void ensureTasksTableColumns() throws SQLException {
        if (!columnExists("tasks", "payload_json")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE tasks ADD COLUMN payload_json TEXT");
            }
        }
        if (!columnExists("tasks", "result_payload_json")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE tasks ADD COLUMN result_payload_json TEXT");
            }
        }
        if (!columnExists("tasks", "lease_owner_id")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE tasks ADD COLUMN lease_owner_id TEXT NOT NULL DEFAULT ''");
            }
        }
        if (!columnExists("tasks", "lease_expires_at")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE tasks ADD COLUMN lease_expires_at INTEGER NOT NULL DEFAULT 0");
            }
        }
    }

    private void migrateAssignmentIdentitySchema() throws SQLException {
        if (!columnExists("tasks", "attempt_number")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE tasks ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 0");
            }
        }
        if (!columnExists("tasks", "assignment_id")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE tasks ADD COLUMN assignment_id TEXT");
            }
        }
        if (!columnExists("task_attempts", "assignment_id")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE task_attempts ADD COLUMN assignment_id TEXT");
            }
        }
        if (!columnExists("task_attempts", "lease_expires_at")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE task_attempts ADD COLUMN lease_expires_at INTEGER NOT NULL DEFAULT 0");
            }
        }
    }

    private void migrateFinalizationIntentSchema() throws SQLException {
        String sql = """
                UPDATE jobs
                SET status='FINALIZING'
                WHERE status='RUNNING'
                  AND file_count > 0
                  AND file_count = (
                      SELECT COUNT(*) FROM tasks WHERE tasks.job_id=jobs.job_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM tasks
                      WHERE tasks.job_id=jobs.job_id
                        AND (
                            tasks.status<>'COMPLETED'
                            OR tasks.result_payload_json IS NULL
                        )
                  )
                """;
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    private void migrateTasksTableToForeignKey() throws SQLException {
        int orphanRows = countOrphanTasks();
        if (orphanRows > 0) {
            throw new SQLException("Cannot migrate task schema because " + orphanRows + " orphan task rows exist.");
        }

        String backupTable = "tasks_without_fk_migration";
        if (tableExists(backupTable)) {
            throw new SQLException("Cannot migrate task schema because migration backup table already exists.");
        }

        boolean hasPayloadJson = columnExists("tasks", "payload_json");
        boolean hasResultPayloadJson = columnExists("tasks", "result_payload_json");
        boolean hasLeaseOwnerId = columnExists("tasks", "lease_owner_id");
        boolean hasLeaseExpiresAt = columnExists("tasks", "lease_expires_at");
        boolean hasAttemptNumber = columnExists("tasks", "attempt_number");
        boolean hasAssignmentId = columnExists("tasks", "assignment_id");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE tasks RENAME TO " + backupTable);
        }
        createTasksTable();
        String payloadJsonExpression = hasPayloadJson ? "payload_json" : "NULL";
        String resultPayloadJsonExpression = hasResultPayloadJson ? "result_payload_json" : "NULL";
        String leaseOwnerIdExpression = hasLeaseOwnerId ? "lease_owner_id" : "''";
        String leaseExpiresAtExpression = hasLeaseExpiresAt ? "lease_expires_at" : "0";
        String attemptNumberExpression = hasAttemptNumber ? "attempt_number" : "0";
        String assignmentIdExpression = hasAssignmentId ? "assignment_id" : "NULL";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                INSERT INTO tasks(
                    task_id,
                    job_id,
                    assigned_peer_id,
                    status,
                    started_at,
                    completed_at,
                    duration_ms,
                    retry_count,
                    payload_json,
                    result_payload_json,
                    lease_owner_id,
                    lease_expires_at,
                    attempt_number,
                    assignment_id
                )
                SELECT
                    task_id,
                    job_id,
                    assigned_peer_id,
                    status,
                    started_at,
                    completed_at,
                    duration_ms,
                    retry_count,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                FROM tasks_without_fk_migration
            """.formatted(
                    payloadJsonExpression,
                    resultPayloadJsonExpression,
                    leaseOwnerIdExpression,
                    leaseExpiresAtExpression,
                    attemptNumberExpression,
                    assignmentIdExpression
            ));
            stmt.execute("DROP TABLE " + backupTable);
        }
    }

    public synchronized int getSchemaVersion() throws SQLException {
        return readSchemaVersion();
    }

    private int readSchemaVersion() throws SQLException {
        String sql = "SELECT version FROM schema_version WHERE id=1";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                return 0;
            }
            return rs.getInt("version");
        }
    }

    private void writeSchemaVersion(int version) throws SQLException {
        String sql = """
                INSERT INTO schema_version(id, version, applied_at)
                VALUES(1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    version=excluded.version,
                    applied_at=excluded.applied_at
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, version);
            ps.setLong(2, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    private boolean tableExists(String tableName) throws SQLException {
        String sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean columnExists(String tableName, String columnName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {
            while (rs.next()) {
                if (columnName.equals(rs.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean tasksHaveJobForeignKey() throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("PRAGMA foreign_key_list(tasks)")) {
            while (rs.next()) {
                if ("jobs".equals(rs.getString("table"))
                        && "job_id".equals(rs.getString("from"))
                        && "job_id".equals(rs.getString("to"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private int countOrphanTasks() throws SQLException {
        String sql = """
                SELECT COUNT(*)
                FROM tasks
                WHERE NOT EXISTS (
                    SELECT 1 FROM jobs WHERE jobs.job_id = tasks.job_id
                )
                """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private void validateCurrentSchema() throws SQLException {
        int version = readSchemaVersion();
        if (version != CURRENT_SCHEMA_VERSION) {
            throw new SQLException("Database schema version " + version
                    + " does not match supported version " + CURRENT_SCHEMA_VERSION);
        }
        if (!tasksHaveJobForeignKey()) {
            throw new SQLException("Database schema is missing tasks.job_id foreign-key enforcement.");
        }
        if (!columnExists("jobs", "parameter")) {
            throw new SQLException("Database schema is missing jobs.parameter.");
        }
        if (!columnExists("jobs", "requester_token_hash")) {
            throw new SQLException("Database schema is missing jobs.requester_token_hash.");
        }
        if (!columnExists("jobs", "requester_identity_key")) {
            throw new SQLException("Database schema is missing jobs.requester_identity_key.");
        }
        if (!columnExists("jobs", "result_payload_json")) {
            throw new SQLException("Database schema is missing jobs.result_payload_json.");
        }
        if (!columnExists("jobs", "request_hash")) {
            throw new SQLException("Database schema is missing jobs.request_hash.");
        }
        if (!columnExists("tasks", "payload_json")
                || !columnExists("tasks", "result_payload_json")
                || !columnExists("tasks", "lease_owner_id")
                || !columnExists("tasks", "lease_expires_at")
                || !columnExists("tasks", "attempt_number")
                || !columnExists("tasks", "assignment_id")) {
            throw new SQLException("Database schema is missing persisted task payload columns.");
        }
        if (!tableExists("task_attempts")) {
            throw new SQLException("Database schema is missing task_attempts table.");
        }
        if (!columnExists("task_attempts", "job_id")
                || !columnExists("task_attempts", "task_id")
                || !columnExists("task_attempts", "attempt_number")
                || !columnExists("task_attempts", "assignment_id")
                || !columnExists("task_attempts", "peer_id")
                || !columnExists("task_attempts", "started_at")
                || !columnExists("task_attempts", "lease_expires_at")
                || !columnExists("task_attempts", "finished_at")
                || !columnExists("task_attempts", "duration_ms")
                || !columnExists("task_attempts", "outcome")
                || !columnExists("task_attempts", "failure_reason")) {
            throw new SQLException("Database schema is missing task attempt history columns.");
        }
        if (!tableExists("peer_registry")) {
            throw new SQLException("Database schema is missing peer_registry table.");
        }
        if (!columnExists("peer_registry", "runtime_type")
                || !columnExists("peer_registry", "transport")
                || !columnExists("peer_registry", "supported_task_types_json")
                || !columnExists("peer_registry", "first_seen_at")
                || !columnExists("peer_registry", "last_heartbeat_at")
                || !columnExists("peer_registry", "last_disconnected_at")
                || !columnExists("peer_registry", "status")
                || !columnExists("peer_registry", "completed_tasks")
                || !columnExists("peer_registry", "failed_tasks")
                || !columnExists("peer_registry", "latency_ewma_ms")
                || !columnExists("peer_registry", "task_duration_ewma_ms")) {
            throw new SQLException("Database schema is missing peer registry metadata columns.");
        }
        if (!tableExists("broker_outbox")) {
            throw new SQLException("Database schema is missing broker_outbox table.");
        }
        if (!columnExists("broker_outbox", "route")
                || !columnExists("broker_outbox", "peer_node_id")
                || !columnExists("broker_outbox", "from_node_id")
                || !columnExists("broker_outbox", "message_type")
                || !columnExists("broker_outbox", "message_json")
                || !columnExists("broker_outbox", "created_at")
                || !columnExists("broker_outbox", "published_at")
                || !columnExists("broker_outbox", "attempt_count")
                || !columnExists("broker_outbox", "last_attempt_at")
                || !columnExists("broker_outbox", "last_error")) {
            throw new SQLException("Database schema is missing broker outbox columns.");
        }
        if (countInvalidFinalizingJobs() > 0) {
            throw new SQLException(
                    "Database contains a FINALIZING job without a complete authoritative task set."
            );
        }
    }

    private int countInvalidFinalizingJobs() throws SQLException {
        String sql = """
                SELECT COUNT(*)
                FROM jobs
                WHERE status='FINALIZING'
                  AND (
                      file_count <= 0
                      OR file_count <> (
                          SELECT COUNT(*) FROM tasks WHERE tasks.job_id=jobs.job_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM tasks
                          WHERE tasks.job_id=jobs.job_id
                            AND (
                                tasks.status<>'COMPLETED'
                                OR tasks.result_payload_json IS NULL
                            )
                      )
                  )
                """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    // -------------------------------------------------------------------------
    // Write methods (called from coordinator / scheduler thread)
    // -------------------------------------------------------------------------

    public synchronized boolean insertJobWithTasks(String jobId,
                                                   String taskType,
                                                   String requesterId,
                                                   int fileCount,
                                                   Collection<String> taskIds) {
        return insertJobWithTasks(
                jobId,
                taskType,
                requesterId,
                "",
                taskIds.stream()
                        .map(taskId -> new TaskStartupState(taskId, null))
                        .toList()
        );
    }

    @Override
    public synchronized boolean insertJobWithTasks(String jobId,
                                                   String taskType,
                                                   String requesterId,
                                                   String requesterTokenHash,
                                                   String requesterIdentityKey,
                                                   String parameter,
                                                   Collection<TaskStartupState> tasks) {
        return commitJobSubmissionInternal(
                jobId,
                taskType,
                requesterId,
                requesterTokenHash,
                requesterIdentityKey,
                "",
                parameter,
                tasks,
                false
        ).outcome() == JobSubmissionOutcome.COMMITTED;
    }

    @Override
    public synchronized JobSubmissionDecision inspectJobSubmission(String jobId,
                                                                   String requesterTokenHash,
                                                                   String requesterIdentityKey,
                                                                   String requestHash) {
        if (jobId == null || jobId.isBlank()) {
            return JobSubmissionDecision.newSubmission();
        }
        try {
            return classifyJobSubmission(
                    jobId,
                    requesterTokenHash,
                    requesterIdentityKey,
                    requestHash
            );
        } catch (SQLException e) {
            logSqlFailure("inspectJobSubmission", e);
            return JobSubmissionDecision.storageFailure();
        }
    }

    @Override
    public synchronized JobSubmissionDecision commitJobSubmission(String jobId,
                                                                  String taskType,
                                                                  String requesterId,
                                                                  String requesterTokenHash,
                                                                  String requesterIdentityKey,
                                                                  String requestHash,
                                                                  String parameter,
                                                                  Collection<TaskStartupState> tasks) {
        return commitJobSubmissionInternal(
                jobId,
                taskType,
                requesterId,
                requesterTokenHash,
                requesterIdentityKey,
                requestHash,
                parameter,
                tasks,
                true
        );
    }

    private JobSubmissionDecision commitJobSubmissionInternal(String jobId,
                                                              String taskType,
                                                              String requesterId,
                                                              String requesterTokenHash,
                                                              String requesterIdentityKey,
                                                              String requestHash,
                                                              String parameter,
                                                              Collection<TaskStartupState> tasks,
                                                              boolean requireRequestHash) {
        if (requireRequestHash && (requestHash == null || requestHash.isBlank())) {
            LOGGER.error("event=sqlite_operation_failed operation=commitJobSubmission error=missing_request_hash");
            return JobSubmissionDecision.storageFailure();
        }
        List<TaskStartupState> taskStates = List.copyOf(tasks);
        String insertJobSql = """
                INSERT INTO jobs(
                    job_id,
                    task_type,
                    requester_node_id,
                    requester_token_hash,
                    requester_identity_key,
                    status,
                    submitted_at,
                    file_count,
                    parameter,
                    request_hash
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """;
        String insertTaskSql = "INSERT INTO tasks(task_id,job_id,status,payload_json) VALUES(?,?,?,?)";
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement job = conn.prepareStatement(insertJobSql);
                 PreparedStatement task = conn.prepareStatement(insertTaskSql)) {
                JobSubmissionDecision existing = classifyJobSubmission(
                        jobId,
                        requesterTokenHash,
                        requesterIdentityKey,
                        requestHash
                );
                if (existing.outcome() != JobSubmissionOutcome.NEW_SUBMISSION) {
                    conn.rollback();
                    return existing;
                }

                job.setString(1, jobId);
                job.setString(2, taskType);
                job.setString(3, requesterId);
                job.setString(4, requesterTokenHash == null ? "" : requesterTokenHash);
                job.setString(5, requesterIdentityKey == null ? "" : requesterIdentityKey);
                job.setString(6, "RUNNING");
                job.setLong(7, System.currentTimeMillis());
                job.setInt(8, taskStates.size());
                job.setString(9, parameter == null ? "" : parameter);
                job.setString(10, requestHash == null ? "" : requestHash);
                if (job.executeUpdate() <= 0) {
                    throw new SQLException("No job row inserted.");
                }

                for (TaskStartupState taskState : taskStates) {
                    task.setString(1, taskState.taskId());
                    task.setString(2, jobId);
                    task.setString(3, "PENDING");
                    task.setString(4, toJson(taskState.payload()));
                    if (task.executeUpdate() <= 0) {
                        throw new SQLException("No task row inserted for task " + taskState.taskId());
                    }
                }

                conn.commit();
                return JobSubmissionDecision.committed(taskType);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure(requireRequestHash ? "commitJobSubmission" : "insertJobWithTasks", e);
            return JobSubmissionDecision.storageFailure();
        }
    }

    private JobSubmissionDecision classifyJobSubmission(String jobId,
                                                        String requesterTokenHash,
                                                        String requesterIdentityKey,
                                                        String requestHash) throws SQLException {
        String sql = """
                SELECT task_type,
                       status,
                       requester_token_hash,
                       requester_identity_key,
                       request_hash
                FROM jobs
                WHERE job_id=?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return JobSubmissionDecision.newSubmission();
                }

                String status = rs.getString("status");
                String taskType = rs.getString("task_type");
                if (!secureEquals(rs.getString("requester_token_hash"), requesterTokenHash)
                        || !Objects.equals(
                                value(rs.getString("requester_identity_key")),
                                value(requesterIdentityKey)
                        )) {
                    return new JobSubmissionDecision(
                            JobSubmissionOutcome.OWNER_CONFLICT,
                            status,
                            taskType
                    );
                }

                String storedRequestHash = rs.getString("request_hash");
                if (storedRequestHash == null || storedRequestHash.isBlank()) {
                    return new JobSubmissionDecision(
                            JobSubmissionOutcome.LEGACY_CONFLICT,
                            status,
                            taskType
                    );
                }
                if (!secureEquals(storedRequestHash, requestHash)) {
                    return new JobSubmissionDecision(
                            JobSubmissionOutcome.REQUEST_CONFLICT,
                            status,
                            taskType
                    );
                }
                return new JobSubmissionDecision(JobSubmissionOutcome.REPLAY, status, taskType);
            }
        }
    }

    private static boolean secureEquals(String left, String right) {
        return MessageDigest.isEqual(
                value(left).getBytes(StandardCharsets.UTF_8),
                value(right).getBytes(StandardCharsets.UTF_8)
        );
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }

    public synchronized boolean insertJob(String jobId, String taskType, String requesterId, int fileCount) {
        String sql = """
                INSERT INTO jobs(
                    job_id,
                    task_type,
                    requester_node_id,
                    requester_token_hash,
                    requester_identity_key,
                    status,
                    submitted_at,
                    file_count,
                    parameter
                )
                VALUES(?,?,?,?,?,?,?,?,?)
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            ps.setString(2, taskType);
            ps.setString(3, requesterId);
            ps.setString(4, "");
            ps.setString(5, "");
            ps.setString(6, "RUNNING");
            ps.setLong(7, System.currentTimeMillis());
            ps.setInt(8, fileCount);
            ps.setString(9, "");
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("insertJob", e);
            return false;
        }
    }

    public synchronized boolean insertTask(String taskId, String jobId) {
        String sql = "INSERT INTO tasks(task_id,job_id,status) VALUES(?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setString(2, jobId);
            ps.setString(3, "PENDING");
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("insertTask", e);
            return false;
        }
    }

    @Override
    public synchronized boolean hasJob(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return false;
        }
        String sql = "SELECT 1 FROM jobs WHERE job_id=? LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            logSqlFailure("hasJob", e);
            return false;
        }
    }

    public synchronized boolean markTaskAssigned(String taskId, String peerId, long startedAt) {
        return markTaskAssigned(taskId, peerId, startedAt, "", 0L);
    }

    @Override
    public synchronized boolean markTaskAssigned(String taskId,
                                                 String peerId,
                                                 long startedAt,
                                                 String leaseOwnerId,
                                                 long leaseExpiresAt) {
        return persistTaskAssignment(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                null
        ) == DurableTransitionOutcome.COMMITTED;
    }

    @Override
    public synchronized boolean markTaskAssigned(String taskId,
                                                 String peerId,
                                                 long startedAt,
                                                 String leaseOwnerId,
                                                 long leaseExpiresAt,
                                                 int attemptNumber,
                                                 String assignmentId) {
        AssignmentIdentity identity;
        try {
            identity = new AssignmentIdentity(
                    taskId,
                    attemptNumber,
                    assignmentId,
                    peerId,
                    leaseExpiresAt
            );
        } catch (IllegalArgumentException e) {
            LOGGER.warn("event=task_assignment_persistence_rejected task_id={} reason=invalid_assignment_identity error={}",
                    taskId, e.getMessage());
            return false;
        }
        return persistTaskAssignment(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                identity
        ) == DurableTransitionOutcome.COMMITTED;
    }

    @Override
    public synchronized DurableTransitionOutcome commitTaskAssignment(String taskId,
                                                                       String peerId,
                                                                       long startedAt,
                                                                       String leaseOwnerId,
                                                                       long leaseExpiresAt,
                                                                       int attemptNumber,
                                                                       String assignmentId) {
        AssignmentIdentity identity;
        try {
            identity = new AssignmentIdentity(
                    taskId,
                    attemptNumber,
                    assignmentId,
                    peerId,
                    leaseExpiresAt
            );
        } catch (IllegalArgumentException e) {
            LOGGER.warn("event=task_assignment_persistence_rejected task_id={} reason=invalid_assignment_identity error={}",
                    taskId, e.getMessage());
            return DurableTransitionOutcome.STALE_STATE;
        }
        return persistTaskAssignment(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                identity
        );
    }

    private DurableTransitionOutcome persistTaskAssignment(String taskId,
                                                            String peerId,
                                                            long startedAt,
                                                            String leaseOwnerId,
                                                            long leaseExpiresAt,
                                                            AssignmentIdentity suppliedIdentity) {
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                AssignmentIdentity identity = suppliedIdentity == null
                        ? nextAssignmentIdentity(taskId, peerId, leaseExpiresAt)
                        : suppliedIdentity;
                if (identity == null || !markTaskAssignedInCurrentTransaction(
                        taskId,
                        startedAt,
                        leaseOwnerId,
                        identity
                )) {
                    DurableTransitionOutcome outcome = identity == null
                            ? classifyMissingAssignmentTarget(taskId)
                            : classifyTaskAssignment(identity, startedAt, leaseOwnerId);
                    conn.rollback();
                    return outcome;
                }
                conn.commit();
                return DurableTransitionOutcome.COMMITTED;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("markTaskAssigned", e);
            return DurableTransitionOutcome.STORAGE_FAILURE;
        }
    }

    private DurableTransitionOutcome classifyMissingAssignmentTarget(String taskId) throws SQLException {
        return loadPersistedTaskIdentity(taskId).isEmpty()
                ? DurableTransitionOutcome.UNKNOWN_ENTITY
                : DurableTransitionOutcome.STALE_STATE;
    }

    private DurableTransitionOutcome classifyTaskAssignment(AssignmentIdentity requested,
                                                              long startedAt,
                                                              String leaseOwnerId) throws SQLException {
        Optional<PersistedTaskIdentity> stored = loadPersistedTaskIdentity(requested.taskId());
        if (stored.isEmpty()) {
            return DurableTransitionOutcome.UNKNOWN_ENTITY;
        }
        PersistedTaskIdentity identity = stored.get();
        boolean exactAssignment = "ASSIGNED".equals(identity.status())
                && identity.attemptNumber() == requested.attemptNumber()
                && Objects.equals(identity.assignmentId(), requested.assignmentId())
                && Objects.equals(identity.assignedPeerId(), requested.workerId())
                && identity.startedAt() == startedAt
                && Objects.equals(identity.leaseOwnerId(), leaseOwnerId == null ? "" : leaseOwnerId)
                && identity.leaseExpiresAt() == requested.leaseExpiresAtEpochMillis();
        return exactAssignment
                ? DurableTransitionOutcome.ALREADY_APPLIED
                : DurableTransitionOutcome.STALE_STATE;
    }

    private AssignmentIdentity nextAssignmentIdentity(String taskId,
                                                       String peerId,
                                                       long leaseExpiresAt) throws SQLException {
        return nextAssignmentIdentity(
                taskId,
                peerId,
                leaseExpiresAt,
                UuidAssignmentIdGenerator.INSTANCE.nextAssignmentId()
        );
    }

    private AssignmentIdentity nextAssignmentIdentity(String taskId,
                                                       String peerId,
                                                       long leaseExpiresAt,
                                                       String assignmentId) throws SQLException {
        String sql = "SELECT attempt_number FROM tasks WHERE task_id=? AND status='PENDING'";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                int nextAttemptNumber;
                try {
                    nextAttemptNumber = Math.incrementExact(rs.getInt("attempt_number"));
                } catch (ArithmeticException e) {
                    throw new SQLException("Assignment attempt number overflow for task " + taskId, e);
                }
                return new AssignmentIdentity(
                        taskId,
                        nextAttemptNumber,
                        assignmentId,
                        peerId,
                        leaseExpiresAt
                );
            }
        }
    }

    private boolean markTaskAssignedInCurrentTransaction(String taskId,
                                                         long startedAt,
                                                         String leaseOwnerId,
                                                         AssignmentIdentity identity) throws SQLException {
        String updateTaskSql = """
                UPDATE tasks
                SET status='ASSIGNED',
                    assigned_peer_id=?,
                    started_at=?,
                    lease_owner_id=?,
                    lease_expires_at=?,
                    attempt_number=?,
                    assignment_id=?
                WHERE task_id=? AND status='PENDING' AND attempt_number < ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(updateTaskSql)) {
            ps.setString(1, identity.workerId());
            ps.setLong(2, startedAt);
            ps.setString(3, leaseOwnerId == null ? "" : leaseOwnerId);
            ps.setLong(4, identity.leaseExpiresAtEpochMillis());
            ps.setInt(5, identity.attemptNumber());
            ps.setString(6, identity.assignmentId());
            ps.setString(7, taskId);
            ps.setInt(8, identity.attemptNumber());
            return ps.executeUpdate() > 0 && insertTaskAttempt(identity, startedAt);
        }
    }

    public synchronized boolean markTaskCompleted(String taskId, long completedAt, long durationMs) {
        return markTaskCompleted(taskId, completedAt, durationMs, null);
    }

    @Override
    public synchronized boolean markTaskCompleted(String taskId,
                                                 long completedAt,
                                                 long durationMs,
                                                 Object resultPayload) {
        try {
            PersistedTaskIdentity identity = loadPersistedTaskIdentity(taskId).orElse(null);
            if (identity == null
                    || !"ASSIGNED".equals(identity.status())
                    || identity.attemptNumber() < 1
                    || identity.assignmentId() == null
                    || identity.assignedPeerId() == null) {
                return false;
            }
            return commitTaskResult(
                    taskId,
                    identity.attemptNumber(),
                    identity.assignmentId(),
                    identity.assignedPeerId(),
                    completedAt,
                    durationMs,
                    resultPayload
            ) == ResultCommitOutcome.COMMITTED;
        } catch (SQLException e) {
            logSqlFailure("markTaskCompleted", e);
            return false;
        }
    }

    @Override
    public synchronized ResultCommitOutcome commitTaskResult(String taskId,
                                                             int attemptNumber,
                                                             String assignmentId,
                                                             String assignedPeerId,
                                                             long completedAt,
                                                             long durationMs,
                                                             Object resultPayload) {
        String sql = """
                UPDATE tasks
                SET status='COMPLETED',
                    completed_at=?,
                    duration_ms=?,
                    result_payload_json=?,
                    lease_owner_id='',
                    lease_expires_at=0
                WHERE task_id=?
                  AND status='ASSIGNED'
                  AND attempt_number=?
                  AND assignment_id=?
                  AND assigned_peer_id=?
                  AND EXISTS (
                      SELECT 1
                      FROM jobs
                      WHERE jobs.job_id=tasks.job_id
                        AND jobs.status='RUNNING'
                  )
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ResultCommitOutcome referenceOwnership =
                        validateTaskOutputReferenceOwnership(
                                taskId,
                                attemptNumber,
                                assignmentId,
                                resultPayload
                        );
                if (referenceOwnership != null) {
                    conn.rollback();
                    return referenceOwnership;
                }
                long normalizedDuration = Math.max(0L, durationMs);
                ps.setLong(1, completedAt);
                ps.setLong(2, normalizedDuration);
                ps.setString(3, toTaskResultJson(resultPayload));
                ps.setString(4, taskId);
                ps.setInt(5, attemptNumber);
                ps.setString(6, assignmentId);
                ps.setString(7, assignedPeerId);
                if (ps.executeUpdate() == 0) {
                    ResultCommitOutcome outcome = classifyResultCommit(
                            taskId,
                            attemptNumber,
                            assignmentId,
                            assignedPeerId
                    );
                    conn.rollback();
                    return outcome;
                }
                if (!finishExactRunningAttempt(
                        taskId,
                        attemptNumber,
                        assignmentId,
                        assignedPeerId,
                        completedAt,
                        normalizedDuration
                )) {
                    conn.rollback();
                    LOGGER.warn(
                            "event=result_commit_storage_failure task_id={} attempt_number={} "
                                    + "assignment_id={} assigned_peer_id={} reason=attempt_audit_mismatch",
                            taskId,
                            attemptNumber,
                            assignmentId,
                            assignedPeerId
                    );
                    return ResultCommitOutcome.STORAGE_FAILURE;
                }
                if (!persistJobFinalizationIntentIfReady(taskId)) {
                    conn.rollback();
                    LOGGER.warn(
                            "event=result_commit_storage_failure task_id={} attempt_number={} "
                                    + "assignment_id={} assigned_peer_id={} "
                                    + "reason=job_finalization_intent_mismatch",
                            taskId,
                            attemptNumber,
                            assignmentId,
                            assignedPeerId
                    );
                    return ResultCommitOutcome.STORAGE_FAILURE;
                }
                conn.commit();
                return ResultCommitOutcome.COMMITTED;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException sqlException) {
                logSqlFailure("commitTaskResult", sqlException);
            } else {
                LOGGER.warn(
                        "event=database_operation_failed operation=commitTaskResult error_type={} error={}",
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
            }
            return ResultCommitOutcome.STORAGE_FAILURE;
        }
    }

    private boolean persistJobFinalizationIntentIfReady(String taskId) throws SQLException {
        String updateSql = """
                UPDATE jobs
                SET status='FINALIZING'
                WHERE status='RUNNING'
                  AND job_id=(SELECT job_id FROM tasks WHERE task_id=?)
                  AND file_count > 0
                  AND file_count = (
                      SELECT COUNT(*) FROM tasks WHERE tasks.job_id=jobs.job_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM tasks
                      WHERE tasks.job_id=jobs.job_id
                        AND (
                            tasks.status<>'COMPLETED'
                            OR tasks.result_payload_json IS NULL
                        )
                  )
                """;
        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            ps.setString(1, taskId);
            ps.executeUpdate();
        }

        String stateSql = """
                SELECT jobs.status,
                       jobs.file_count,
                       COUNT(job_tasks.task_id) AS task_count,
                       SUM(CASE WHEN job_tasks.status='COMPLETED' THEN 1 ELSE 0 END)
                           AS completed_count,
                       SUM(CASE WHEN job_tasks.result_payload_json IS NOT NULL THEN 1 ELSE 0 END)
                           AS durable_result_count
                FROM tasks anchor
                JOIN jobs ON jobs.job_id=anchor.job_id
                LEFT JOIN tasks job_tasks ON job_tasks.job_id=jobs.job_id
                WHERE anchor.task_id=?
                GROUP BY jobs.job_id, jobs.status, jobs.file_count
                """;
        try (PreparedStatement ps = conn.prepareStatement(stateSql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return false;
                }
                int expectedTasks = rs.getInt("file_count");
                int taskCount = rs.getInt("task_count");
                int completedCount = rs.getInt("completed_count");
                int durableResultCount = rs.getInt("durable_result_count");
                boolean validTaskSet = expectedTasks > 0
                        && taskCount == expectedTasks
                        && completedCount >= 0
                        && completedCount <= expectedTasks
                        && durableResultCount == completedCount;
                if (!validTaskSet) {
                    return false;
                }
                boolean allTasksCompleted = completedCount == expectedTasks;
                return allTasksCompleted
                        ? "FINALIZING".equals(rs.getString("status"))
                        : "RUNNING".equals(rs.getString("status"));
            }
        }
    }

    public synchronized boolean markTaskRetried(String taskId, int retryCount) {
        return markTaskRetried(
                taskId,
                retryCount,
                TaskAttemptOutcome.RETRY_SCHEDULED,
                "",
                System.currentTimeMillis()
        );
    }

    @Override
    public synchronized boolean markTaskRetried(String taskId,
                                               int retryCount,
                                               TaskAttemptOutcome outcome,
                                               String failureReason,
                                               long finishedAt) {
        String sql = """
                UPDATE tasks
                SET status='PENDING',
                    assigned_peer_id=NULL,
                    assignment_id=NULL,
                    started_at=NULL,
                    completed_at=NULL,
                    duration_ms=NULL,
                    lease_owner_id='',
                    lease_expires_at=0,
                    retry_count=?
                WHERE task_id=? AND status='ASSIGNED'
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (!finishRunningAttempt(
                        taskId,
                        finishedAt,
                        attemptDuration(taskId, finishedAt),
                        normalizeFailureOutcome(outcome, TaskAttemptOutcome.RETRY_SCHEDULED),
                        failureReason
                )) {
                    conn.rollback();
                    return false;
                }
                ps.setInt(1, retryCount);
                ps.setString(2, taskId);
                if (ps.executeUpdate() <= 0) {
                    conn.rollback();
                    return false;
                }
                conn.commit();
                return true;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("markTaskRetried", e);
            return false;
        }
    }

    public synchronized boolean markTaskFailed(String taskId) {
        return markTaskFailed(
                taskId,
                TaskAttemptOutcome.TERMINAL_FAILURE,
                "",
                System.currentTimeMillis()
        );
    }

    @Override
    public synchronized boolean markTaskFailed(String taskId,
                                               TaskAttemptOutcome outcome,
                                               String failureReason,
                                              long finishedAt) {
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                if (!markTaskFailedInCurrentTransaction(taskId, outcome, failureReason, finishedAt)) {
                    conn.rollback();
                    return false;
                }
                conn.commit();
                return true;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("markTaskFailed", e);
            return false;
        }
    }

    @Override
    public synchronized DurableTransitionOutcome commitAssignedTaskFailure(String taskId,
                                                                            int attemptNumber,
                                                                            String assignmentId,
                                                                            String assignedPeerId,
                                                                            int retryCount,
                                                                            TaskAttemptOutcome outcome,
                                                                            String failureReason,
                                                                            long finishedAt) {
        TaskAttemptOutcome normalizedOutcome = normalizeAssignedFailureOutcome(outcome);
        if (normalizedOutcome == null || retryCount < 0) {
            return DurableTransitionOutcome.STALE_STATE;
        }
        boolean terminal = normalizedOutcome == TaskAttemptOutcome.TERMINAL_FAILURE;
        String updateTaskSql = terminal
                ? """
                  UPDATE tasks
                  SET status='FAILED',
                      completed_at=?,
                      lease_owner_id='',
                      lease_expires_at=0,
                      retry_count=?
                  WHERE task_id=?
                    AND status='ASSIGNED'
                    AND attempt_number=?
                    AND assignment_id=?
                    AND assigned_peer_id=?
                  """
                : """
                  UPDATE tasks
                  SET status='PENDING',
                      assigned_peer_id=NULL,
                      assignment_id=NULL,
                      started_at=NULL,
                      completed_at=NULL,
                      duration_ms=NULL,
                      lease_owner_id='',
                      lease_expires_at=0,
                      retry_count=?
                  WHERE task_id=?
                    AND status='ASSIGNED'
                    AND attempt_number=?
                    AND assignment_id=?
                    AND assigned_peer_id=?
                  """;

        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(updateTaskSql)) {
                int parameter = 1;
                if (terminal) {
                    ps.setLong(parameter++, finishedAt);
                }
                ps.setInt(parameter++, retryCount);
                ps.setString(parameter++, taskId);
                ps.setInt(parameter++, attemptNumber);
                ps.setString(parameter++, assignmentId);
                ps.setString(parameter, assignedPeerId);
                if (ps.executeUpdate() == 0) {
                    DurableTransitionOutcome classified = classifyAssignedTaskFailure(
                            taskId,
                            attemptNumber,
                            assignmentId,
                            assignedPeerId,
                            retryCount,
                            normalizedOutcome
                    );
                    conn.rollback();
                    return classified;
                }
                if (!finishExactRunningAttempt(
                        taskId,
                        attemptNumber,
                        assignmentId,
                        assignedPeerId,
                        finishedAt,
                        exactAttemptDuration(
                                taskId,
                                attemptNumber,
                                assignmentId,
                                assignedPeerId,
                                finishedAt
                        ),
                        normalizedOutcome,
                        failureReason
                )) {
                    conn.rollback();
                    return DurableTransitionOutcome.STORAGE_FAILURE;
                }
                conn.commit();
                return DurableTransitionOutcome.COMMITTED;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException sqlException) {
                logSqlFailure("commitAssignedTaskFailure", sqlException);
            } else {
                LOGGER.warn(
                        "event=database_operation_failed operation=commitAssignedTaskFailure error_type={} error={}",
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
            }
            return DurableTransitionOutcome.STORAGE_FAILURE;
        }
    }

    private DurableTransitionOutcome classifyAssignedTaskFailure(String taskId,
                                                                  int attemptNumber,
                                                                  String assignmentId,
                                                                  String assignedPeerId,
                                                                  int retryCount,
                                                                  TaskAttemptOutcome outcome) throws SQLException {
        Optional<PersistedTaskIdentity> stored = loadPersistedTaskIdentity(taskId);
        if (stored.isEmpty()) {
            return DurableTransitionOutcome.UNKNOWN_ENTITY;
        }
        PersistedTaskIdentity identity = stored.get();
        boolean expectedTaskState = outcome == TaskAttemptOutcome.TERMINAL_FAILURE
                ? "FAILED".equals(identity.status()) && identity.retryCount() == retryCount
                : "PENDING".equals(identity.status()) && identity.retryCount() == retryCount;
        if (expectedTaskState
                && identity.attemptNumber() == attemptNumber
                && exactAttemptHasOutcome(
                taskId,
                attemptNumber,
                assignmentId,
                assignedPeerId,
                outcome
        )) {
            return DurableTransitionOutcome.ALREADY_APPLIED;
        }
        return DurableTransitionOutcome.STALE_STATE;
    }

    private static TaskAttemptOutcome normalizeAssignedFailureOutcome(TaskAttemptOutcome outcome) {
        if (outcome == TaskAttemptOutcome.RETRY_SCHEDULED
                || outcome == TaskAttemptOutcome.DISPATCH_FAILED
                || outcome == TaskAttemptOutcome.TERMINAL_FAILURE) {
            return outcome;
        }
        return null;
    }

    private boolean markTaskFailedInCurrentTransaction(String taskId,
                                                       TaskAttemptOutcome outcome,
                                                       String failureReason,
                                                       long finishedAt) throws SQLException {
        String sql = """
                UPDATE tasks
                SET status='FAILED',
                    completed_at=?,
                    lease_owner_id='',
                    lease_expires_at=0
                WHERE task_id=? AND status NOT IN ('COMPLETED', 'FAILED')
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (hasRunningAttempt(taskId)
                    && !finishRunningAttempt(
                    taskId,
                    finishedAt,
                    attemptDuration(taskId, finishedAt),
                    normalizeFailureOutcome(outcome, TaskAttemptOutcome.TERMINAL_FAILURE),
                    failureReason
            )) {
                return false;
            }
            ps.setLong(1, finishedAt);
            ps.setString(2, taskId);
            return ps.executeUpdate() > 0;
        }
    }

    public synchronized boolean markJobCompleted(String jobId) {
        return markJobCompleted(jobId, null);
    }

    @Override
    public synchronized boolean markJobCompleted(String jobId, Object resultPayload) {
        try {
            return markJobCompletedInCurrentTransaction(jobId, resultPayload, System.currentTimeMillis());
        } catch (SQLException e) {
            logSqlFailure("markJobCompleted", e);
            return false;
        }
    }

    public synchronized boolean markJobFailed(String jobId) {
        try {
            return markJobFailedInCurrentTransaction(jobId, System.currentTimeMillis());
        } catch (SQLException e) {
            logSqlFailure("markJobFailed", e);
            return false;
        }
    }

    @Override
    public synchronized DurableTransitionOutcome commitJobCompleted(String jobId,
                                                                    Object resultPayload,
                                                                    long completedAt) {
        try {
            if (markJobCompletedInCurrentTransaction(jobId, resultPayload, completedAt)) {
                return DurableTransitionOutcome.COMMITTED;
            }
            return classifyJobCompletion(jobId, resultPayload);
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException sqlException) {
                logSqlFailure("commitJobCompleted", sqlException);
            } else {
                LOGGER.warn(
                        "event=database_operation_failed operation=commitJobCompleted error_type={} error={}",
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
            }
            return DurableTransitionOutcome.STORAGE_FAILURE;
        }
    }

    @Override
    public synchronized DurableTransitionOutcome commitJobFailed(
            String jobId,
            Collection<JobStateStore.TaskFailureUpdate> taskFailures,
            long completedAt) {
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                Collection<JobStateStore.TaskFailureUpdate> failures = taskFailures == null
                        ? List.of()
                        : taskFailures;
                DurableTransitionOutcome current = classifyRunningJob(jobId);
                if (current != DurableTransitionOutcome.COMMITTED) {
                    if (current == DurableTransitionOutcome.ALREADY_APPLIED) {
                        current = classifyCommittedJobFailure(jobId, failures);
                    }
                    conn.rollback();
                    return current;
                }
                for (JobStateStore.TaskFailureUpdate failure : failures) {
                    if (!markTaskFailedInCurrentTransaction(
                            failure.taskId(),
                            failure.outcome(),
                            failure.failureReason(),
                            failure.finishedAt()
                    )) {
                        String status = taskStatus(failure.taskId());
                        if ("FAILED".equals(status)) {
                            continue;
                        }
                        conn.rollback();
                        return status.isEmpty()
                                ? DurableTransitionOutcome.UNKNOWN_ENTITY
                                : DurableTransitionOutcome.STALE_STATE;
                    }
                }
                if (!markJobFailedInCurrentTransaction(jobId, completedAt)) {
                    DurableTransitionOutcome classified = classifyJobTransition(jobId, "FAILED");
                    conn.rollback();
                    return classified;
                }
                conn.commit();
                return DurableTransitionOutcome.COMMITTED;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException sqlException) {
                logSqlFailure("commitJobFailed", sqlException);
            } else {
                LOGGER.warn(
                        "event=database_operation_failed operation=commitJobFailed error_type={} error={}",
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
            }
            return DurableTransitionOutcome.STORAGE_FAILURE;
        }
    }

    private DurableTransitionOutcome classifyCommittedJobFailure(
            String jobId,
            Collection<JobStateStore.TaskFailureUpdate> failures) throws SQLException {
        String sql = "SELECT status FROM tasks WHERE task_id=? AND job_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (JobStateStore.TaskFailureUpdate failure : failures) {
                ps.setString(1, failure.taskId());
                ps.setString(2, jobId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return DurableTransitionOutcome.UNKNOWN_ENTITY;
                    }
                    if (!"FAILED".equals(rs.getString("status"))) {
                        return DurableTransitionOutcome.STALE_STATE;
                    }
                }
            }
        }
        return DurableTransitionOutcome.ALREADY_APPLIED;
    }

    private DurableTransitionOutcome classifyRunningJob(String jobId) throws SQLException {
        String status = jobStatus(jobId);
        if (status.isEmpty()) {
            return DurableTransitionOutcome.UNKNOWN_ENTITY;
        }
        if ("RUNNING".equals(status)) {
            return DurableTransitionOutcome.COMMITTED;
        }
        return "FAILED".equals(status)
                ? DurableTransitionOutcome.ALREADY_APPLIED
                : DurableTransitionOutcome.STALE_STATE;
    }

    private DurableTransitionOutcome classifyJobTransition(String jobId,
                                                            String expectedStatus) throws SQLException {
        String status = jobStatus(jobId);
        if (status.isEmpty()) {
            return DurableTransitionOutcome.UNKNOWN_ENTITY;
        }
        return expectedStatus.equals(status)
                ? DurableTransitionOutcome.ALREADY_APPLIED
                : DurableTransitionOutcome.STALE_STATE;
    }

    private DurableTransitionOutcome classifyJobCompletion(String jobId,
                                                             Object resultPayload) throws SQLException {
        String sql = "SELECT status, result_payload_json FROM jobs WHERE job_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return DurableTransitionOutcome.UNKNOWN_ENTITY;
                }
                boolean exactCompletion = "COMPLETED".equals(rs.getString("status"))
                        && Objects.equals(rs.getString("result_payload_json"), toJson(resultPayload));
                return exactCompletion
                        ? DurableTransitionOutcome.ALREADY_APPLIED
                        : DurableTransitionOutcome.STALE_STATE;
            }
        }
    }

    private String jobStatus(String jobId) throws SQLException {
        String sql = "SELECT status FROM jobs WHERE job_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("status") : "";
            }
        }
    }

    private boolean markJobCompletedInCurrentTransaction(String jobId,
                                                         Object resultPayload,
                                                         long completedAt) throws SQLException {
        String sql = """
                UPDATE jobs
                SET status='COMPLETED',
                    completed_at=?,
                    result_payload_json=?
                WHERE job_id=?
                  AND status IN ('RUNNING', 'FINALIZING')
                  AND file_count > 0
                  AND file_count = (
                      SELECT COUNT(*) FROM tasks WHERE tasks.job_id=jobs.job_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM tasks
                      WHERE tasks.job_id=jobs.job_id
                        AND (
                            tasks.status<>'COMPLETED'
                            OR tasks.result_payload_json IS NULL
                        )
                  )
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, completedAt);
            ps.setString(2, toJson(resultPayload));
            ps.setString(3, jobId);
            return ps.executeUpdate() > 0;
        }
    }

    private boolean markJobFailedInCurrentTransaction(String jobId, long completedAt) throws SQLException {
        String sql = "UPDATE jobs SET status='FAILED', completed_at=? WHERE job_id=? AND status='RUNNING'";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, completedAt);
            ps.setString(2, jobId);
            return ps.executeUpdate() > 0;
        }
    }

    @Override
    public synchronized Optional<OutboxRecord> enqueueBrokerOutbox(OutboxMessage message) {
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                OutboxRecord record = insertBrokerOutboxInCurrentTransaction(message, System.currentTimeMillis());
                conn.commit();
                return Optional.of(record);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("enqueueBrokerOutbox", e);
            return Optional.empty();
        }
    }

    @Override
    public synchronized Optional<CommittedTaskAssignment> createTaskAssignmentAndEnqueueBrokerOutbox(
            String taskId,
            String peerId,
            long startedAt,
            String leaseOwnerId,
            long leaseExpiresAt,
            String assignmentId,
            OutboxMessage messageTemplate) {
        if (!outboxTemplateMatchesTask(messageTemplate, taskId, peerId)) {
            LOGGER.warn("event=task_assignment_persistence_rejected task_id={} reason=invalid_outbox_template",
                    taskId);
            return Optional.empty();
        }
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                AssignmentIdentity identity = nextAssignmentIdentity(
                        taskId,
                        peerId,
                        leaseExpiresAt,
                        assignmentId
                );
                if (identity == null) {
                    conn.rollback();
                    return Optional.empty();
                }
                OutboxMessage committedMessage = assignmentOutboxMessage(messageTemplate, identity);
                if (!markTaskAssignedInCurrentTransaction(taskId, startedAt, leaseOwnerId, identity)) {
                    conn.rollback();
                    return Optional.empty();
                }
                OutboxRecord record = insertBrokerOutboxInCurrentTransaction(
                        committedMessage,
                        startedAt
                );
                CommittedTaskAssignment committed = new CommittedTaskAssignment(identity, record);
                conn.commit();
                return Optional.of(committed);
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("createTaskAssignmentAndEnqueueBrokerOutbox", e);
            return Optional.empty();
        } catch (RuntimeException e) {
            LOGGER.warn("event=task_assignment_persistence_rejected task_id={} reason=invalid_committed_outbox error={}",
                    taskId, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public synchronized TaskAssignmentCommit commitTaskAssignmentAndEnqueueBrokerOutbox(
            String taskId,
            String peerId,
            long startedAt,
            String leaseOwnerId,
            long leaseExpiresAt,
            String assignmentId,
            OutboxMessage messageTemplate) {
        Optional<CommittedTaskAssignment> committed = createTaskAssignmentAndEnqueueBrokerOutbox(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                assignmentId,
                messageTemplate
        );
        if (committed.isPresent()) {
            return new TaskAssignmentCommit(DurableTransitionOutcome.COMMITTED, committed.get());
        }
        try {
            Optional<PersistedTaskIdentity> stored = loadPersistedTaskIdentity(taskId);
            if (stored.isEmpty()) {
                return new TaskAssignmentCommit(DurableTransitionOutcome.UNKNOWN_ENTITY, null);
            }
            PersistedTaskIdentity identity = stored.get();
            if ("PENDING".equals(identity.status())) {
                return new TaskAssignmentCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
            }
            boolean exactCommittedIdentity = "ASSIGNED".equals(identity.status())
                    && Objects.equals(identity.assignmentId(), assignmentId)
                    && Objects.equals(identity.assignedPeerId(), peerId)
                    && identity.startedAt() == startedAt
                    && Objects.equals(identity.leaseOwnerId(), leaseOwnerId == null ? "" : leaseOwnerId)
                    && identity.leaseExpiresAt() == leaseExpiresAt;
            if (!exactCommittedIdentity) {
                return new TaskAssignmentCommit(DurableTransitionOutcome.STALE_STATE, null);
            }
            AssignmentIdentity committedIdentity = new AssignmentIdentity(
                    taskId,
                    identity.attemptNumber(),
                    assignmentId,
                    peerId,
                    leaseExpiresAt
            );
            OutboxMessage committedMessage = assignmentOutboxMessage(messageTemplate, committedIdentity);
            Optional<OutboxRecord> existingOutbox = findExactBrokerOutbox(committedMessage);
            if (existingOutbox.isEmpty()) {
                return new TaskAssignmentCommit(DurableTransitionOutcome.STALE_STATE, null);
            }
            return new TaskAssignmentCommit(
                    DurableTransitionOutcome.ALREADY_APPLIED,
                    new CommittedTaskAssignment(committedIdentity, existingOutbox.get())
            );
        } catch (SQLException | RuntimeException e) {
            logSqlFailure("classifyTaskAssignmentAndOutbox", asSqlException(e));
            return new TaskAssignmentCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
        }
    }

    private static boolean outboxTemplateMatchesTask(OutboxMessage message,
                                                     String taskId,
                                                     String peerId) {
        return message != null
                && message.route() == TransportRoute.TASK_ASSIGN
                && Objects.equals(peerId, message.peerNodeId())
                && message.message() instanceof TaskAssignMessage assignment
                && Objects.equals(taskId, assignment.getTaskId());
    }

    private static OutboxMessage assignmentOutboxMessage(OutboxMessage template,
                                                         AssignmentIdentity identity) {
        TaskAssignMessage assignmentTemplate = (TaskAssignMessage) template.message();
        TaskAssignMessage assignment = assignmentTemplate.withAssignmentIdentity(
                identity.attemptNumber(),
                identity.assignmentId(),
                identity.leaseExpiresAtEpochMillis()
        );
        MessageValidator.validate(assignment);
        return new OutboxMessage(
                template.route(),
                template.peerNodeId(),
                template.fromNodeId(),
                assignment
        );
    }

    @Override
    public synchronized Optional<OutboxRecord> markJobCompletedAndEnqueueBrokerOutbox(String jobId,
                                                                                     Object resultPayload,
                                                                                     OutboxMessage message) {
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                if (!markJobCompletedInCurrentTransaction(jobId, resultPayload, System.currentTimeMillis())) {
                    conn.rollback();
                    return Optional.empty();
                }
                OutboxRecord record = insertBrokerOutboxInCurrentTransaction(message, System.currentTimeMillis());
                conn.commit();
                return Optional.of(record);
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException | RuntimeException e) {
            logSqlFailure("markJobCompletedAndEnqueueBrokerOutbox", asSqlException(e));
            return Optional.empty();
        }
    }

    @Override
    public synchronized OutboxCommit commitJobCompletedAndEnqueueBrokerOutbox(String jobId,
                                                                              Object resultPayload,
                                                                              OutboxMessage message) {
        Optional<OutboxRecord> committed = markJobCompletedAndEnqueueBrokerOutbox(
                jobId,
                resultPayload,
                message
        );
        return committed
                .map(record -> new OutboxCommit(DurableTransitionOutcome.COMMITTED, record))
                .orElseGet(() -> classifyCompletedJobOutboxReplay(
                        jobId,
                        resultPayload,
                        message
                ));
    }

    @Override
    public synchronized Optional<OutboxRecord> markJobFailedAndEnqueueBrokerOutbox(String jobId,
                                                                                  Collection<BrokerOutboxStore.TaskFailureUpdate> taskFailures,
                                                                                  OutboxMessage message) {
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                Collection<BrokerOutboxStore.TaskFailureUpdate> failures = taskFailures == null
                        ? List.of()
                        : taskFailures;
                for (BrokerOutboxStore.TaskFailureUpdate failure : failures) {
                    if (!markTaskFailedInCurrentTransaction(
                            failure.taskId(),
                            failure.outcome(),
                            failure.failureReason(),
                            failure.finishedAt()
                    )) {
                        conn.rollback();
                        return Optional.empty();
                    }
                }
                if (!markJobFailedInCurrentTransaction(jobId, System.currentTimeMillis())) {
                    conn.rollback();
                    return Optional.empty();
                }
                OutboxRecord record = insertBrokerOutboxInCurrentTransaction(message, System.currentTimeMillis());
                conn.commit();
                return Optional.of(record);
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException | RuntimeException e) {
            logSqlFailure("markJobFailedAndEnqueueBrokerOutbox", asSqlException(e));
            return Optional.empty();
        }
    }

    @Override
    public synchronized OutboxCommit commitJobFailedAndEnqueueBrokerOutbox(
            String jobId,
            Collection<BrokerOutboxStore.TaskFailureUpdate> taskFailures,
            OutboxMessage message) {
        Optional<OutboxRecord> committed = markJobFailedAndEnqueueBrokerOutbox(
                jobId,
                taskFailures,
                message
        );
        return committed
                .map(record -> new OutboxCommit(DurableTransitionOutcome.COMMITTED, record))
                .orElseGet(() -> classifyFailedJobOutboxReplay(
                        jobId,
                        taskFailures,
                        message
                ));
    }

    private OutboxCommit classifyCompletedJobOutboxReplay(String jobId,
                                                           Object resultPayload,
                                                           OutboxMessage message) {
        try {
            String status = jobStatus(jobId);
            if ("RUNNING".equals(status) || "FINALIZING".equals(status)) {
                return new OutboxCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
            }
            return classifyExistingJobOutbox(
                    classifyJobCompletion(jobId, resultPayload),
                    message
            );
        } catch (SQLException | RuntimeException e) {
            logSqlFailure("classifyCompletedJobOutbox", asSqlException(e));
            return new OutboxCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
        }
    }

    private OutboxCommit classifyFailedJobOutboxReplay(
            String jobId,
            Collection<BrokerOutboxStore.TaskFailureUpdate> taskFailures,
            OutboxMessage message) {
        try {
            DurableTransitionOutcome stateOutcome = classifyRunningJob(jobId);
            if (stateOutcome == DurableTransitionOutcome.COMMITTED) {
                return new OutboxCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
            }
            if (stateOutcome == DurableTransitionOutcome.ALREADY_APPLIED) {
                Collection<BrokerOutboxStore.TaskFailureUpdate> failures = taskFailures == null
                        ? List.of()
                        : taskFailures;
                List<JobStateStore.TaskFailureUpdate> durableFailures = failures.stream()
                        .map(failure -> new JobStateStore.TaskFailureUpdate(
                                failure.taskId(),
                                failure.outcome(),
                                failure.failureReason(),
                                failure.finishedAt()
                        ))
                        .toList();
                stateOutcome = classifyCommittedJobFailure(jobId, durableFailures);
            }
            return classifyExistingJobOutbox(stateOutcome, message);
        } catch (SQLException | RuntimeException e) {
            logSqlFailure("classifyFailedJobOutbox", asSqlException(e));
            return new OutboxCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
        }
    }

    private OutboxCommit classifyExistingJobOutbox(DurableTransitionOutcome stateOutcome,
                                                     OutboxMessage message) throws SQLException {
        if (stateOutcome != DurableTransitionOutcome.ALREADY_APPLIED) {
            return new OutboxCommit(stateOutcome, null);
        }
        Optional<OutboxRecord> existingOutbox = findExactBrokerOutbox(message);
        return existingOutbox
                .map(record -> new OutboxCommit(DurableTransitionOutcome.ALREADY_APPLIED, record))
                .orElseGet(() -> new OutboxCommit(DurableTransitionOutcome.STALE_STATE, null));
    }

    @Override
    public synchronized List<OutboxRecord> loadPendingBrokerOutbox(int limit) {
        int normalizedLimit = limit <= 0 ? 100 : limit;
        List<OutboxRecord> records = new ArrayList<>();
        String sql = """
                SELECT
                    outbox_id,
                    route,
                    peer_node_id,
                    from_node_id,
                    message_type,
                    message_json,
                    created_at,
                    attempt_count,
                    last_attempt_at,
                    last_error
                FROM broker_outbox
                WHERE published_at IS NULL
                ORDER BY created_at ASC, outbox_id ASC
                LIMIT ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, normalizedLimit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    records.add(outboxRecordFromResultSet(rs));
                }
            }
        } catch (SQLException | RuntimeException e) {
            logSqlFailure("loadPendingBrokerOutbox", asSqlException(e));
        }
        return records;
    }

    @Override
    public synchronized PendingOutboxCount countPendingBrokerOutbox() {
        String sql = """
                SELECT COUNT(*) AS pending_count
                FROM broker_outbox
                WHERE published_at IS NULL
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                logSqlFailure(
                        "countPendingBrokerOutbox",
                        new SQLException("Pending outbox aggregate returned no row.")
                );
                return PendingOutboxCount.storageFailure();
            }
            return PendingOutboxCount.counted(rs.getLong("pending_count"));
        } catch (SQLException | RuntimeException e) {
            logSqlFailure("countPendingBrokerOutbox", asSqlException(e));
            return PendingOutboxCount.storageFailure();
        }
    }

    @Override
    public synchronized boolean markBrokerOutboxPublished(long outboxId, long publishedAt) {
        String sql = """
                UPDATE broker_outbox
                SET published_at=?,
                    last_attempt_at=?,
                    last_error=''
                WHERE outbox_id=? AND published_at IS NULL
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, publishedAt);
            ps.setLong(2, publishedAt);
            ps.setLong(3, outboxId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markBrokerOutboxPublished", e);
            return false;
        }
    }

    @Override
    public synchronized boolean markBrokerOutboxPublishFailed(long outboxId, String error, long attemptedAt) {
        String sql = """
                UPDATE broker_outbox
                SET attempt_count=attempt_count + 1,
                    last_attempt_at=?,
                    last_error=?
                WHERE outbox_id=? AND published_at IS NULL
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, attemptedAt);
            ps.setString(2, error == null ? "" : error);
            ps.setLong(3, outboxId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markBrokerOutboxPublishFailed", e);
            return false;
        }
    }

    private OutboxRecord insertBrokerOutboxInCurrentTransaction(OutboxMessage message,
                                                                long createdAt) throws SQLException {
        String sql = """
                INSERT INTO broker_outbox(
                    route,
                    peer_node_id,
                    from_node_id,
                    message_type,
                    message_json,
                    created_at
                )
                VALUES(?,?,?,?,?,?)
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, message.route().name());
            ps.setString(2, message.peerNodeId());
            ps.setString(3, message.fromNodeId());
            ps.setString(4, message.message().getType());
            ps.setString(5, GSON.toJson(message.message()));
            ps.setLong(6, createdAt);
            if (ps.executeUpdate() <= 0) {
                throw new SQLException("No broker outbox row inserted.");
            }
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    return new OutboxRecord(keys.getLong(1), message, createdAt, 0, 0L, "");
                }
            }
        }

        String lastIdSql = "SELECT last_insert_rowid()";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(lastIdSql)) {
            if (rs.next()) {
                return new OutboxRecord(rs.getLong(1), message, createdAt, 0, 0L, "");
            }
        }
        throw new SQLException("Broker outbox id was not available.");
    }

    private OutboxRecord outboxRecordFromResultSet(ResultSet rs) throws SQLException {
        TransportRoute route = transportRouteFromDb(rs.getString("route"));
        Message message = messageFromJson(rs.getString("message_type"), rs.getString("message_json"));
        return new OutboxRecord(
                rs.getLong("outbox_id"),
                new OutboxMessage(
                        route,
                        rs.getString("peer_node_id"),
                        rs.getString("from_node_id"),
                        message
                ),
                rs.getLong("created_at"),
                rs.getInt("attempt_count"),
                rs.getLong("last_attempt_at"),
                rs.getString("last_error")
        );
    }

    private Optional<OutboxRecord> findExactBrokerOutbox(OutboxMessage message) throws SQLException {
        if (message == null) {
            return Optional.empty();
        }
        String sql = """
                SELECT
                    outbox_id,
                    route,
                    peer_node_id,
                    from_node_id,
                    message_type,
                    message_json,
                    created_at,
                    attempt_count,
                    last_attempt_at,
                    last_error
                FROM broker_outbox
                WHERE route=?
                  AND peer_node_id=?
                  AND from_node_id=?
                  AND message_type=?
                  AND message_json=?
                ORDER BY outbox_id DESC
                LIMIT 1
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, message.route().name());
            ps.setString(2, message.peerNodeId());
            ps.setString(3, message.fromNodeId());
            ps.setString(4, message.message().getType());
            ps.setString(5, GSON.toJson(message.message()));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next()
                        ? Optional.of(outboxRecordFromResultSet(rs))
                        : Optional.empty();
            }
        }
    }

    public synchronized int markRunningJobsFailedOnStartup(long completedAt) {
        String failAttemptsSql = """
                UPDATE task_attempts
                SET outcome='JOB_FAILED',
                    finished_at=?,
                    duration_ms=MAX(0, ? - started_at),
                    failure_reason='coordinator_startup_reconciliation'
                WHERE outcome='RUNNING'
                  AND job_id IN (SELECT job_id FROM jobs WHERE status='RUNNING')
                """;
        String failTasksSql = """
                UPDATE tasks
                SET status='FAILED',
                    completed_at=?,
                    lease_owner_id='',
                    lease_expires_at=0
                WHERE status NOT IN ('COMPLETED', 'FAILED')
                  AND job_id IN (SELECT job_id FROM jobs WHERE status='RUNNING')
                """;
        String failJobsSql = "UPDATE jobs SET status='FAILED', completed_at=? WHERE status='RUNNING'";
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement failAttempts = conn.prepareStatement(failAttemptsSql);
                 PreparedStatement failTasks = conn.prepareStatement(failTasksSql);
                 PreparedStatement failJobs = conn.prepareStatement(failJobsSql)) {
                failAttempts.setLong(1, completedAt);
                failAttempts.setLong(2, completedAt);
                failAttempts.executeUpdate();

                failTasks.setLong(1, completedAt);
                failTasks.executeUpdate();

                failJobs.setLong(1, completedAt);
                int failedJobs = failJobs.executeUpdate();
                conn.commit();
                return failedJobs;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("markRunningJobsFailedOnStartup", e);
            return -1;
        }
    }

    @Override
    public synchronized boolean markRunningJobFailedOnStartup(String jobId, long completedAt) {
        String failAttemptsSql = """
                UPDATE task_attempts
                SET outcome='JOB_FAILED',
                    finished_at=?,
                    duration_ms=MAX(0, ? - started_at),
                    failure_reason='coordinator_startup_reconciliation'
                WHERE outcome='RUNNING'
                  AND job_id=?
                """;
        String failTasksSql = """
                UPDATE tasks
                SET status='FAILED',
                    completed_at=?,
                    lease_owner_id='',
                    lease_expires_at=0
                WHERE status NOT IN ('COMPLETED', 'FAILED')
                  AND job_id=?
                """;
        String failJobSql = """
                UPDATE jobs
                SET status='FAILED', completed_at=?
                WHERE job_id=? AND status IN ('RUNNING', 'FINALIZING')
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement failAttempts = conn.prepareStatement(failAttemptsSql);
                 PreparedStatement failTasks = conn.prepareStatement(failTasksSql);
                 PreparedStatement failJob = conn.prepareStatement(failJobSql)) {
                failAttempts.setLong(1, completedAt);
                failAttempts.setLong(2, completedAt);
                failAttempts.setString(3, jobId);
                failAttempts.executeUpdate();

                failTasks.setLong(1, completedAt);
                failTasks.setString(2, jobId);
                failTasks.executeUpdate();

                failJob.setLong(1, completedAt);
                failJob.setString(2, jobId);
                boolean failed = failJob.executeUpdate() > 0;
                conn.commit();
                return failed;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("markRunningJobFailedOnStartup", e);
            return false;
        }
    }

    @Override
    public synchronized boolean resetTaskForResume(String taskId) {
        return resetTaskForResume(taskId, 0);
    }

    @Override
    public synchronized boolean resetTaskForResume(String taskId, int lastAssignmentAttemptNumber) {
        String sql = """
                UPDATE tasks
                SET status='PENDING',
                    assigned_peer_id=NULL,
                    assignment_id=NULL,
                    attempt_number=MAX(attempt_number, ?),
                    started_at=NULL,
                    completed_at=NULL,
                    duration_ms=NULL,
                    lease_owner_id='',
                    lease_expires_at=0
                WHERE task_id=? AND status IN ('PENDING', 'ASSIGNED')
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                String priorStatus = taskStatus(taskId);
                long resetAt = System.currentTimeMillis();
                if ("ASSIGNED".equals(priorStatus)
                        && hasRunningAttempt(taskId)
                        && !finishRunningAttempt(
                        taskId,
                        resetAt,
                        attemptDuration(taskId, resetAt),
                        TaskAttemptOutcome.RETRY_SCHEDULED,
                        "coordinator_restart"
                )) {
                    conn.rollback();
                    return false;
                }
                ps.setInt(1, Math.max(0, lastAssignmentAttemptNumber));
                ps.setString(2, taskId);
                if (ps.executeUpdate() <= 0) {
                    conn.rollback();
                    return false;
                }
                conn.commit();
                return true;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("resetTaskForResume", e);
            return false;
        }
    }

    @Override
    public synchronized boolean releaseExpiredTaskLeaseForResume(String taskId, long releasedAt) {
        return releaseExpiredTaskLeaseForResume(taskId, releasedAt, 0);
    }

    @Override
    public synchronized boolean releaseExpiredTaskLeaseForResume(String taskId,
                                                                 long releasedAt,
                                                                 int lastAssignmentAttemptNumber) {
        String sql = """
                UPDATE tasks
                SET status='PENDING',
                    assigned_peer_id=NULL,
                    assignment_id=NULL,
                    attempt_number=MAX(attempt_number, ?),
                    started_at=NULL,
                    completed_at=NULL,
                    duration_ms=NULL,
                    lease_owner_id='',
                    lease_expires_at=0
                WHERE task_id=?
                  AND status='ASSIGNED'
                  AND (lease_expires_at<=0 OR lease_expires_at<=?)
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (hasRunningAttempt(taskId)
                        && !finishRunningAttempt(
                        taskId,
                        releasedAt,
                        attemptDuration(taskId, releasedAt),
                        TaskAttemptOutcome.RETRY_SCHEDULED,
                        "lease_expired"
                )) {
                    conn.rollback();
                    return false;
                }
                ps.setInt(1, Math.max(0, lastAssignmentAttemptNumber));
                ps.setString(2, taskId);
                ps.setLong(3, releasedAt);
                if (ps.executeUpdate() <= 0) {
                    conn.rollback();
                    return false;
                }
                conn.commit();
                return true;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("releaseExpiredTaskLeaseForResume", e);
            return false;
        }
    }

    @Override
    public synchronized List<ResumableJobState> loadRunningJobsForResume() {
        List<ResumableJobState> jobs = new ArrayList<>();
        String sql = """
                SELECT *
                FROM jobs
                WHERE status IN ('RUNNING', 'FINALIZING')
                ORDER BY submitted_at ASC
                """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String jobId = rs.getString("job_id");
                jobs.add(new ResumableJobState(
                        jobId,
                        rs.getString("task_type"),
                        rs.getString("requester_node_id"),
                        rs.getString("requester_token_hash"),
                        rs.getString("requester_identity_key"),
                        rs.getString("parameter"),
                        loadTasksForResume(jobId)
                ));
            }
        } catch (SQLException e) {
            logSqlFailure("loadRunningJobsForResume", e);
        }
        return jobs;
    }

    private List<ResumableTaskState> loadTasksForResume(String jobId) throws SQLException {
        List<ResumableTaskState> tasks = new ArrayList<>();
        String sql = """
                SELECT task_id,
                       status,
                       payload_json,
                       result_payload_json,
                       retry_count,
                       assigned_peer_id,
                       started_at,
                       lease_owner_id,
                       lease_expires_at,
                       attempt_number,
                       assignment_id
                FROM tasks
                WHERE job_id=?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tasks.add(new ResumableTaskState(
                            rs.getString("task_id"),
                            rs.getString("status"),
                            fromJson(rs.getString("payload_json")),
                            fromJson(rs.getString("result_payload_json")),
                            rs.getInt("retry_count"),
                            rs.getString("assigned_peer_id"),
                            rs.getLong("started_at"),
                            rs.getString("lease_owner_id"),
                            rs.getLong("lease_expires_at"),
                            rs.getInt("attempt_number"),
                            rs.getString("assignment_id"),
                            rs.getString("result_payload_json") != null
                    ));
                }
            }
        }
        return tasks.stream()
                .sorted((left, right) -> compareTaskIds(left.taskId(), right.taskId()))
                .toList();
    }

    @Override
    public synchronized Optional<CompletedJobResultState> loadCompletedJobResult(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return Optional.empty();
        }

        String jobSql = """
                SELECT task_type, file_count, requester_token_hash, requester_identity_key, result_payload_json
                FROM jobs
                WHERE job_id=? AND status='COMPLETED'
                """;
        try (PreparedStatement jobStatement = conn.prepareStatement(jobSql)) {
            jobStatement.setString(1, jobId);
            try (ResultSet jobResult = jobStatement.executeQuery()) {
                if (!jobResult.next()) {
                    return Optional.empty();
                }

                String taskType = jobResult.getString("task_type");
                int expectedTasks = jobResult.getInt("file_count");
                if (expectedTasks <= 0) {
                    return Optional.empty();
                }

                List<TaskResultSnapshot> taskResults = loadCompletedTaskResults(jobId);
                if (taskResults.size() != expectedTasks) {
                    return Optional.empty();
                }

                return Optional.of(new CompletedJobResultState(
                        jobId,
                        taskType,
                        jobResult.getString("requester_token_hash"),
                        jobResult.getString("requester_identity_key"),
                        completedResultPayload(jobResult.getString("result_payload_json"), taskResults),
                        taskResults.stream()
                                .sorted((left, right) -> compareTaskIds(left.taskId(), right.taskId()))
                                .map(TaskResultSnapshot::resultPayload)
                                .toList()
                ));
            }
        } catch (SQLException e) {
            logSqlFailure("loadCompletedJobResult", e);
            return Optional.empty();
        }
    }

    private Object completedResultPayload(String resultPayloadJson, List<TaskResultSnapshot> taskResults) {
        Object resultPayload = fromJson(resultPayloadJson);
        if (resultPayload != null) {
            return resultPayload;
        }
        return taskResults.stream()
                .sorted((left, right) -> compareTaskIds(left.taskId(), right.taskId()))
                .map(TaskResultSnapshot::resultPayload)
                .toList();
    }

    private List<TaskResultSnapshot> loadCompletedTaskResults(String jobId) throws SQLException {
        List<TaskResultSnapshot> tasks = new ArrayList<>();
        String taskSql = """
                SELECT task_id, status, result_payload_json
                FROM tasks
                WHERE job_id=?
                """;
        try (PreparedStatement taskStatement = conn.prepareStatement(taskSql)) {
            taskStatement.setString(1, jobId);
            try (ResultSet taskResult = taskStatement.executeQuery()) {
                while (taskResult.next()) {
                    String status = taskResult.getString("status");
                    String resultPayloadJson = taskResult.getString("result_payload_json");
                    if (!"COMPLETED".equals(status) || resultPayloadJson == null) {
                        return List.of();
                    }
                    tasks.add(new TaskResultSnapshot(
                            taskResult.getString("task_id"),
                            fromJson(resultPayloadJson)
                    ));
                }
            }
        }
        return tasks;
    }

    @Override
    public synchronized List<TaskAttemptRecord> loadTaskAttempts(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return List.of();
        }
        List<TaskAttemptRecord> attempts = new ArrayList<>();
        String sql = """
                SELECT
                    job_id,
                    task_id,
                    attempt_number,
                    assignment_id,
                    peer_id,
                    started_at,
                    lease_expires_at,
                    finished_at,
                    duration_ms,
                    outcome,
                    failure_reason
                FROM task_attempts
                WHERE job_id=?
                ORDER BY task_id ASC, attempt_number ASC
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    attempts.add(new TaskAttemptRecord(
                            rs.getString("job_id"),
                            rs.getString("task_id"),
                            rs.getInt("attempt_number"),
                            rs.getString("assignment_id"),
                            rs.getString("peer_id"),
                            rs.getLong("started_at"),
                            rs.getLong("lease_expires_at"),
                            rs.getLong("finished_at"),
                            rs.getLong("duration_ms"),
                            attemptOutcomeFromDb(rs.getString("outcome")),
                            rs.getString("failure_reason")
                    ));
                }
            }
        } catch (SQLException e) {
            logSqlFailure("loadTaskAttempts", e);
        }
        return attempts;
    }

    private boolean insertTaskAttempt(AssignmentIdentity identity, long startedAt) throws SQLException {
        String sql = """
                INSERT INTO task_attempts(
                    job_id,
                    task_id,
                    attempt_number,
                    assignment_id,
                    peer_id,
                    started_at,
                    lease_expires_at,
                    outcome,
                    failure_reason
                )
                SELECT
                    job_id,
                    task_id,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    'RUNNING',
                    ''
                FROM tasks
                WHERE task_id=?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, identity.attemptNumber());
            ps.setString(2, identity.assignmentId());
            ps.setString(3, identity.workerId());
            ps.setLong(4, startedAt);
            ps.setLong(5, identity.leaseExpiresAtEpochMillis());
            ps.setString(6, identity.taskId());
            return ps.executeUpdate() > 0;
        }
    }

    private ResultCommitOutcome classifyResultCommit(String taskId,
                                                     int attemptNumber,
                                                     String assignmentId,
                                                     String assignedPeerId) throws SQLException {
        Optional<PersistedTaskIdentity> stored = loadPersistedTaskIdentity(taskId);
        if (stored.isEmpty()) {
            return ResultCommitOutcome.UNKNOWN_TASK;
        }

        PersistedTaskIdentity identity = stored.get();
        boolean exactAssignment = identity.attemptNumber() == attemptNumber
                && Objects.equals(identity.assignmentId(), assignmentId)
                && Objects.equals(identity.assignedPeerId(), assignedPeerId);
        if ("COMPLETED".equals(identity.status()) && exactAssignment) {
            return ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED;
        }
        return ResultCommitOutcome.STALE_ASSIGNMENT;
    }

    /**
     * Repeats protocol output ownership validation inside the authoritative
     * result transaction. A non-null return value rejects the commit.
     */
    private ResultCommitOutcome validateTaskOutputReferenceOwnership(String taskId,
                                                                     int attemptNumber,
                                                                     String assignmentId,
                                                                     Object resultPayload)
            throws SQLException {
        List<ObjectReference> references = PayloadLimits.objectReferences(resultPayload);
        if (references.isEmpty()) {
            return null;
        }
        String jobId = loadTaskJobId(taskId).orElse(null);
        if (jobId == null) {
            return ResultCommitOutcome.UNKNOWN_TASK;
        }
        String expectedKey = TaskFlowObjectKeys.attemptOutputKey(
                jobId,
                taskId,
                attemptNumber,
                assignmentId
        );
        for (ObjectReference reference : references) {
            if (!expectedKey.equals(reference.key())) {
                return ResultCommitOutcome.STALE_ASSIGNMENT;
            }
        }
        return null;
    }

    private Optional<String> loadTaskJobId(String taskId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT job_id FROM tasks WHERE task_id=?"
        )) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
            }
        }
    }

    private Optional<PersistedTaskIdentity> loadPersistedTaskIdentity(String taskId) throws SQLException {
        String sql = """
                SELECT status,
                       attempt_number,
                       assignment_id,
                       assigned_peer_id,
                       retry_count,
                       started_at,
                       lease_owner_id,
                       lease_expires_at
                FROM tasks
                WHERE task_id=?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new PersistedTaskIdentity(
                        rs.getString("status"),
                        rs.getInt("attempt_number"),
                        rs.getString("assignment_id"),
                        rs.getString("assigned_peer_id"),
                        rs.getInt("retry_count"),
                        rs.getLong("started_at"),
                        rs.getString("lease_owner_id"),
                        rs.getLong("lease_expires_at")
                ));
            }
        }
    }

    private boolean finishExactRunningAttempt(String taskId,
                                              int attemptNumber,
                                              String assignmentId,
                                              String assignedPeerId,
                                              long finishedAt,
                                              long durationMs) throws SQLException {
        return finishExactRunningAttempt(
                taskId,
                attemptNumber,
                assignmentId,
                assignedPeerId,
                finishedAt,
                durationMs,
                TaskAttemptOutcome.SUCCEEDED,
                ""
        );
    }

    private boolean finishExactRunningAttempt(String taskId,
                                              int attemptNumber,
                                              String assignmentId,
                                              String assignedPeerId,
                                              long finishedAt,
                                              long durationMs,
                                              TaskAttemptOutcome outcome,
                                              String failureReason) throws SQLException {
        String sql = """
                UPDATE task_attempts
                SET outcome=?,
                    finished_at=?,
                    duration_ms=?,
                    failure_reason=?
                WHERE task_id=?
                  AND attempt_number=?
                  AND assignment_id=?
                  AND peer_id=?
                  AND outcome='RUNNING'
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalizeFinishedOutcome(outcome).name());
            ps.setLong(2, finishedAt);
            ps.setLong(3, Math.max(0L, durationMs));
            ps.setString(4, failureReason == null ? "" : failureReason);
            ps.setString(5, taskId);
            ps.setInt(6, attemptNumber);
            ps.setString(7, assignmentId);
            ps.setString(8, assignedPeerId);
            return ps.executeUpdate() == 1;
        }
    }

    private long exactAttemptDuration(String taskId,
                                      int attemptNumber,
                                      String assignmentId,
                                      String assignedPeerId,
                                      long finishedAt) throws SQLException {
        String sql = """
                SELECT started_at
                FROM task_attempts
                WHERE task_id=?
                  AND attempt_number=?
                  AND assignment_id=?
                  AND peer_id=?
                  AND outcome='RUNNING'
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setInt(2, attemptNumber);
            ps.setString(3, assignmentId);
            ps.setString(4, assignedPeerId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Math.max(0L, finishedAt - rs.getLong("started_at")) : 0L;
            }
        }
    }

    private boolean exactAttemptHasOutcome(String taskId,
                                           int attemptNumber,
                                           String assignmentId,
                                           String assignedPeerId,
                                           TaskAttemptOutcome outcome) throws SQLException {
        String sql = """
                SELECT 1
                FROM task_attempts
                WHERE task_id=?
                  AND attempt_number=?
                  AND assignment_id=?
                  AND peer_id=?
                  AND outcome=?
                LIMIT 1
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setInt(2, attemptNumber);
            ps.setString(3, assignmentId);
            ps.setString(4, assignedPeerId);
            ps.setString(5, normalizeFinishedOutcome(outcome).name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean finishRunningAttempt(String taskId,
                                         long finishedAt,
                                         long durationMs,
                                         TaskAttemptOutcome outcome,
                                         String failureReason) throws SQLException {
        String sql = """
                UPDATE task_attempts
                SET outcome=?,
                    finished_at=?,
                    duration_ms=?,
                    failure_reason=?
                WHERE attempt_id=(
                    SELECT attempt_id
                    FROM task_attempts
                    WHERE task_id=? AND outcome='RUNNING'
                    ORDER BY attempt_number DESC
                    LIMIT 1
                )
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalizeFinishedOutcome(outcome).name());
            ps.setLong(2, finishedAt);
            ps.setLong(3, Math.max(0L, durationMs));
            ps.setString(4, failureReason == null ? "" : failureReason);
            ps.setString(5, taskId);
            return ps.executeUpdate() > 0;
        }
    }

    private boolean hasRunningAttempt(String taskId) throws SQLException {
        String sql = "SELECT 1 FROM task_attempts WHERE task_id=? AND outcome='RUNNING' LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private long attemptDuration(String taskId, long finishedAt) throws SQLException {
        String sql = """
                SELECT started_at
                FROM task_attempts
                WHERE task_id=? AND outcome='RUNNING'
                ORDER BY attempt_number DESC
                LIMIT 1
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return 0L;
                }
                return Math.max(0L, finishedAt - rs.getLong("started_at"));
            }
        }
    }

    private String taskStatus(String taskId) throws SQLException {
        String sql = "SELECT status FROM tasks WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("status") : "";
            }
        }
    }

    @Override
    public synchronized boolean upsertPeerRecord(PeerRegistryRecord record) {
        if (record == null) {
            return false;
        }
        String sql = """
                INSERT INTO peer_registry(
                    peer_id,
                    runtime_type,
                    transport,
                    supported_task_types_json,
                    first_seen_at,
                    last_heartbeat_at,
                    last_disconnected_at,
                    status,
                    completed_tasks,
                    failed_tasks,
                    latency_ewma_ms,
                    task_duration_ewma_ms
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(peer_id) DO UPDATE SET
                    runtime_type=excluded.runtime_type,
                    transport=excluded.transport,
                    supported_task_types_json=excluded.supported_task_types_json,
                    first_seen_at=CASE
                        WHEN peer_registry.first_seen_at > 0 THEN peer_registry.first_seen_at
                        ELSE excluded.first_seen_at
                    END,
                    last_heartbeat_at=MAX(peer_registry.last_heartbeat_at, excluded.last_heartbeat_at),
                    last_disconnected_at=CASE
                        WHEN excluded.last_disconnected_at > 0 THEN excluded.last_disconnected_at
                        ELSE peer_registry.last_disconnected_at
                    END,
                    status=excluded.status,
                    completed_tasks=excluded.completed_tasks,
                    failed_tasks=excluded.failed_tasks,
                    latency_ewma_ms=excluded.latency_ewma_ms,
                    task_duration_ewma_ms=excluded.task_duration_ewma_ms
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bindPeerRecord(ps, record);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("upsertPeerRecord", e);
            return false;
        }
    }

    @Override
    public synchronized List<PeerRegistryRecord> loadPeerRecords() {
        List<PeerRegistryRecord> peers = new ArrayList<>();
        String sql = """
                SELECT
                    peer_id,
                    runtime_type,
                    transport,
                    supported_task_types_json,
                    first_seen_at,
                    last_heartbeat_at,
                    last_disconnected_at,
                    status,
                    completed_tasks,
                    failed_tasks,
                    latency_ewma_ms,
                    task_duration_ewma_ms
                FROM peer_registry
                ORDER BY first_seen_at ASC, peer_id ASC
                """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                peers.add(new PeerRegistryRecord(
                        rs.getString("peer_id"),
                        rs.getString("runtime_type"),
                        peerTransportFromDb(rs.getString("transport")),
                        taskTypesFromJson(rs.getString("supported_task_types_json")),
                        rs.getLong("first_seen_at"),
                        rs.getLong("last_heartbeat_at"),
                        rs.getLong("last_disconnected_at"),
                        peerStatusFromDb(rs.getString("status")),
                        new PeerMetricsSnapshot(
                                rs.getLong("completed_tasks"),
                                rs.getLong("failed_tasks"),
                                rs.getLong("latency_ewma_ms"),
                                rs.getLong("task_duration_ewma_ms")
                        )
                ));
            }
        } catch (SQLException e) {
            logSqlFailure("loadPeerRecords", e);
        }
        return peers;
    }

    // -------------------------------------------------------------------------
    // Read methods (called from GUI process via its own connection)
    // -------------------------------------------------------------------------

    public synchronized List<JobRecord> getJobHistory() {
        List<JobRecord> jobs = new ArrayList<>();
        String sql = "SELECT * FROM jobs ORDER BY submitted_at DESC";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                jobs.add(new JobRecord(
                    rs.getString("job_id"),
                    rs.getString("task_type"),
                    rs.getString("requester_node_id"),
                    rs.getString("status"),
                    rs.getLong("submitted_at"),
                    rs.getLong("completed_at"),
                    rs.getInt("file_count")
                ));
            }
        } catch (SQLException e) {
            logSqlFailure("getJobHistory", e);
        }
        return jobs;
    }

    public synchronized List<TaskRecord> getTasksForJob(String jobId) {
        List<TaskRecord> tasks = new ArrayList<>();
        String sql = "SELECT * FROM tasks WHERE job_id=? ORDER BY started_at ASC";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                tasks.add(new TaskRecord(
                    rs.getString("task_id"),
                    rs.getString("job_id"),
                    rs.getString("assigned_peer_id"),
                    rs.getString("status"),
                    rs.getLong("started_at"),
                        rs.getLong("completed_at"),
                        rs.getLong("duration_ms"),
                        rs.getInt("retry_count"),
                        rs.getString("lease_owner_id"),
                        rs.getLong("lease_expires_at"),
                        rs.getInt("attempt_number"),
                        rs.getString("assignment_id")
                ));
            }
        } catch (SQLException e) {
            logSqlFailure("getTasksForJob", e);
        }
        return tasks;
    }

    public void close() {
        try { conn.close(); } catch (SQLException ignored) {}
    }

    private static void logSqlFailure(String operation, SQLException e) {
        LOGGER.warn("event=db_operation_failed operation={} error={}", operation, e.getMessage(), e);
    }

    private static void bindPeerRecord(PreparedStatement ps, PeerRegistryRecord record) throws SQLException {
        PeerMetricsSnapshot metrics = record.metricsSnapshot();
        ps.setString(1, record.peerId());
        ps.setString(2, record.runtimeType());
        ps.setString(3, record.transport().name());
        ps.setString(4, taskTypesToJson(record.supportedTaskTypes()));
        ps.setLong(5, record.firstSeenAtMillis());
        ps.setLong(6, record.lastHeartbeatAtMillis());
        ps.setLong(7, record.lastDisconnectedAtMillis());
        ps.setString(8, record.status().name());
        ps.setLong(9, metrics.completedTasks());
        ps.setLong(10, metrics.failedTasks());
        ps.setLong(11, metrics.latencyEwmaMs());
        ps.setLong(12, metrics.taskDurationEwmaMs());
    }

    private static String taskTypesToJson(Collection<String> taskTypes) {
        List<String> normalized = PeerRegistryRecord.normalizeTaskTypes(taskTypes).stream()
                .sorted()
                .toList();
        return GSON.toJson(normalized);
    }

    private static Set<String> taskTypesFromJson(String json) {
        if (json == null || json.isBlank()) {
            return Set.of();
        }
        try {
            String[] values = GSON.fromJson(json, String[].class);
            return values == null
                    ? Set.of()
                    : PeerRegistryRecord.normalizeTaskTypes(Arrays.asList(values));
        } catch (RuntimeException ignored) {
            return Set.of();
        }
    }

    private static PeerTransport peerTransportFromDb(String value) {
        if (value == null || value.isBlank()) {
            return PeerTransport.UNKNOWN;
        }
        try {
            return PeerTransport.valueOf(value);
        } catch (IllegalArgumentException ignored) {
            return PeerTransport.UNKNOWN;
        }
    }

    private static PeerStatus peerStatusFromDb(String value) {
        if (value == null || value.isBlank()) {
            return PeerStatus.DISCONNECTED;
        }
        try {
            return PeerStatus.valueOf(value);
        } catch (IllegalArgumentException ignored) {
            return PeerStatus.DISCONNECTED;
        }
    }

    private static TransportRoute transportRouteFromDb(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Broker outbox route is missing.");
        }
        return TransportRoute.valueOf(value);
    }

    private static Message messageFromJson(String messageType, String json) {
        if (messageType == null || messageType.isBlank()) {
            throw new IllegalArgumentException("Broker outbox message type is missing.");
        }
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("Broker outbox message JSON is missing.");
        }
        return switch (messageType) {
            case MessageType.JOB_SUBMIT -> GSON.fromJson(json, JobSubmitMessage.class);
            case MessageType.JOB_RESULT_REQUEST -> GSON.fromJson(json, JobResultRequestMessage.class);
            case MessageType.TASK_ASSIGN -> GSON.fromJson(json, TaskAssignMessage.class);
            case MessageType.TASK_RESULT -> GSON.fromJson(json, TaskResultMessage.class);
            case MessageType.JOB_RESULT -> GSON.fromJson(json, JobResultMessage.class);
            case MessageType.PING -> GSON.fromJson(json, PingMessage.class);
            case MessageType.PONG -> GSON.fromJson(json, PongMessage.class);
            default -> throw new IllegalArgumentException("Unknown broker outbox message type: " + messageType);
        };
    }

    private static TaskAttemptOutcome attemptOutcomeFromDb(String value) {
        if (value == null || value.isBlank()) {
            return TaskAttemptOutcome.JOB_FAILED;
        }
        try {
            return TaskAttemptOutcome.valueOf(value);
        } catch (IllegalArgumentException ignored) {
            return TaskAttemptOutcome.JOB_FAILED;
        }
    }

    private static TaskAttemptOutcome normalizeFailureOutcome(TaskAttemptOutcome outcome,
                                                             TaskAttemptOutcome fallback) {
        if (outcome == null || outcome == TaskAttemptOutcome.RUNNING || outcome == TaskAttemptOutcome.SUCCEEDED) {
            return fallback;
        }
        return outcome;
    }

    private static TaskAttemptOutcome normalizeFinishedOutcome(TaskAttemptOutcome outcome) {
        if (outcome == null || outcome == TaskAttemptOutcome.RUNNING) {
            return TaskAttemptOutcome.JOB_FAILED;
        }
        return outcome;
    }

    private static String toJson(Object payload) {
        return payload == null ? null : GSON.toJson(payload);
    }

    private static String toTaskResultJson(Object payload) {
        return GSON.toJson(payload);
    }

    private record PersistedTaskIdentity(String status,
                                         int attemptNumber,
                                         String assignmentId,
                                         String assignedPeerId,
                                         int retryCount,
                                         long startedAt,
                                         String leaseOwnerId,
                                         long leaseExpiresAt) {
    }

    private static Object fromJson(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        return GSON.fromJson(json, Object.class);
    }

    private static SQLException asSqlException(Exception e) {
        if (e instanceof SQLException sqlException) {
            return sqlException;
        }
        return new SQLException(e.getMessage(), e);
    }

    private static int taskIndex(String taskId) {
        int marker = taskId == null ? -1 : taskId.lastIndexOf('-');
        if (marker < 0 || marker == taskId.length() - 1) {
            return Integer.MAX_VALUE;
        }
        try {
            return Integer.parseInt(taskId.substring(marker + 1));
        } catch (NumberFormatException ignored) {
            return Integer.MAX_VALUE;
        }
    }

    private static int compareTaskIds(String left, String right) {
        int indexOrder = Integer.compare(taskIndex(left), taskIndex(right));
        if (indexOrder != 0) {
            return indexOrder;
        }
        if (Objects.equals(left, right)) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return left.compareTo(right);
    }

    // -------------------------------------------------------------------------
    // Record types
    // -------------------------------------------------------------------------

    public record JobRecord(
        String jobId,
        String taskType,
        String requesterId,
        String status,
        long submittedAt,
        long completedAt,
        int fileCount
    ) {}

    public record TaskRecord(
        String taskId,
        String jobId,
        String assignedPeerId,
        String status,
        long startedAt,
        long completedAt,
        long durationMs,
        int retryCount,
        String leaseOwnerId,
        long leaseExpiresAt,
        int attemptNumber,
        String assignmentId
    ) {
        public TaskRecord(String taskId,
                          String jobId,
                          String assignedPeerId,
                          String status,
                          long startedAt,
                          long completedAt,
                          long durationMs,
                          int retryCount,
                          String leaseOwnerId,
                          long leaseExpiresAt) {
            this(
                    taskId,
                    jobId,
                    assignedPeerId,
                    status,
                    startedAt,
                    completedAt,
                    durationMs,
                    retryCount,
                    leaseOwnerId,
                    leaseExpiresAt,
                    0,
                    null
            );
        }
    }

    private record TaskResultSnapshot(String taskId, Object resultPayload) {
    }
}
