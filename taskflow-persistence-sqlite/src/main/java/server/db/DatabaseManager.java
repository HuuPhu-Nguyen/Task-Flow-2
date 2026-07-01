package server.db;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.registry.PeerMetricsSnapshot;
import server.registry.PeerRegistryRecord;
import server.registry.PeerRegistryStore;
import server.registry.PeerStatus;
import server.registry.PeerTransport;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class DatabaseManager implements JobStateStore, PeerRegistryStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseManager.class);

    public static final String DB_PATH = "taskflow.db";
    public static final int CURRENT_SCHEMA_VERSION = 7;
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
            createPeerRegistryTable();

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
                    payload_json     TEXT,
                    result_payload_json TEXT,
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
                    peer_id        TEXT    NOT NULL,
                    started_at     INTEGER NOT NULL,
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

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE tasks RENAME TO " + backupTable);
        }
        createTasksTable();
        String payloadJsonExpression = hasPayloadJson ? "payload_json" : "NULL";
        String resultPayloadJsonExpression = hasResultPayloadJson ? "result_payload_json" : "NULL";
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
                    result_payload_json
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
                    %s
                FROM tasks_without_fk_migration
            """.formatted(payloadJsonExpression, resultPayloadJsonExpression));
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
        if (!columnExists("tasks", "payload_json") || !columnExists("tasks", "result_payload_json")) {
            throw new SQLException("Database schema is missing persisted task payload columns.");
        }
        if (!tableExists("task_attempts")) {
            throw new SQLException("Database schema is missing task_attempts table.");
        }
        if (!columnExists("task_attempts", "job_id")
                || !columnExists("task_attempts", "task_id")
                || !columnExists("task_attempts", "attempt_number")
                || !columnExists("task_attempts", "peer_id")
                || !columnExists("task_attempts", "started_at")
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
                    parameter
                )
                VALUES(?,?,?,?,?,?,?,?,?)
                """;
        String insertTaskSql = "INSERT INTO tasks(task_id,job_id,status,payload_json) VALUES(?,?,?,?)";
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement job = conn.prepareStatement(insertJobSql);
                 PreparedStatement task = conn.prepareStatement(insertTaskSql)) {
                job.setString(1, jobId);
                job.setString(2, taskType);
                job.setString(3, requesterId);
                job.setString(4, requesterTokenHash == null ? "" : requesterTokenHash);
                job.setString(5, requesterIdentityKey == null ? "" : requesterIdentityKey);
                job.setString(6, "RUNNING");
                job.setLong(7, System.currentTimeMillis());
                job.setInt(8, taskStates.size());
                job.setString(9, parameter == null ? "" : parameter);
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
                return true;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            logSqlFailure("insertJobWithTasks", e);
            return false;
        }
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
        String updateTaskSql = """
                UPDATE tasks
                SET status='ASSIGNED',
                    assigned_peer_id=?,
                    started_at=?
                WHERE task_id=? AND status='PENDING'
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(updateTaskSql)) {
                ps.setString(1, peerId);
                ps.setLong(2, startedAt);
                ps.setString(3, taskId);
                if (ps.executeUpdate() <= 0 || !insertTaskAttempt(taskId, peerId, startedAt)) {
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
            logSqlFailure("markTaskAssigned", e);
            return false;
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
        String sql = """
                UPDATE tasks
                SET status='COMPLETED',
                    completed_at=?,
                    duration_ms=?,
                    result_payload_json=?
                WHERE task_id=? AND status='ASSIGNED'
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                long normalizedDuration = Math.max(0L, durationMs);
                if (!finishRunningAttempt(
                        taskId,
                        completedAt,
                        normalizedDuration,
                        TaskAttemptOutcome.SUCCEEDED,
                        ""
                )) {
                    conn.rollback();
                    return false;
                }
                ps.setLong(1, completedAt);
                ps.setLong(2, normalizedDuration);
                ps.setString(3, toJson(resultPayload));
                ps.setString(4, taskId);
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
            logSqlFailure("markTaskCompleted", e);
            return false;
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
                    started_at=NULL,
                    completed_at=NULL,
                    duration_ms=NULL,
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
        String sql = """
                UPDATE tasks
                SET status='FAILED',
                    completed_at=?
                WHERE task_id=? AND status NOT IN ('COMPLETED', 'FAILED')
                """;
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (hasRunningAttempt(taskId)
                        && !finishRunningAttempt(
                        taskId,
                        finishedAt,
                        attemptDuration(taskId, finishedAt),
                        normalizeFailureOutcome(outcome, TaskAttemptOutcome.TERMINAL_FAILURE),
                        failureReason
                )) {
                    conn.rollback();
                    return false;
                }
                ps.setLong(1, finishedAt);
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
            logSqlFailure("markTaskFailed", e);
            return false;
        }
    }

    public synchronized boolean markJobCompleted(String jobId) {
        return markJobCompleted(jobId, null);
    }

    @Override
    public synchronized boolean markJobCompleted(String jobId, Object resultPayload) {
        String sql = """
                UPDATE jobs
                SET status='COMPLETED',
                    completed_at=?,
                    result_payload_json=?
                WHERE job_id=? AND status='RUNNING'
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, toJson(resultPayload));
            ps.setString(3, jobId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markJobCompleted", e);
            return false;
        }
    }

    public synchronized boolean markJobFailed(String jobId) {
        String sql = "UPDATE jobs SET status='FAILED', completed_at=? WHERE job_id=? AND status='RUNNING'";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, jobId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markJobFailed", e);
            return false;
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
                SET status='FAILED', completed_at=?
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
                SET status='FAILED', completed_at=?
                WHERE status NOT IN ('COMPLETED', 'FAILED')
                  AND job_id=?
                """;
        String failJobSql = "UPDATE jobs SET status='FAILED', completed_at=? WHERE job_id=? AND status='RUNNING'";
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
        String sql = """
                UPDATE tasks
                SET status='PENDING',
                    assigned_peer_id=NULL,
                    started_at=NULL,
                    completed_at=NULL,
                    duration_ms=NULL
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
                ps.setString(1, taskId);
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
    public synchronized List<ResumableJobState> loadRunningJobsForResume() {
        List<ResumableJobState> jobs = new ArrayList<>();
        String sql = "SELECT * FROM jobs WHERE status='RUNNING' ORDER BY submitted_at ASC";
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
                SELECT task_id, status, payload_json, result_payload_json, retry_count
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
                            rs.getInt("retry_count")
                    ));
                }
            }
        }
        return tasks.stream()
                .sorted(Comparator.comparingInt(task -> taskIndex(task.taskId())))
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
                                .sorted(Comparator.comparingInt(task -> taskIndex(task.taskId())))
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
                .sorted(Comparator.comparingInt(task -> taskIndex(task.taskId())))
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
                    peer_id,
                    started_at,
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
                            rs.getString("peer_id"),
                            rs.getLong("started_at"),
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

    private boolean insertTaskAttempt(String taskId, String peerId, long startedAt) throws SQLException {
        String sql = """
                INSERT INTO task_attempts(
                    job_id,
                    task_id,
                    attempt_number,
                    peer_id,
                    started_at,
                    outcome,
                    failure_reason
                )
                SELECT
                    job_id,
                    task_id,
                    COALESCE((
                        SELECT MAX(attempt_number) + 1
                        FROM task_attempts
                        WHERE task_attempts.task_id=tasks.task_id
                    ), 1),
                    ?,
                    ?,
                    'RUNNING',
                    ''
                FROM tasks
                WHERE task_id=?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peerId);
            ps.setLong(2, startedAt);
            ps.setString(3, taskId);
            return ps.executeUpdate() > 0;
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
                    rs.getInt("retry_count")
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

    private static Object fromJson(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        return GSON.fromJson(json, Object.class);
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
        int retryCount
    ) {}

    private record TaskResultSnapshot(String taskId, Object resultPayload) {
    }
}
