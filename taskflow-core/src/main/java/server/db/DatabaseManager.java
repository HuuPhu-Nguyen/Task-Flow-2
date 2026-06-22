package server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class DatabaseManager implements JobStateStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseManager.class);

    public static final String DB_PATH = "taskflow.db";
    public static final int CURRENT_SCHEMA_VERSION = 1;

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
            if (tableExists("tasks")) {
                if (!tasksHaveJobForeignKey()) {
                    migrateTasksTableToForeignKey();
                }
            } else {
                createTasksTable();
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
                    status           TEXT    NOT NULL DEFAULT 'RUNNING',
                    submitted_at     INTEGER NOT NULL,
                    completed_at     INTEGER,
                    file_count       INTEGER NOT NULL
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
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                )
            """);
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

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE tasks RENAME TO " + backupTable);
        }
        createTasksTable();
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
                    retry_count
                )
                SELECT
                    task_id,
                    job_id,
                    assigned_peer_id,
                    status,
                    started_at,
                    completed_at,
                    duration_ms,
                    retry_count
                FROM tasks_without_fk_migration
            """);
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
    }

    // -------------------------------------------------------------------------
    // Write methods (called from coordinator / scheduler thread)
    // -------------------------------------------------------------------------

    public synchronized boolean insertJobWithTasks(String jobId,
                                                   String taskType,
                                                   String requesterId,
                                                   int fileCount,
                                                   Collection<String> taskIds) {
        String insertJobSql = "INSERT INTO jobs(job_id,task_type,requester_node_id,status,submitted_at,file_count) VALUES(?,?,?,?,?,?)";
        String insertTaskSql = "INSERT INTO tasks(task_id,job_id,status) VALUES(?,?,?)";
        boolean originalAutoCommit;
        try {
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement job = conn.prepareStatement(insertJobSql);
                 PreparedStatement task = conn.prepareStatement(insertTaskSql)) {
                job.setString(1, jobId);
                job.setString(2, taskType);
                job.setString(3, requesterId);
                job.setString(4, "RUNNING");
                job.setLong(5, System.currentTimeMillis());
                job.setInt(6, fileCount);
                if (job.executeUpdate() <= 0) {
                    throw new SQLException("No job row inserted.");
                }

                for (String taskId : taskIds) {
                    task.setString(1, taskId);
                    task.setString(2, jobId);
                    task.setString(3, "PENDING");
                    if (task.executeUpdate() <= 0) {
                        throw new SQLException("No task row inserted for task " + taskId);
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
        String sql = "INSERT INTO jobs(job_id,task_type,requester_node_id,status,submitted_at,file_count) VALUES(?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            ps.setString(2, taskType);
            ps.setString(3, requesterId);
            ps.setString(4, "RUNNING");
            ps.setLong(5, System.currentTimeMillis());
            ps.setInt(6, fileCount);
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

    public synchronized boolean markTaskAssigned(String taskId, String peerId, long startedAt) {
        String sql = "UPDATE tasks SET status='ASSIGNED', assigned_peer_id=?, started_at=? WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peerId);
            ps.setLong(2, startedAt);
            ps.setString(3, taskId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markTaskAssigned", e);
            return false;
        }
    }

    public synchronized boolean markTaskCompleted(String taskId, long completedAt, long durationMs) {
        String sql = "UPDATE tasks SET status='COMPLETED', completed_at=?, duration_ms=? WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, completedAt);
            ps.setLong(2, durationMs);
            ps.setString(3, taskId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markTaskCompleted", e);
            return false;
        }
    }

    public synchronized boolean markTaskRetried(String taskId, int retryCount) {
        String sql = """
                UPDATE tasks
                SET status='PENDING',
                    assigned_peer_id=NULL,
                    started_at=NULL,
                    completed_at=NULL,
                    duration_ms=NULL,
                    retry_count=?
                WHERE task_id=?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, retryCount);
            ps.setString(2, taskId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markTaskRetried", e);
            return false;
        }
    }

    public synchronized boolean markTaskFailed(String taskId) {
        String sql = "UPDATE tasks SET status='FAILED', completed_at=? WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, taskId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markTaskFailed", e);
            return false;
        }
    }

    public synchronized boolean markJobCompleted(String jobId) {
        String sql = "UPDATE jobs SET status='COMPLETED', completed_at=? WHERE job_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, jobId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logSqlFailure("markJobCompleted", e);
            return false;
        }
    }

    public synchronized boolean markJobFailed(String jobId) {
        String sql = "UPDATE jobs SET status='FAILED', completed_at=? WHERE job_id=?";
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
            try (PreparedStatement failTasks = conn.prepareStatement(failTasksSql);
                 PreparedStatement failJobs = conn.prepareStatement(failJobsSql)) {
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
}
