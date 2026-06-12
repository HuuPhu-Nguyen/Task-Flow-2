package server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseManager.class);

    public static final String DB_PATH = "taskflow.db";

    private final Connection conn;

    public DatabaseManager() throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
        }
        initSchema();
    }

    private void initSchema() throws SQLException {
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
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id          TEXT    PRIMARY KEY,
                    job_id           TEXT    NOT NULL,
                    assigned_peer_id TEXT,
                    status           TEXT    NOT NULL DEFAULT 'PENDING',
                    started_at       INTEGER,
                    completed_at     INTEGER,
                    duration_ms      INTEGER,
                    retry_count      INTEGER NOT NULL DEFAULT 0
                )
            """);
        }
    }

    // -------------------------------------------------------------------------
    // Write methods (called from coordinator / scheduler thread)
    // -------------------------------------------------------------------------

    public synchronized void insertJob(String jobId, String taskType, String requesterId, int fileCount) {
        String sql = "INSERT OR IGNORE INTO jobs(job_id,task_type,requester_node_id,status,submitted_at,file_count) VALUES(?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            ps.setString(2, taskType);
            ps.setString(3, requesterId);
            ps.setString(4, "RUNNING");
            ps.setLong(5, System.currentTimeMillis());
            ps.setInt(6, fileCount);
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("insertJob", e);
        }
    }

    public synchronized void insertTask(String taskId, String jobId) {
        String sql = "INSERT OR IGNORE INTO tasks(task_id,job_id,status) VALUES(?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setString(2, jobId);
            ps.setString(3, "PENDING");
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("insertTask", e);
        }
    }

    public synchronized void markTaskAssigned(String taskId, String peerId, long startedAt) {
        String sql = "UPDATE tasks SET status='ASSIGNED', assigned_peer_id=?, started_at=? WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, peerId);
            ps.setLong(2, startedAt);
            ps.setString(3, taskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("markTaskAssigned", e);
        }
    }

    public synchronized void markTaskCompleted(String taskId, long completedAt, long durationMs) {
        String sql = "UPDATE tasks SET status='COMPLETED', completed_at=?, duration_ms=? WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, completedAt);
            ps.setLong(2, durationMs);
            ps.setString(3, taskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("markTaskCompleted", e);
        }
    }

    public synchronized void markTaskRetried(String taskId, int retryCount) {
        String sql = "UPDATE tasks SET status='PENDING', retry_count=? WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, retryCount);
            ps.setString(2, taskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("markTaskRetried", e);
        }
    }

    public synchronized void markTaskFailed(String taskId) {
        String sql = "UPDATE tasks SET status='FAILED', completed_at=? WHERE task_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, taskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("markTaskFailed", e);
        }
    }

    public synchronized void markJobCompleted(String jobId) {
        String sql = "UPDATE jobs SET status='COMPLETED', completed_at=? WHERE job_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("markJobCompleted", e);
        }
    }

    public synchronized void markJobFailed(String jobId) {
        String sql = "UPDATE jobs SET status='FAILED', completed_at=? WHERE job_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logSqlFailure("markJobFailed", e);
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
