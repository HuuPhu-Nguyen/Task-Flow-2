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

    private final Connection conn;

    public DatabaseManager() throws SQLException {
        this(DB_PATH);
    }

    public DatabaseManager(String dbPath) throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite:" + Objects.requireNonNull(dbPath, "dbPath"));
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
        String sql = "UPDATE tasks SET status='PENDING', retry_count=? WHERE task_id=?";
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
