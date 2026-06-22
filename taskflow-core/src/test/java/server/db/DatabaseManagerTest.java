package server.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void persistsJobAndTaskLifecycleToConfiguredDatabasePath() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-1", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-1", "job-1");
            db.markTaskAssigned("task-1", "peer-1", 123L);
            db.markTaskCompleted("task-1", 456L, 333L);
            db.markJobCompleted("job-1");

            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, db.getSchemaVersion());

            List<DatabaseManager.JobRecord> jobs = db.getJobHistory();
            assertEquals(1, jobs.size());
            DatabaseManager.JobRecord job = jobs.getFirst();
            assertEquals("job-1", job.jobId());
            assertEquals("TEST_TASK", job.taskType());
            assertEquals("requester-1", job.requesterId());
            assertEquals("COMPLETED", job.status());
            assertEquals(1, job.fileCount());

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-1");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("task-1", task.taskId());
            assertEquals("job-1", task.jobId());
            assertEquals("peer-1", task.assignedPeerId());
            assertEquals("COMPLETED", task.status());
            assertEquals(123L, task.startedAt());
            assertEquals(456L, task.completedAt());
            assertEquals(333L, task.durationMs());
            assertEquals(0, task.retryCount());
        } finally {
            db.close();
        }
    }

    @Test
    void rejectsTaskRowsWithoutExistingJob() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-foreign-key-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertFalse(db.insertTask("orphan-task", "missing-job"));
            assertEquals(0, db.getTasksForJob("missing-job").size());
            assertTrue(tasksTableReferencesJobs(dbPath));
        } finally {
            db.close();
        }
    }

    @Test
    void retriedTaskRowsClearPreviousAssignmentState() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-retry-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-retry", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-retry", "job-retry");
            db.markTaskAssigned("task-retry", "peer-1", 123L);

            assertTrue(db.markTaskRetried("task-retry", 1));

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-retry");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("PENDING", task.status());
            assertEquals(1, task.retryCount());
            assertNull(task.assignedPeerId());
            assertEquals(0L, task.startedAt());
            assertEquals(0L, task.completedAt());
            assertEquals(0L, task.durationMs());
        } finally {
            db.close();
        }
    }

    @Test
    void migratesLegacyTasksTableToForeignKeySchema() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-legacy-migration-test.db");
        createLegacyDatabaseWithoutTaskForeignKey(dbPath);

        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, db.getSchemaVersion());
            assertTrue(tasksTableReferencesJobs(dbPath));
            assertEquals(1, db.getTasksForJob("legacy-job").size());
            assertFalse(db.insertTask("orphan-task", "missing-job"));
        } finally {
            db.close();
        }
    }

    @Test
    void rejectsDatabaseSchemaNewerThanRuntimeSupports() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-future-schema-test.db");
        createFutureSchemaVersionDatabase(dbPath);

        SQLException failure = assertThrows(SQLException.class, () -> new DatabaseManager(dbPath.toString()));
        assertTrue(failure.getMessage().contains("newer than supported version"));
    }

    @Test
    void marksRunningJobsAndNonTerminalTasksFailedOnStartup() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-recovery-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("running-job", "TEST_TASK", "requester-1", 2);
            db.insertTask("completed-task", "running-job");
            db.insertTask("assigned-task", "running-job");
            db.markTaskAssigned("completed-task", "peer-1", 100L);
            db.markTaskCompleted("completed-task", 200L, 100L);
            db.markTaskAssigned("assigned-task", "peer-2", 150L);

            db.insertJob("completed-job", "TEST_TASK", "requester-2", 1);
            db.insertTask("completed-job-task", "completed-job");
            db.markTaskCompleted("completed-job-task", 250L, 50L);
            db.markJobCompleted("completed-job");

            assertEquals(1, db.markRunningJobsFailedOnStartup(999L));

            DatabaseManager.JobRecord runningJob = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("running-job"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", runningJob.status());
            assertEquals(999L, runningJob.completedAt());

            DatabaseManager.JobRecord completedJob = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("completed-job"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completedJob.status());
            assertTrue(completedJob.completedAt() > 0L);

            List<DatabaseManager.TaskRecord> runningTasks = db.getTasksForJob("running-job");
            DatabaseManager.TaskRecord completedTask = runningTasks.stream()
                    .filter(task -> task.taskId().equals("completed-task"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completedTask.status());
            assertEquals(200L, completedTask.completedAt());

            DatabaseManager.TaskRecord assignedTask = runningTasks.stream()
                    .filter(task -> task.taskId().equals("assigned-task"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", assignedTask.status());
            assertEquals(999L, assignedTask.completedAt());
        } finally {
            db.close();
        }
    }

    @Test
    void rollsBackAtomicJobStartupWhenTaskInsertFails() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-startup-rollback-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("existing-job", "TEST_TASK", "requester-1", 1);
            db.insertTask("duplicate-task", "existing-job");

            assertFalse(db.insertJobWithTasks(
                    "new-job",
                    "TEST_TASK",
                    "requester-2",
                    1,
                    List.of("duplicate-task")
            ));

            assertTrue(db.getJobHistory().stream().noneMatch(job -> job.jobId().equals("new-job")));
            assertEquals(0, db.getTasksForJob("new-job").size());
            assertEquals(1, db.getTasksForJob("existing-job").size());
        } finally {
            db.close();
        }
    }

    private static void createLegacyDatabaseWithoutTaskForeignKey(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE jobs (
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
                CREATE TABLE tasks (
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
            stmt.execute("""
                INSERT INTO jobs(job_id, task_type, requester_node_id, status, submitted_at, file_count)
                VALUES('legacy-job', 'TEST_TASK', 'requester-1', 'RUNNING', 100, 1)
            """);
            stmt.execute("""
                INSERT INTO tasks(task_id, job_id, status)
                VALUES('legacy-task', 'legacy-job', 'PENDING')
            """);
        }
    }

    private static void createFutureSchemaVersionDatabase(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE schema_version (
                    id         INTEGER PRIMARY KEY CHECK (id = 1),
                    version    INTEGER NOT NULL CHECK (version >= 0),
                    applied_at INTEGER NOT NULL
                )
            """);
            stmt.execute("INSERT INTO schema_version(id, version, applied_at) VALUES(1, 999, 100)");
        }
    }

    private static boolean tasksTableReferencesJobs(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA foreign_key_list(tasks)")) {
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
}
