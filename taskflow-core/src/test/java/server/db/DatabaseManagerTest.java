package server.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
