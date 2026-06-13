package server.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
