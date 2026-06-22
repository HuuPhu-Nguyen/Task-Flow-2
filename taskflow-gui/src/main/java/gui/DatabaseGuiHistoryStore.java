package gui;

import server.db.DatabaseManager;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

final class DatabaseGuiHistoryStore implements GuiHistoryStore {
    private final DatabaseManager database;

    DatabaseGuiHistoryStore() throws SQLException {
        this(new DatabaseManager());
    }

    DatabaseGuiHistoryStore(DatabaseManager database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    @Override
    public List<JobRecord> getJobHistory() {
        return database.getJobHistory().stream()
                .map(job -> new JobRecord(
                        job.jobId(),
                        job.taskType(),
                        job.requesterId(),
                        job.status(),
                        job.submittedAt(),
                        job.completedAt(),
                        job.fileCount()
                ))
                .toList();
    }

    @Override
    public List<TaskRecord> getTasksForJob(String jobId) {
        return database.getTasksForJob(jobId).stream()
                .map(task -> new TaskRecord(
                        task.taskId(),
                        task.jobId(),
                        task.assignedPeerId(),
                        task.status(),
                        task.startedAt(),
                        task.completedAt(),
                        task.durationMs(),
                        task.retryCount()
                ))
                .toList();
    }

    @Override
    public void close() {
        database.close();
    }
}
