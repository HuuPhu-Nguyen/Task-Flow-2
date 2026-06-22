package gui;

import java.util.List;

interface GuiHistoryStore extends AutoCloseable {
    List<JobRecord> getJobHistory();

    List<TaskRecord> getTasksForJob(String jobId);

    @Override
    void close();

    record JobRecord(
            String jobId,
            String taskType,
            String requesterId,
            String status,
            long submittedAt,
            long completedAt,
            int fileCount
    ) {
    }

    record TaskRecord(
            String taskId,
            String jobId,
            String assignedPeerId,
            String status,
            long startedAt,
            long completedAt,
            long durationMs,
            int retryCount
    ) {
    }
}
