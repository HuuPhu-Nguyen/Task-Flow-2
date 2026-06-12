package server.db;

public interface JobStateStore {
    void insertJob(String jobId, String taskType, String requesterId, int fileCount);

    void insertTask(String taskId, String jobId);

    void markTaskAssigned(String taskId, String peerId, long startedAt);

    void markTaskCompleted(String taskId, long completedAt, long durationMs);

    void markTaskRetried(String taskId, int retryCount);

    void markTaskFailed(String taskId);

    void markJobCompleted(String jobId);

    void markJobFailed(String jobId);
}
