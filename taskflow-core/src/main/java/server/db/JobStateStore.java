package server.db;

import java.util.Collection;

public interface JobStateStore {
    boolean insertJobWithTasks(String jobId,
                               String taskType,
                               String requesterId,
                               int fileCount,
                               Collection<String> taskIds);

    boolean insertJob(String jobId, String taskType, String requesterId, int fileCount);

    boolean insertTask(String taskId, String jobId);

    boolean markTaskAssigned(String taskId, String peerId, long startedAt);

    boolean markTaskCompleted(String taskId, long completedAt, long durationMs);

    boolean markTaskRetried(String taskId, int retryCount);

    boolean markTaskFailed(String taskId);

    boolean markJobCompleted(String jobId);

    boolean markJobFailed(String jobId);

    int markRunningJobsFailedOnStartup(long completedAt);
}
