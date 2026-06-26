package server.db;

import java.util.List;
import java.util.Collection;
import java.util.Optional;

public interface JobStateStore {
    record TaskStartupState(String taskId, Object payload) {
    }

    record ResumableTaskState(String taskId,
                              String status,
                              Object payload,
                              Object resultPayload,
                              int retryCount) {
    }

    record ResumableJobState(String jobId,
                             String taskType,
                             String requesterId,
                             String requesterTokenHash,
                             String parameter,
                             List<ResumableTaskState> tasks) {
    }

    record CompletedJobResultState(String jobId,
                                   String taskType,
                                   String requesterTokenHash,
                                   List<Object> resultsByTaskId) {
    }

    boolean insertJobWithTasks(String jobId,
                               String taskType,
                               String requesterId,
                               int fileCount,
                               Collection<String> taskIds);

    default boolean insertJobWithTasks(String jobId,
                                       String taskType,
                                       String requesterId,
                                       String parameter,
                                       Collection<TaskStartupState> tasks) {
        return insertJobWithTasks(
                jobId,
                taskType,
                requesterId,
                "",
                parameter,
                tasks
        );
    }

    default boolean insertJobWithTasks(String jobId,
                                       String taskType,
                                       String requesterId,
                                       String requesterTokenHash,
                                       String parameter,
                                       Collection<TaskStartupState> tasks) {
        return insertJobWithTasks(
                jobId,
                taskType,
                requesterId,
                tasks.size(),
                tasks.stream().map(TaskStartupState::taskId).toList()
        );
    }

    boolean insertJob(String jobId, String taskType, String requesterId, int fileCount);

    boolean insertTask(String taskId, String jobId);

    boolean markTaskAssigned(String taskId, String peerId, long startedAt);

    boolean markTaskCompleted(String taskId, long completedAt, long durationMs);

    default boolean markTaskCompleted(String taskId,
                                      long completedAt,
                                      long durationMs,
                                      Object resultPayload) {
        return markTaskCompleted(taskId, completedAt, durationMs);
    }

    boolean markTaskRetried(String taskId, int retryCount);

    boolean markTaskFailed(String taskId);

    boolean markJobCompleted(String jobId);

    boolean markJobFailed(String jobId);

    int markRunningJobsFailedOnStartup(long completedAt);

    default List<ResumableJobState> loadRunningJobsForResume() {
        return List.of();
    }

    default Optional<CompletedJobResultState> loadCompletedJobResult(String jobId) {
        return Optional.empty();
    }

    default boolean resetTaskForResume(String taskId) {
        return true;
    }

    default boolean markRunningJobFailedOnStartup(String jobId, long completedAt) {
        return markJobFailed(jobId);
    }
}
