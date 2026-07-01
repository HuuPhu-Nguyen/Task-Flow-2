package server.db;

import java.util.List;
import java.util.Collection;
import java.util.Optional;

public interface JobStateStore {
    enum TaskAttemptOutcome {
        RUNNING,
        SUCCEEDED,
        RETRY_SCHEDULED,
        TERMINAL_FAILURE,
        DISPATCH_FAILED,
        JOB_FAILED
    }

    record TaskStartupState(String taskId, Object payload) {
    }

    record TaskAttemptRecord(String jobId,
                             String taskId,
                             int attemptNumber,
                             String peerId,
                             long startedAt,
                             long finishedAt,
                             long durationMs,
                             TaskAttemptOutcome outcome,
                             String failureReason) {
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
                             String requesterIdentityKey,
                             String parameter,
                             List<ResumableTaskState> tasks) {
    }

    record CompletedJobResultState(String jobId,
                                   String taskType,
                                   String requesterTokenHash,
                                   String requesterIdentityKey,
                                   Object resultPayload,
                                   List<Object> resultsByTaskId) {
        public CompletedJobResultState(String jobId,
                                       String taskType,
                                       String requesterTokenHash,
                                       String requesterIdentityKey,
                                       List<Object> resultsByTaskId) {
            this(jobId, taskType, requesterTokenHash, requesterIdentityKey, resultsByTaskId, resultsByTaskId);
        }
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
                requesterTokenHash,
                "",
                parameter,
                tasks
        );
    }

    default boolean insertJobWithTasks(String jobId,
                                       String taskType,
                                       String requesterId,
                                       String requesterTokenHash,
                                       String requesterIdentityKey,
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

    default boolean hasJob(String jobId) {
        return false;
    }

    boolean markTaskAssigned(String taskId, String peerId, long startedAt);

    boolean markTaskCompleted(String taskId, long completedAt, long durationMs);

    default boolean markTaskCompleted(String taskId,
                                      long completedAt,
                                      long durationMs,
                                      Object resultPayload) {
        return markTaskCompleted(taskId, completedAt, durationMs);
    }

    boolean markTaskRetried(String taskId, int retryCount);

    default boolean markTaskRetried(String taskId,
                                    int retryCount,
                                    TaskAttemptOutcome outcome,
                                    String failureReason,
                                    long finishedAt) {
        return markTaskRetried(taskId, retryCount);
    }

    boolean markTaskFailed(String taskId);

    default boolean markTaskFailed(String taskId,
                                   TaskAttemptOutcome outcome,
                                   String failureReason,
                                   long finishedAt) {
        return markTaskFailed(taskId);
    }

    boolean markJobCompleted(String jobId);

    default boolean markJobCompleted(String jobId, Object resultPayload) {
        return markJobCompleted(jobId);
    }

    boolean markJobFailed(String jobId);

    int markRunningJobsFailedOnStartup(long completedAt);

    default List<ResumableJobState> loadRunningJobsForResume() {
        return List.of();
    }

    default Optional<CompletedJobResultState> loadCompletedJobResult(String jobId) {
        return Optional.empty();
    }

    default List<TaskAttemptRecord> loadTaskAttempts(String jobId) {
        return List.of();
    }

    default boolean resetTaskForResume(String taskId) {
        return true;
    }

    default boolean markRunningJobFailedOnStartup(String jobId, long completedAt) {
        return markJobFailed(jobId);
    }
}
