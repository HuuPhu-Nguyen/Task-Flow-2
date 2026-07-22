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

    /**
     * Authoritative result-commit disposition returned by the state store.
     */
    enum ResultCommitOutcome {
        COMMITTED,
        DUPLICATE_ALREADY_COMPLETED,
        STALE_ASSIGNMENT,
        UNKNOWN_TASK,
        STORAGE_FAILURE
    }

    record TaskStartupState(String taskId, Object payload) {
    }

    record TaskAttemptRecord(String jobId,
                             String taskId,
                             int attemptNumber,
                             String assignmentId,
                             String peerId,
                             long startedAt,
                             long leaseExpiresAt,
                             long finishedAt,
                             long durationMs,
                             TaskAttemptOutcome outcome,
                             String failureReason) {
        public TaskAttemptRecord(String jobId,
                                 String taskId,
                                 int attemptNumber,
                                 String peerId,
                                 long startedAt,
                                 long finishedAt,
                                 long durationMs,
                                 TaskAttemptOutcome outcome,
                                 String failureReason) {
            this(
                    jobId,
                    taskId,
                    attemptNumber,
                    null,
                    peerId,
                    startedAt,
                    0L,
                    finishedAt,
                    durationMs,
                    outcome,
                    failureReason
            );
        }
    }

    record ResumableTaskState(String taskId,
                              String status,
                              Object payload,
                              Object resultPayload,
                              int retryCount,
                              String assignedPeerId,
                              long startedAt,
                              String leaseOwnerId,
                              long leaseExpiresAt,
                              int attemptNumber,
                              String assignmentId) {
        public ResumableTaskState(String taskId,
                                  String status,
                                  Object payload,
                                  Object resultPayload,
                                  int retryCount,
                                  String assignedPeerId,
                                  long startedAt,
                                  String leaseOwnerId,
                                  long leaseExpiresAt) {
            this(
                    taskId,
                    status,
                    payload,
                    resultPayload,
                    retryCount,
                    assignedPeerId,
                    startedAt,
                    leaseOwnerId,
                    leaseExpiresAt,
                    0,
                    null
            );
        }

        public ResumableTaskState(String taskId,
                                  String status,
                                  Object payload,
                                  Object resultPayload,
                                  int retryCount) {
            this(taskId, status, payload, resultPayload, retryCount, "", 0L, "", 0L, 0, null);
        }
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

    default boolean markTaskAssigned(String taskId,
                                     String peerId,
                                     long startedAt,
                                     String leaseOwnerId,
                                     long leaseExpiresAt) {
        return markTaskAssigned(taskId, peerId, startedAt);
    }

    default boolean markTaskAssigned(String taskId,
                                     String peerId,
                                     long startedAt,
                                     String leaseOwnerId,
                                     long leaseExpiresAt,
                                     int attemptNumber,
                                     String assignmentId) {
        return markTaskAssigned(taskId, peerId, startedAt, leaseOwnerId, leaseExpiresAt);
    }

    boolean markTaskCompleted(String taskId, long completedAt, long durationMs);

    default boolean markTaskCompleted(String taskId,
                                      long completedAt,
                                      long durationMs,
                                      Object resultPayload) {
        return markTaskCompleted(taskId, completedAt, durationMs);
    }

    /**
     * Conditionally commits a successful result for one exact assignment
     * generation. Persistent implementations must make the complete tuple
     * comparison and task transition atomically.
     */
    default ResultCommitOutcome commitTaskResult(String taskId,
                                                 int attemptNumber,
                                                 String assignmentId,
                                                 String assignedPeerId,
                                                 long completedAt,
                                                 long durationMs,
                                                 Object resultPayload) {
        return markTaskCompleted(taskId, completedAt, durationMs, resultPayload)
                ? ResultCommitOutcome.COMMITTED
                : ResultCommitOutcome.STORAGE_FAILURE;
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

    default boolean resetTaskForResume(String taskId, int lastAssignmentAttemptNumber) {
        return resetTaskForResume(taskId);
    }

    default boolean releaseExpiredTaskLeaseForResume(String taskId, long releasedAt) {
        return resetTaskForResume(taskId);
    }

    default boolean releaseExpiredTaskLeaseForResume(String taskId,
                                                     long releasedAt,
                                                     int lastAssignmentAttemptNumber) {
        return releaseExpiredTaskLeaseForResume(taskId, releasedAt);
    }

    default boolean markRunningJobFailedOnStartup(String jobId, long completedAt) {
        return markJobFailed(jobId);
    }
}
