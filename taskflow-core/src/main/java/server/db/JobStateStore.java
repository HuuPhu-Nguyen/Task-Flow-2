package server.db;

import java.util.List;
import java.util.Collection;
import java.util.Optional;

public interface JobStateStore {
    /**
     * Typed disposition for correctness-relevant durable transitions other
     * than the specialized successful-result fence below.
     */
    enum DurableTransitionOutcome {
        COMMITTED,
        ALREADY_APPLIED,
        STALE_STATE,
        UNKNOWN_ENTITY,
        STORAGE_FAILURE;

        public boolean projectionAllowed() {
            return this == COMMITTED || this == ALREADY_APPLIED;
        }
    }

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

    record TaskFailureUpdate(String taskId,
                             TaskAttemptOutcome outcome,
                             String failureReason,
                             long finishedAt) {
        public TaskFailureUpdate {
            if (taskId == null || taskId.isBlank()) {
                throw new IllegalArgumentException("taskId is required");
            }
            outcome = outcome == null ? TaskAttemptOutcome.JOB_FAILED : outcome;
            failureReason = failureReason == null ? "" : failureReason;
            finishedAt = Math.max(0L, finishedAt);
        }
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

    /**
     * Durable task snapshot used for restart hydration. {@code
     * resultPayloadPresent} distinguishes a stored JSON {@code null} result
     * from an absent SQL result value.
     */
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
                              String assignmentId,
                              boolean resultPayloadPresent) {
        public ResumableTaskState(String taskId,
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
                    attemptNumber,
                    assignmentId,
                    resultPayload != null
            );
        }

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
                    null,
                    resultPayload != null
            );
        }

        public ResumableTaskState(String taskId,
                                  String status,
                                  Object payload,
                                  Object resultPayload,
                                  int retryCount) {
            this(
                    taskId,
                    status,
                    payload,
                    resultPayload,
                    retryCount,
                    "",
                    0L,
                    "",
                    0L,
                    0,
                    null,
                    resultPayload != null
            );
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

    default DurableTransitionOutcome commitTaskAssignment(String taskId,
                                                           String peerId,
                                                           long startedAt,
                                                           String leaseOwnerId,
                                                           long leaseExpiresAt,
                                                           int attemptNumber,
                                                           String assignmentId) {
        return markTaskAssigned(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                attemptNumber,
                assignmentId
        ) ? DurableTransitionOutcome.COMMITTED : DurableTransitionOutcome.STORAGE_FAILURE;
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
     * comparison and task transition atomically. When that result completes
     * every task successfully, the same transaction must also persist a
     * replayable job-finalization intent before returning {@link
     * ResultCommitOutcome#COMMITTED}.
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

    /**
     * Conditionally closes one exact running assignment as retryable or
     * terminal. Implementations with durable assignment identity must fence
     * the write by the complete generation tuple.
     */
    default DurableTransitionOutcome commitAssignedTaskFailure(String taskId,
                                                                int attemptNumber,
                                                                String assignmentId,
                                                                String assignedPeerId,
                                                                int retryCount,
                                                                TaskAttemptOutcome outcome,
                                                                String failureReason,
                                                                long finishedAt) {
        boolean committed = outcome == TaskAttemptOutcome.TERMINAL_FAILURE
                ? markTaskFailed(taskId, outcome, failureReason, finishedAt)
                : markTaskRetried(taskId, retryCount, outcome, failureReason, finishedAt);
        return committed
                ? DurableTransitionOutcome.COMMITTED
                : DurableTransitionOutcome.STORAGE_FAILURE;
    }

    boolean markJobCompleted(String jobId);

    default boolean markJobCompleted(String jobId, Object resultPayload) {
        return markJobCompleted(jobId);
    }

    boolean markJobFailed(String jobId);

    default DurableTransitionOutcome commitJobCompleted(String jobId,
                                                         Object resultPayload,
                                                         long completedAt) {
        return markJobCompleted(jobId, resultPayload)
                ? DurableTransitionOutcome.COMMITTED
                : DurableTransitionOutcome.STORAGE_FAILURE;
    }

    default DurableTransitionOutcome commitJobFailed(String jobId,
                                                      Collection<TaskFailureUpdate> taskFailures,
                                                      long completedAt) {
        Collection<TaskFailureUpdate> failures = taskFailures == null ? List.of() : taskFailures;
        for (TaskFailureUpdate failure : failures) {
            if (!markTaskFailed(
                    failure.taskId(),
                    failure.outcome(),
                    failure.failureReason(),
                    failure.finishedAt()
            )) {
                return DurableTransitionOutcome.STORAGE_FAILURE;
            }
        }
        return markJobFailed(jobId)
                ? DurableTransitionOutcome.COMMITTED
                : DurableTransitionOutcome.STORAGE_FAILURE;
    }

    int markRunningJobsFailedOnStartup(long completedAt);

    /**
     * Loads resumable nonterminal jobs. Implementations with a durable
     * finalization state include both ordinary running work and jobs whose
     * committed task results are ready for terminal aggregation.
     */
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
