package server.job;

import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class EmbarrassinglyParallelJob<T, R> {
    protected final String jobId;
    protected final String requesterNodeId;
    protected final String taskType;

    // Mapping TaskID to the internal tracking unit
    protected final Map<String, TaskUnit<T>> tasks = new ConcurrentHashMap<>();

    protected final AtomicInteger completedCount = new AtomicInteger(0);

    public EmbarrassinglyParallelJob(String jobId, String requesterNodeId, String taskType) {
        this.jobId = jobId;
        this.requesterNodeId = requesterNodeId;
        this.taskType = taskType;
    }

    public abstract void initializeTasks(JobSubmitMessage message);

    public record TaskCompletion(boolean accepted, long durationMs) {}

    public record PreparedTaskResult(Object resultData) {}

    public PreparedTaskResult prepareTaskResult(Object rawResultData) {
        return new PreparedTaskResult(parseResult(rawResultData));
    }

    /**
     * Updates the in-memory projection after the authoritative store has
     * committed the exact assignment generation.
     */
    @SuppressWarnings("unchecked")
    public synchronized TaskCompletion applyCommittedResult(String taskId,
                                                            String reportingPeerId,
                                                            int attemptNumber,
                                                            String assignmentId,
                                                            long completedAt,
                                                            PreparedTaskResult preparedResult) {
        TaskUnit<T> task = tasks.get(taskId);
        if (task == null || preparedResult == null) {
            return new TaskCompletion(false, -1L);
        }

        long durationMs = task.markCompletedBy(
                reportingPeerId,
                attemptNumber,
                assignmentId,
                completedAt
        );
        if (durationMs < 0L) {
            return new TaskCompletion(false, -1L);
        }

        onTaskSuccess(task, (R) preparedResult.resultData());
        completedCount.incrementAndGet();
        return new TaskCompletion(true, durationMs);
    }

    /**
     * Idempotent result path:
     * only accepts a result from the currently assigned peer for this task.
     */
    public synchronized TaskCompletion recordResult(String taskId, String reportingPeerId, Object rawResultData) {
        TaskUnit<T> task = tasks.get(taskId);

        if (task == null) {
            return new TaskCompletion(false, -1);
        }

        Optional<AssignmentIdentity> identity = task.getAssignmentIdentity();
        if (identity.isEmpty() || !identity.get().workerId().equals(reportingPeerId)) {
            return new TaskCompletion(false, -1);
        }

        AssignmentIdentity current = identity.get();
        return applyCommittedResult(
                taskId,
                reportingPeerId,
                current.attemptNumber(),
                current.assignmentId(),
                System.currentTimeMillis(),
                prepareTaskResult(rawResultData)
        );
    }

    public boolean isJobComplete() {
        return !tasks.isEmpty() && completedCount.get() == tasks.size();
    }

    public boolean hasTerminalFailure() {
        return tasks.values().stream().anyMatch(t -> t.getStatus() == TaskUnit.TaskStatus.FAILED);
    }

    @SuppressWarnings("unchecked")
    public synchronized boolean restoreTaskForResume(String taskId,
                                                     TaskUnit.TaskStatus status,
                                                     Object rawResultData,
                                                     int retryCount) {
        return restoreTaskForResume(taskId, status, rawResultData, retryCount, "", 0L, "", 0L);
    }

    @SuppressWarnings("unchecked")
    public synchronized boolean restoreTaskForResume(String taskId,
                                                     TaskUnit.TaskStatus status,
                                                     Object rawResultData,
                                                     int retryCount,
                                                     String assignedPeerId,
                                                     long startedAt,
                                                     String leaseOwnerId,
                                                     long leaseExpiresAt) {
        return restoreTaskForResume(
                taskId,
                status,
                rawResultData,
                retryCount,
                assignedPeerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                0
        );
    }

    @SuppressWarnings("unchecked")
    public synchronized boolean restoreTaskForResume(String taskId,
                                                     TaskUnit.TaskStatus status,
                                                     Object rawResultData,
                                                     int retryCount,
                                                     String assignedPeerId,
                                                     long startedAt,
                                                     String leaseOwnerId,
                                                     long leaseExpiresAt,
                                                     int lastAssignmentAttemptNumber) {
        return restoreTaskForResume(
                taskId,
                status,
                rawResultData,
                retryCount,
                assignedPeerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                lastAssignmentAttemptNumber,
                null
        );
    }

    @SuppressWarnings("unchecked")
    public synchronized boolean restoreTaskForResume(String taskId,
                                                     TaskUnit.TaskStatus status,
                                                     Object rawResultData,
                                                     int retryCount,
                                                     String assignedPeerId,
                                                     long startedAt,
                                                     String leaseOwnerId,
                                                     long leaseExpiresAt,
                                                     int lastAssignmentAttemptNumber,
                                                     String assignmentId) {
        TaskUnit<T> task = tasks.get(taskId);
        if (task == null || status == null) {
            return false;
        }

        if (status == TaskUnit.TaskStatus.COMPLETED) {
            if (rawResultData == null) {
                return false;
            }
            R resultData = parseResult(rawResultData);
            task.restoreCompletedForResume(retryCount, lastAssignmentAttemptNumber);
            onTaskSuccess(task, resultData);
            completedCount.incrementAndGet();
            return true;
        }

        if (status == TaskUnit.TaskStatus.FAILED) {
            task.restoreFailedForResume(retryCount, lastAssignmentAttemptNumber);
            return true;
        }

        if (status == TaskUnit.TaskStatus.ASSIGNED) {
            if (assignedPeerId == null || assignedPeerId.isBlank()) {
                return false;
            }
            if (assignmentId == null || assignmentId.isBlank()) {
                task.restoreAssignedForResume(
                        assignedPeerId,
                        startedAt,
                        leaseOwnerId,
                        leaseExpiresAt,
                        retryCount,
                        lastAssignmentAttemptNumber
                );
            } else {
                task.restoreAssignedForResume(
                        new AssignmentIdentity(
                                taskId,
                                lastAssignmentAttemptNumber,
                                assignmentId,
                                assignedPeerId,
                                leaseExpiresAt
                        ),
                        startedAt,
                        leaseOwnerId,
                        retryCount
                );
            }
            return true;
        }

        task.restorePendingForResume(retryCount, lastAssignmentAttemptNumber);
        return true;
    }

    public int getFailedCount() {
        return (int) tasks.values().stream()
                .filter(t -> t.getStatus() == TaskUnit.TaskStatus.FAILED)
                .count();
    }

    protected abstract void onTaskSuccess(TaskUnit<T> task, R resultData);

    public abstract List<Object> aggregateAndSendResult();

    public Object aggregateResultPayload() {
        return aggregateAndSendResult();
    }

    protected abstract R parseResult(Object payloads);

    public List<TaskUnit<T>> getPendingTasks() {
        return tasks.values().stream()
                .filter(t -> t.getStatus() == TaskUnit.TaskStatus.PENDING)
                .toList();
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskType(){
        return taskType;
    }

    public Map<String, TaskUnit<T>> getTasks() {
        return tasks;
    }

    public String getRequesterNodeId() {
        return requesterNodeId;
    }

    public abstract TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task);
}
