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

    /**
     * Idempotent result path:
     * only accepts a result from the currently assigned peer for this task.
     */
    public synchronized TaskCompletion recordResult(String taskId, String reportingPeerId, Object rawResultData) {
        TaskUnit<T> task = tasks.get(taskId);

        if (task == null) {
            return new TaskCompletion(false, -1);
        }

        long durationMs = task.markCompletedBy(reportingPeerId);
        if (durationMs < 0) {
            return new TaskCompletion(false, -1);
        }

        R resultData = parseResult(rawResultData);
        onTaskSuccess(task, resultData);
        completedCount.incrementAndGet();
        return new TaskCompletion(true, durationMs);
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
        TaskUnit<T> task = tasks.get(taskId);
        if (task == null || status == null) {
            return false;
        }

        if (status == TaskUnit.TaskStatus.COMPLETED) {
            if (rawResultData == null) {
                return false;
            }
            R resultData = parseResult(rawResultData);
            task.restoreCompletedForResume(retryCount);
            onTaskSuccess(task, resultData);
            completedCount.incrementAndGet();
            return true;
        }

        if (status == TaskUnit.TaskStatus.FAILED) {
            task.restoreFailedForResume(retryCount);
            return true;
        }

        if (status == TaskUnit.TaskStatus.ASSIGNED) {
            if (assignedPeerId == null || assignedPeerId.isBlank()) {
                return false;
            }
            task.restoreAssignedForResume(assignedPeerId, startedAt, leaseOwnerId, leaseExpiresAt, retryCount);
            return true;
        }

        task.restorePendingForResume(retryCount);
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
