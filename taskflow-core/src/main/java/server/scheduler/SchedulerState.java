package server.scheduler;

import protocol.RequesterTokens;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Scheduler-owned in-memory projection indexes.
 *
 * <p>This object does not decide or execute task transitions. Focused services
 * update it only after their existing persistence/transition boundary permits
 * the projection change.</p>
 */
final class SchedulerState {
    private final Map<String, EmbarrassinglyParallelJob<?, ?>> activeJobs = new LinkedHashMap<>();
    private final Map<String, String> requesterTokenHashes = new LinkedHashMap<>();
    private final Map<String, String> requesterIdentityKeys = new LinkedHashMap<>();
    private final Map<String, String> requestHashes = new LinkedHashMap<>();
    private final SchedulerWorkloadIndex workloadIndex = new SchedulerWorkloadIndex();
    private final long taskTimeoutMillis;

    SchedulerState(SchedulerConfig config) {
        SchedulerConfig effectiveConfig = config == null ? SchedulerConfig.defaults() : config;
        this.taskTimeoutMillis = effectiveConfig.taskTimeoutMillis();
    }

    boolean hasActiveJob(String jobId) {
        return activeJobs.containsKey(jobId);
    }

    EmbarrassinglyParallelJob<?, ?> activeJob(String jobId) {
        return activeJobs.get(jobId);
    }

    List<EmbarrassinglyParallelJob<?, ?>> activeJobsSnapshot() {
        return List.copyOf(activeJobs.values());
    }

    int activeJobCount() {
        return activeJobs.size();
    }

    void addActiveJob(EmbarrassinglyParallelJob<?, ?> job,
                      String requesterTokenHash,
                      String requesterIdentityKey) {
        addActiveJob(job, requesterTokenHash, requesterIdentityKey, "");
    }

    void addActiveJob(EmbarrassinglyParallelJob<?, ?> job,
                      String requesterTokenHash,
                      String requesterIdentityKey,
                      String requestHash) {
        if (activeJobs.containsKey(job.getJobId())) {
            workloadIndex.removeJob(job.getJobId());
        }
        activeJobs.put(job.getJobId(), job);
        workloadIndex.indexJob(job, taskTimeoutMillis);
        if (RequesterTokens.hasTokenHash(requesterTokenHash)) {
            requesterTokenHashes.put(job.getJobId(), requesterTokenHash);
        }
        if (hasText(requesterIdentityKey)) {
            requesterIdentityKeys.put(job.getJobId(), requesterIdentityKey);
        }
        if (hasText(requestHash)) {
            requestHashes.put(job.getJobId(), requestHash);
        }
    }

    void removeJob(String jobId) {
        activeJobs.remove(jobId);
        workloadIndex.removeJob(jobId);
        requesterTokenHashes.remove(jobId);
        requesterIdentityKeys.remove(jobId);
        requestHashes.remove(jobId);
    }

    int runnableJobCount() {
        return workloadIndex.runnableJobCount();
    }

    int capacityWaitingJobCount() {
        return workloadIndex.capacityWaitingJobCount();
    }

    EmbarrassinglyParallelJob<?, ?> pollRunnableJob() {
        while (true) {
            String jobId = workloadIndex.pollRunnableJob();
            if (jobId == null) {
                return null;
            }
            EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(jobId);
            if (job != null && workloadIndex.pendingTaskCount(jobId) > 0) {
                return job;
            }
        }
    }

    void requeueRunnableJob(String jobId) {
        if (activeJobs.containsKey(jobId)) {
            workloadIndex.requeueRunnableJob(jobId);
        }
    }

    void waitForCapacity(String jobId, long signalGeneration) {
        if (activeJobs.containsKey(jobId)) {
            workloadIndex.waitForCapacity(jobId, signalGeneration);
        }
    }

    EmbarrassinglyParallelJob<?, ?> pollCapacityWaitingJob(
            long eligibleBeforeSignalGeneration) {
        while (true) {
            String jobId = workloadIndex.pollCapacityWaitingJob(
                    eligibleBeforeSignalGeneration
            );
            if (jobId == null) {
                return null;
            }
            EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(jobId);
            if (job != null && workloadIndex.pendingTaskCount(jobId) > 0) {
                return job;
            }
        }
    }

    boolean hasCapacityWaitingJobEligibleBefore(long signalGeneration) {
        return workloadIndex.hasCapacityWaitingJobEligibleBefore(signalGeneration);
    }

    int pendingTaskCount(String jobId) {
        return workloadIndex.pendingTaskCount(jobId);
    }

    TaskUnit<?> pollPendingTask(String jobId) {
        EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(jobId);
        if (job == null) {
            return null;
        }
        while (true) {
            String taskId = workloadIndex.pollPendingTask(jobId);
            if (taskId == null) {
                return null;
            }
            TaskUnit<?> task = job.getTasks().get(taskId);
            if (task != null && task.getStatus() == TaskUnit.TaskStatus.PENDING) {
                return task;
            }
        }
    }

    void indexPendingTask(TaskUnit<?> task, boolean retryPriority) {
        EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(task.getJobId());
        if (job == null
                || job.getTasks().get(task.getTaskId()) != task
                || task.getStatus() != TaskUnit.TaskStatus.PENDING) {
            return;
        }
        workloadIndex.addPendingTask(task.getJobId(), task.getTaskId(), retryPriority);
    }

    void indexAssignedTask(TaskUnit<?> task, AssignmentIdentity identity) {
        if (!isCurrentAssignment(task, identity)) {
            throw new IllegalStateException(
                    "Cannot index a deadline for a non-current assignment: " + task.getTaskId()
            );
        }
        workloadIndex.removePendingTask(task.getJobId(), task.getTaskId());
        workloadIndex.scheduleAssignment(
                task.getJobId(),
                task.getTaskId(),
                task.getStartTime(),
                taskTimeoutMillis,
                identity
        );
    }

    void indexClosedAssignment(TaskUnit<?> task, AssignmentIdentity closedIdentity) {
        workloadIndex.cancelAssignment(task.getJobId(), task.getTaskId(), closedIdentity);
        if (task.getStatus() == TaskUnit.TaskStatus.PENDING) {
            indexPendingTask(task, true);
        } else {
            workloadIndex.removePendingTask(task.getJobId(), task.getTaskId());
        }
    }

    void indexTerminalTask(TaskUnit<?> task, AssignmentIdentity closedIdentity) {
        workloadIndex.cancelAssignment(task.getJobId(), task.getTaskId(), closedIdentity);
        workloadIndex.removePendingTask(task.getJobId(), task.getTaskId());
    }

    DeadlineTarget pollDueTimeout(long nowMillis) {
        return pollDueDeadline(SchedulerWorkloadIndex.DeadlineKind.TASK_TIMEOUT, nowMillis);
    }

    DeadlineTarget pollDueLeaseExpiry(long nowMillis) {
        return pollDueDeadline(SchedulerWorkloadIndex.DeadlineKind.LEASE_EXPIRY, nowMillis);
    }

    DeadlinePoll pollNextDueDeadline(long nowMillis) {
        SchedulerWorkloadIndex.ScheduledDeadline deadline = workloadIndex.pollNextDue(nowMillis);
        if (deadline == null) {
            return DeadlinePoll.none();
        }
        EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(deadline.jobId());
        TaskUnit<?> task = job == null ? null : job.getTasks().get(deadline.taskId());
        boolean currentAssignment = task != null && matches(task, deadline);
        workloadIndex.recordDeadlineValidation(deadline, currentAssignment);
        return currentAssignment
                ? DeadlinePoll.consumed(new DeadlineTarget(job, task, deadline))
                : DeadlinePoll.consumed(null);
    }

    long nextDeadlineAtMillis() {
        return workloadIndex.nextDeadlineAtMillis();
    }

    void rescheduleCurrentDeadline(DeadlineTarget target, long nextDueAtMillis) {
        if (target != null && matches(target.task(), target.deadline())) {
            workloadIndex.reschedule(target.deadline(), nextDueAtMillis);
        }
    }

    SchedulerWorkloadIndex.Snapshot workloadSnapshot() {
        return workloadIndex.snapshot();
    }

    List<AssignmentTarget> currentAssignmentsForWorker(String workerId) {
        List<AssignmentTarget> current = new ArrayList<>();
        for (SchedulerWorkloadIndex.IndexedAssignment indexed
                : workloadIndex.assignmentsForWorker(workerId)) {
            EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(indexed.jobId());
            TaskUnit<?> task = job == null ? null : job.getTasks().get(indexed.taskId());
            AssignmentIdentity identity = task == null
                    ? null
                    : task.getAssignmentIdentity().orElse(null);
            boolean matches = identity != null
                    && task.getStatus() == TaskUnit.TaskStatus.ASSIGNED
                    && identity.attemptNumber() == indexed.attemptNumber()
                    && identity.assignmentId().equals(indexed.assignmentId())
                    && identity.workerId().equals(indexed.workerId());
            if (matches) {
                current.add(new AssignmentTarget(job, task, identity));
            } else {
                workloadIndex.cancelAssignment(indexed);
            }
        }
        return List.copyOf(current);
    }

    String requesterTokenHash(String jobId) {
        return requesterTokenHashes.get(jobId);
    }

    String requesterIdentityKey(String jobId) {
        return requesterIdentityKeys.get(jobId);
    }

    String requestHash(String jobId) {
        return requestHashes.get(jobId);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private DeadlineTarget pollDueDeadline(SchedulerWorkloadIndex.DeadlineKind kind,
                                           long nowMillis) {
        while (true) {
            SchedulerWorkloadIndex.ScheduledDeadline deadline = workloadIndex.pollDue(
                    kind,
                    nowMillis
            );
            if (deadline == null) {
                return null;
            }
            EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(deadline.jobId());
            TaskUnit<?> task = job == null ? null : job.getTasks().get(deadline.taskId());
            boolean currentAssignment = task != null && matches(task, deadline);
            workloadIndex.recordDeadlineValidation(deadline, currentAssignment);
            if (currentAssignment) {
                return new DeadlineTarget(job, task, deadline);
            }
        }
    }

    private static boolean matches(TaskUnit<?> task,
                                   SchedulerWorkloadIndex.ScheduledDeadline deadline) {
        return task.getStatus() == TaskUnit.TaskStatus.ASSIGNED
                && task.getAssignmentIdentity()
                .filter(identity -> identity.attemptNumber() == deadline.attemptNumber())
                .filter(identity -> identity.assignmentId().equals(deadline.assignmentId()))
                .filter(identity -> identity.workerId().equals(deadline.workerId()))
                .isPresent();
    }

    private static boolean isCurrentAssignment(TaskUnit<?> task, AssignmentIdentity expected) {
        return expected != null
                && task.getStatus() == TaskUnit.TaskStatus.ASSIGNED
                && task.getAssignmentIdentity().filter(expected::equals).isPresent();
    }

    record DeadlineTarget(
            EmbarrassinglyParallelJob<?, ?> job,
            TaskUnit<?> task,
            SchedulerWorkloadIndex.ScheduledDeadline deadline
    ) {
    }

    record DeadlinePoll(boolean consumed, DeadlineTarget target) {
        static DeadlinePoll none() {
            return new DeadlinePoll(false, null);
        }

        static DeadlinePoll consumed(DeadlineTarget target) {
            return new DeadlinePoll(true, target);
        }
    }

    record AssignmentTarget(
            EmbarrassinglyParallelJob<?, ?> job,
            TaskUnit<?> task,
            AssignmentIdentity identity
    ) {
    }
}
