package server.scheduler;

import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Scheduler-owned, non-authoritative workload indexes.
 *
 * <p>Every mutating caller crosses its durable transition boundary before
 * changing this projection. The index therefore accelerates discovery but
 * never authorizes a lifecycle transition.</p>
 */
final class SchedulerWorkloadIndex {
    enum DeadlineKind {
        TASK_TIMEOUT,
        LEASE_EXPIRY
    }

    record ScheduledDeadline(
            DeadlineKind kind,
            long dueAtMillis,
            String jobId,
            String taskId,
            int attemptNumber,
            String assignmentId,
            String workerId
    ) implements Comparable<ScheduledDeadline> {
        ScheduledDeadline {
            Objects.requireNonNull(kind, "kind");
            if (dueAtMillis < 0L) {
                throw new IllegalArgumentException("dueAtMillis must not be negative");
            }
            jobId = requireText(jobId, "jobId");
            taskId = requireText(taskId, "taskId");
            if (attemptNumber <= 0) {
                throw new IllegalArgumentException("attemptNumber must be positive");
            }
            assignmentId = requireText(assignmentId, "assignmentId");
            workerId = requireText(workerId, "workerId");
        }

        AssignmentKey assignmentKey() {
            return new AssignmentKey(jobId, taskId, attemptNumber, assignmentId, workerId);
        }

        ScheduledDeadline rescheduledAt(long nextDueAtMillis) {
            return new ScheduledDeadline(
                    kind,
                    nextDueAtMillis,
                    jobId,
                    taskId,
                    attemptNumber,
                    assignmentId,
                    workerId
            );
        }

        @Override
        public int compareTo(ScheduledDeadline other) {
            int compared = Long.compare(dueAtMillis, other.dueAtMillis);
            if (compared != 0) {
                return compared;
            }
            compared = jobId.compareTo(other.jobId);
            if (compared != 0) {
                return compared;
            }
            compared = taskId.compareTo(other.taskId);
            if (compared != 0) {
                return compared;
            }
            compared = Integer.compare(attemptNumber, other.attemptNumber);
            if (compared != 0) {
                return compared;
            }
            compared = assignmentId.compareTo(other.assignmentId);
            if (compared != 0) {
                return compared;
            }
            compared = workerId.compareTo(other.workerId);
            if (compared != 0) {
                return compared;
            }
            return kind.compareTo(other.kind);
        }

        private static String requireText(String value, String field) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(field + " must not be blank");
            }
            return value;
        }
    }

    record Snapshot(
            long pendingTasks,
            int runnableJobs,
            int capacityWaitingJobs,
            int liveAssignments,
            int deadlineEntries,
            long deadlineHeadChecks,
            long deadlineEntriesPopped,
            long deadlineEntriesValidated,
            long staleDeadlineEntriesRejected,
            long deadlineReschedules
    ) {
    }

    record IndexedAssignment(
            String jobId,
            String taskId,
            int attemptNumber,
            String assignmentId,
            String workerId
    ) {
    }

    private final Map<String, PendingTaskIds> pendingTaskIdsByJob = new HashMap<>();
    private final LinkedHashSet<String> runnableJobIds = new LinkedHashSet<>();
    private final Map<String, Long> capacityWaitingJobSignalGenerations = new LinkedHashMap<>();
    private final TreeSet<ScheduledDeadline> timeoutDeadlines = new TreeSet<>();
    private final TreeSet<ScheduledDeadline> leaseDeadlines = new TreeSet<>();
    private final Map<AssignmentKey, AssignmentDeadlines> deadlinesByAssignment = new HashMap<>();
    private final Map<TaskKey, AssignmentKey> assignmentKeyByTask = new HashMap<>();
    private final Map<String, Set<AssignmentKey>> assignmentKeysByJob = new HashMap<>();
    private final Map<String, Set<AssignmentKey>> assignmentKeysByWorker = new HashMap<>();

    private long pendingTaskCount;
    private long deadlineHeadChecks;
    private long deadlineEntriesPopped;
    private long deadlineEntriesValidated;
    private long staleDeadlineEntriesRejected;
    private long deadlineReschedules;

    void indexJob(EmbarrassinglyParallelJob<?, ?> job, long taskTimeoutMillis) {
        Objects.requireNonNull(job, "job");
        removeJob(job.getJobId());

        List<? extends TaskUnit<?>> tasks = job.getTasks().values().stream()
                .sorted((left, right) -> {
                    int retryOrder = Integer.compare(right.getRetryCount(), left.getRetryCount());
                    return retryOrder != 0
                            ? retryOrder
                            : left.getTaskId().compareTo(right.getTaskId());
                })
                .toList();
        for (TaskUnit<?> task : tasks) {
            if (task.getStatus() == TaskUnit.TaskStatus.PENDING) {
                addPendingTask(
                        task.getJobId(),
                        task.getTaskId(),
                        task.getRetryCount() > 0
                );
                continue;
            }
            if (task.getStatus() == TaskUnit.TaskStatus.ASSIGNED) {
                task.getAssignmentIdentity().ifPresent(identity -> scheduleAssignment(
                        task.getJobId(),
                        task.getTaskId(),
                        task.getStartTime(),
                        taskTimeoutMillis,
                        identity
                ));
            }
        }
    }

    void removeJob(String jobId) {
        PendingTaskIds pending = pendingTaskIdsByJob.remove(jobId);
        if (pending != null) {
            pendingTaskCount = Math.max(0L, pendingTaskCount - pending.size());
        }
        runnableJobIds.remove(jobId);
        capacityWaitingJobSignalGenerations.remove(jobId);

        Set<AssignmentKey> assignmentKeys = assignmentKeysByJob.get(jobId);
        if (assignmentKeys == null || assignmentKeys.isEmpty()) {
            return;
        }
        for (AssignmentKey key : List.copyOf(assignmentKeys)) {
            cancelAssignment(key);
        }
    }

    void addPendingTask(String jobId, String taskId, boolean retryPriority) {
        PendingTaskIds pending = pendingTaskIdsByJob.computeIfAbsent(
                jobId,
                ignored -> new PendingTaskIds()
        );
        boolean newlyIndexed = pending.add(taskId, retryPriority);
        if (newlyIndexed) {
            pendingTaskCount++;
        }
        if (!capacityWaitingJobSignalGenerations.containsKey(jobId)) {
            runnableJobIds.addLast(jobId);
        }
    }

    boolean removePendingTask(String jobId, String taskId) {
        PendingTaskIds pending = pendingTaskIdsByJob.get(jobId);
        if (pending == null || !pending.remove(taskId)) {
            return false;
        }
        pendingTaskCount--;
        if (pending.isEmpty()) {
            pendingTaskIdsByJob.remove(jobId);
            runnableJobIds.remove(jobId);
            capacityWaitingJobSignalGenerations.remove(jobId);
        }
        return true;
    }

    String pollPendingTask(String jobId) {
        PendingTaskIds pending = pendingTaskIdsByJob.get(jobId);
        if (pending == null || pending.isEmpty()) {
            runnableJobIds.remove(jobId);
            return null;
        }
        String taskId = pending.pollFirst();
        pendingTaskCount--;
        if (pending.isEmpty()) {
            pendingTaskIdsByJob.remove(jobId);
            runnableJobIds.remove(jobId);
            capacityWaitingJobSignalGenerations.remove(jobId);
        }
        return taskId;
    }

    int pendingTaskCount(String jobId) {
        PendingTaskIds pending = pendingTaskIdsByJob.get(jobId);
        return pending == null ? 0 : pending.size();
    }

    String pollRunnableJob() {
        return runnableJobIds.isEmpty() ? null : runnableJobIds.removeFirst();
    }

    void requeueRunnableJob(String jobId) {
        if (pendingTaskCount(jobId) > 0
                && !capacityWaitingJobSignalGenerations.containsKey(jobId)) {
            runnableJobIds.addLast(jobId);
        }
    }

    int runnableJobCount() {
        return runnableJobIds.size();
    }

    void waitForCapacity(String jobId, long signalGeneration) {
        if (signalGeneration < 0L) {
            throw new IllegalArgumentException("signalGeneration must not be negative");
        }
        runnableJobIds.remove(jobId);
        if (pendingTaskCount(jobId) > 0) {
            capacityWaitingJobSignalGenerations.put(jobId, signalGeneration);
        }
    }

    String pollCapacityWaitingJob(long eligibleBeforeSignalGeneration) {
        if (eligibleBeforeSignalGeneration < 0L) {
            throw new IllegalArgumentException(
                    "eligibleBeforeSignalGeneration must not be negative"
            );
        }
        if (capacityWaitingJobSignalGenerations.isEmpty()) {
            return null;
        }
        Map.Entry<String, Long> first =
                capacityWaitingJobSignalGenerations.entrySet().iterator().next();
        if (first.getValue() >= eligibleBeforeSignalGeneration) {
            return null;
        }
        capacityWaitingJobSignalGenerations.remove(first.getKey());
        return first.getKey();
    }

    boolean hasCapacityWaitingJobEligibleBefore(long signalGeneration) {
        if (signalGeneration < 0L) {
            throw new IllegalArgumentException("signalGeneration must not be negative");
        }
        if (capacityWaitingJobSignalGenerations.isEmpty()) {
            return false;
        }
        return capacityWaitingJobSignalGenerations.entrySet()
                .iterator()
                .next()
                .getValue() < signalGeneration;
    }

    int capacityWaitingJobCount() {
        return capacityWaitingJobSignalGenerations.size();
    }

    void scheduleAssignment(String jobId,
                            String taskId,
                            long startedAtMillis,
                            long taskTimeoutMillis,
                            AssignmentIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (startedAtMillis < 0L) {
            throw new IllegalArgumentException("startedAtMillis must not be negative.");
        }
        if (taskTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("taskTimeoutMillis must be positive.");
        }
        if (!taskId.equals(identity.taskId())) {
            throw new IllegalArgumentException("Assignment identity belongs to a different task.");
        }
        AssignmentKey key = new AssignmentKey(
                jobId,
                taskId,
                identity.attemptNumber(),
                identity.assignmentId(),
                identity.workerId()
        );
        TaskKey taskKey = new TaskKey(jobId, taskId);
        AssignmentKey previousAssignment = assignmentKeyByTask.get(taskKey);
        if (previousAssignment != null && !previousAssignment.equals(key)) {
            cancelAssignment(previousAssignment);
        }
        cancelAssignment(key);

        ScheduledDeadline timeout = new ScheduledDeadline(
                DeadlineKind.TASK_TIMEOUT,
                timeoutDueAt(startedAtMillis, taskTimeoutMillis),
                jobId,
                taskId,
                identity.attemptNumber(),
                identity.assignmentId(),
                identity.workerId()
        );
        ScheduledDeadline lease = new ScheduledDeadline(
                DeadlineKind.LEASE_EXPIRY,
                identity.leaseExpiresAtEpochMillis(),
                jobId,
                taskId,
                identity.attemptNumber(),
                identity.assignmentId(),
                identity.workerId()
        );
        AssignmentDeadlines pair = new AssignmentDeadlines(timeout, lease);
        deadlinesByAssignment.put(key, pair);
        assignmentKeyByTask.put(taskKey, key);
        assignmentKeysByJob.computeIfAbsent(jobId, ignored -> new HashSet<>()).add(key);
        assignmentKeysByWorker.computeIfAbsent(identity.workerId(), ignored -> new HashSet<>()).add(key);
        timeoutDeadlines.add(timeout);
        leaseDeadlines.add(lease);
    }

    void cancelAssignment(String jobId, String taskId, AssignmentIdentity identity) {
        if (identity == null) {
            return;
        }
        cancelAssignment(new AssignmentKey(
                jobId,
                taskId,
                identity.attemptNumber(),
                identity.assignmentId(),
                identity.workerId()
        ));
    }

    ScheduledDeadline pollDue(DeadlineKind kind, long nowMillis) {
        deadlineHeadChecks++;
        TreeSet<ScheduledDeadline> deadlines = deadlines(kind);
        if (deadlines.isEmpty() || deadlines.getFirst().dueAtMillis() > nowMillis) {
            return null;
        }
        return removeDue(deadlines.getFirst());
    }

    ScheduledDeadline pollNextDue(long nowMillis) {
        deadlineHeadChecks++;
        ScheduledDeadline next = nextDeadline();
        if (next == null || next.dueAtMillis() > nowMillis) {
            return null;
        }
        return removeDue(next);
    }

    long nextDeadlineAtMillis() {
        ScheduledDeadline next = nextDeadline();
        return next == null ? Long.MAX_VALUE : next.dueAtMillis();
    }

    private ScheduledDeadline removeDue(ScheduledDeadline due) {
        deadlines(due.kind()).remove(due);
        deadlineEntriesPopped++;
        AssignmentDeadlines pair = deadlinesByAssignment.get(due.assignmentKey());
        if (pair != null) {
            pair.clear(due);
            if (pair.empty()) {
                removeAssignmentKey(due.assignmentKey());
            }
        }
        return due;
    }

    private ScheduledDeadline nextDeadline() {
        ScheduledDeadline timeout = timeoutDeadlines.isEmpty() ? null : timeoutDeadlines.getFirst();
        ScheduledDeadline lease = leaseDeadlines.isEmpty() ? null : leaseDeadlines.getFirst();
        if (timeout == null) {
            return lease;
        }
        if (lease == null) {
            return timeout;
        }
        return timeout.compareTo(lease) <= 0 ? timeout : lease;
    }

    void recordDeadlineValidation(ScheduledDeadline deadline, boolean currentAssignment) {
        deadlineEntriesValidated++;
        if (currentAssignment) {
            return;
        }
        staleDeadlineEntriesRejected++;
        cancelAssignment(deadline.assignmentKey());
    }

    void reschedule(ScheduledDeadline deadline, long nextDueAtMillis) {
        ScheduledDeadline replacement = deadline.rescheduledAt(nextDueAtMillis);
        AssignmentKey key = replacement.assignmentKey();
        AssignmentDeadlines pair = deadlinesByAssignment.computeIfAbsent(
                key,
                ignored -> new AssignmentDeadlines(null, null)
        );
        ScheduledDeadline previous = pair.replace(replacement);
        if (previous != null) {
            deadlines(previous.kind()).remove(previous);
        }
        deadlines(replacement.kind()).add(replacement);
        assignmentKeyByTask.put(new TaskKey(replacement.jobId(), replacement.taskId()), key);
        assignmentKeysByJob.computeIfAbsent(replacement.jobId(), ignored -> new HashSet<>()).add(key);
        assignmentKeysByWorker.computeIfAbsent(replacement.workerId(), ignored -> new HashSet<>()).add(key);
        deadlineReschedules++;
    }

    List<IndexedAssignment> assignmentsForWorker(String workerId) {
        Set<AssignmentKey> keys = assignmentKeysByWorker.get(workerId);
        if (keys == null || keys.isEmpty()) {
            return List.of();
        }
        return keys.stream()
                .sorted((left, right) -> {
                    int jobOrder = left.jobId().compareTo(right.jobId());
                    return jobOrder != 0
                            ? jobOrder
                            : left.taskId().compareTo(right.taskId());
                })
                .map(key -> {
                    AssignmentDeadlines deadlines = deadlinesByAssignment.get(key);
                    return deadlines == null || deadlines.empty()
                            ? null
                            : new IndexedAssignment(
                                    key.jobId(),
                                    key.taskId(),
                                    key.attemptNumber(),
                                    key.assignmentId(),
                                    key.workerId()
                            );
                })
                .filter(Objects::nonNull)
                .toList();
    }

    void cancelAssignment(IndexedAssignment assignment) {
        if (assignment == null) {
            return;
        }
        cancelAssignment(new AssignmentKey(
                assignment.jobId(),
                assignment.taskId(),
                assignment.attemptNumber(),
                assignment.assignmentId(),
                assignment.workerId()
        ));
    }

    Snapshot snapshot() {
        return new Snapshot(
                pendingTaskCount,
                runnableJobIds.size(),
                capacityWaitingJobSignalGenerations.size(),
                deadlinesByAssignment.size(),
                timeoutDeadlines.size() + leaseDeadlines.size(),
                deadlineHeadChecks,
                deadlineEntriesPopped,
                deadlineEntriesValidated,
                staleDeadlineEntriesRejected,
                deadlineReschedules
        );
    }

    private void cancelAssignment(AssignmentKey key) {
        AssignmentDeadlines pair = deadlinesByAssignment.remove(key);
        if (pair == null) {
            return;
        }
        if (pair.timeout != null) {
            timeoutDeadlines.remove(pair.timeout);
        }
        if (pair.lease != null) {
            leaseDeadlines.remove(pair.lease);
        }
        removeAssignmentIndexKeys(key);
    }

    private void removeAssignmentKey(AssignmentKey key) {
        deadlinesByAssignment.remove(key);
        removeAssignmentIndexKeys(key);
    }

    private void removeAssignmentIndexKeys(AssignmentKey key) {
        assignmentKeyByTask.remove(new TaskKey(key.jobId(), key.taskId()), key);
        Set<AssignmentKey> jobKeys = assignmentKeysByJob.get(key.jobId());
        if (jobKeys != null) {
            jobKeys.remove(key);
            if (jobKeys.isEmpty()) {
                assignmentKeysByJob.remove(key.jobId());
            }
        }
        Set<AssignmentKey> workerKeys = assignmentKeysByWorker.get(key.workerId());
        if (workerKeys != null) {
            workerKeys.remove(key);
            if (workerKeys.isEmpty()) {
                assignmentKeysByWorker.remove(key.workerId());
            }
        }
    }

    private TreeSet<ScheduledDeadline> deadlines(DeadlineKind kind) {
        return kind == DeadlineKind.TASK_TIMEOUT ? timeoutDeadlines : leaseDeadlines;
    }

    private static long timeoutDueAt(long startedAtMillis, long taskTimeoutMillis) {
        if (startedAtMillis >= Long.MAX_VALUE - taskTimeoutMillis) {
            return Long.MAX_VALUE;
        }
        long elapsedBoundary = startedAtMillis + taskTimeoutMillis;
        return elapsedBoundary == Long.MAX_VALUE ? Long.MAX_VALUE : elapsedBoundary + 1L;
    }

    private record AssignmentKey(
            String jobId,
            String taskId,
            int attemptNumber,
            String assignmentId,
            String workerId
    ) {
    }

    private record TaskKey(String jobId, String taskId) {
    }

    private static final class AssignmentDeadlines {
        private ScheduledDeadline timeout;
        private ScheduledDeadline lease;

        private AssignmentDeadlines(ScheduledDeadline timeout, ScheduledDeadline lease) {
            this.timeout = timeout;
            this.lease = lease;
        }

        private void clear(ScheduledDeadline deadline) {
            if (deadline.kind() == DeadlineKind.TASK_TIMEOUT && deadline.equals(timeout)) {
                timeout = null;
            } else if (deadline.kind() == DeadlineKind.LEASE_EXPIRY && deadline.equals(lease)) {
                lease = null;
            }
        }

        private ScheduledDeadline replace(ScheduledDeadline deadline) {
            if (deadline.kind() == DeadlineKind.TASK_TIMEOUT) {
                ScheduledDeadline previous = timeout;
                timeout = deadline;
                return previous;
            }
            ScheduledDeadline previous = lease;
            lease = deadline;
            return previous;
        }

        private boolean empty() {
            return timeout == null && lease == null;
        }
    }

    private static final class PendingTaskIds {
        private final LinkedHashSet<String> retryTaskIds = new LinkedHashSet<>();
        private final LinkedHashSet<String> ordinaryTaskIds = new LinkedHashSet<>();

        private boolean add(String taskId, boolean retryPriority) {
            if (retryPriority) {
                if (retryTaskIds.contains(taskId)) {
                    return false;
                }
                boolean promoted = ordinaryTaskIds.remove(taskId);
                retryTaskIds.addLast(taskId);
                return !promoted;
            }
            if (retryTaskIds.contains(taskId) || ordinaryTaskIds.contains(taskId)) {
                return false;
            }
            ordinaryTaskIds.addLast(taskId);
            return true;
        }

        private boolean remove(String taskId) {
            return retryTaskIds.remove(taskId) || ordinaryTaskIds.remove(taskId);
        }

        private String pollFirst() {
            return retryTaskIds.isEmpty()
                    ? ordinaryTaskIds.removeFirst()
                    : retryTaskIds.removeFirst();
        }

        private boolean isEmpty() {
            return retryTaskIds.isEmpty() && ordinaryTaskIds.isEmpty();
        }

        private int size() {
            return retryTaskIds.size() + ordinaryTaskIds.size();
        }
    }
}
