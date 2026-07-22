package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.JobSubmitMessage;
import protocol.RequesterTokens;
import server.db.JobStateStore;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.job.TaskUnit;
import server.runtime.AssignmentIdGenerator;
import server.runtime.SystemTaskFlowClock;
import server.runtime.TaskFlowClock;
import server.runtime.UuidAssignmentIdGenerator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class CoordinatorStartupRecovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorStartupRecovery.class);

    private CoordinatorStartupRecovery() {
    }

    static boolean reconcileAbandonedJobs(JobStateStore store) {
        return reconcileAbandonedJobs(store, SystemTaskFlowClock.INSTANCE);
    }

    static boolean reconcileAbandonedJobs(JobStateStore store, TaskFlowClock clock) {
        return reconcileAbandonedJobs(
                store,
                Objects.requireNonNull(clock, "clock").nowEpochMillis()
        );
    }

    static boolean reconcileAbandonedJobs(JobStateStore store, long completedAt) {
        int failedJobs = store.markRunningJobsFailedOnStartup(completedAt);
        if (failedJobs > 0) {
            LOGGER.warn("event=abandoned_jobs_marked_failed count={}", failedJobs);
            return true;
        }
        if (failedJobs < 0) {
            LOGGER.error("event=abandoned_job_reconciliation_failed");
            return false;
        }
        return true;
    }

    static RecoveryResult recoverPersistedJobs(JobStateStore store) {
        return recoverPersistedJobs(
                store,
                SystemTaskFlowClock.INSTANCE,
                UuidAssignmentIdGenerator.INSTANCE
        );
    }

    static RecoveryResult recoverPersistedJobs(JobStateStore store, long completedAt) {
        return recoverPersistedJobs(
                store,
                completedAt,
                new EpochMillisClock(completedAt),
                UuidAssignmentIdGenerator.INSTANCE
        );
    }

    static RecoveryResult recoverPersistedJobs(JobStateStore store,
                                               TaskFlowClock clock,
                                               AssignmentIdGenerator assignmentIdGenerator) {
        TaskFlowClock requiredClock = Objects.requireNonNull(clock, "clock");
        return recoverPersistedJobs(
                store,
                requiredClock.nowEpochMillis(),
                requiredClock,
                Objects.requireNonNull(assignmentIdGenerator, "assignmentIdGenerator")
        );
    }

    private static RecoveryResult recoverPersistedJobs(JobStateStore store,
                                                        long completedAt,
                                                        TaskFlowClock clock,
                                                        AssignmentIdGenerator assignmentIdGenerator) {
        List<JobStateStore.ResumableJobState> persistedJobs = store.loadRunningJobsForResume();
        if (persistedJobs.isEmpty()) {
            return new RecoveryResult(true, List.of(), Map.of(), Map.of(), 0);
        }

        List<EmbarrassinglyParallelJob<?, ?>> resumedJobs = new ArrayList<>();
        Map<String, String> requesterTokenHashes = new LinkedHashMap<>();
        Map<String, String> requesterIdentityKeys = new LinkedHashMap<>();
        int failedJobs = 0;
        boolean successful = true;

        for (JobStateStore.ResumableJobState persistedJob : persistedJobs) {
            try {
                EmbarrassinglyParallelJob<?, ?> restored = restoreJob(
                        store,
                        persistedJob,
                        completedAt,
                        clock,
                        assignmentIdGenerator
                );
                if (restored == null) {
                    if (!store.markRunningJobFailedOnStartup(persistedJob.jobId(), completedAt)) {
                        successful = false;
                    }
                    failedJobs++;
                    continue;
                }
                resumedJobs.add(restored);
                requesterTokenHashes.put(restored.getJobId(), persistedJob.requesterTokenHash());
                if (hasText(persistedJob.requesterIdentityKey())) {
                    requesterIdentityKeys.put(restored.getJobId(), persistedJob.requesterIdentityKey());
                }
                LOGGER.warn("event=running_job_resumed job_id={} task_count={}",
                        persistedJob.jobId(), persistedJob.tasks().size());
            } catch (Exception e) {
                LOGGER.error("event=running_job_resume_failed job_id={} error={}",
                        persistedJob.jobId(), e.getMessage(), e);
                if (!store.markRunningJobFailedOnStartup(persistedJob.jobId(), completedAt)) {
                    successful = false;
                }
                failedJobs++;
            }
        }

        if (failedJobs > 0) {
            LOGGER.warn("event=non_resumable_running_jobs_marked_failed count={}", failedJobs);
        }
        return new RecoveryResult(
                successful,
                List.copyOf(resumedJobs),
                Map.copyOf(requesterTokenHashes),
                Map.copyOf(requesterIdentityKeys),
                failedJobs
        );
    }

    private static EmbarrassinglyParallelJob<?, ?> restoreJob(JobStateStore store,
                                                              JobStateStore.ResumableJobState persistedJob,
                                                              long recoveredAt,
                                                              TaskFlowClock clock,
                                                              AssignmentIdGenerator assignmentIdGenerator) {
        if (!RequesterTokens.hasTokenHash(persistedJob.requesterTokenHash())) {
            LOGGER.warn("event=running_job_not_resumable job_id={} reason=missing_requester_token_hash",
                    persistedJob.jobId());
            return null;
        }
        if (persistedJob.tasks().isEmpty()) {
            LOGGER.warn("event=running_job_not_resumable job_id={} reason=no_tasks", persistedJob.jobId());
            return null;
        }
        if (persistedJob.tasks().stream().anyMatch(task -> task.payload() == null)) {
            LOGGER.warn("event=running_job_not_resumable job_id={} reason=missing_task_payload",
                    persistedJob.jobId());
            return null;
        }

        Map<String, Integer> lastAssignmentAttempts = lastAssignmentAttempts(
                persistedJob.jobId(),
                store.loadTaskAttempts(persistedJob.jobId())
        );

        JobSubmitMessage submit = new JobSubmitMessage(
                persistedJob.requesterId(),
                Instant.ofEpochMilli(recoveredAt).toString(),
                persistedJob.jobId(),
                persistedJob.taskType(),
                persistedJob.tasks().stream()
                        .map(JobStateStore.ResumableTaskState::payload)
                        .map(Object.class::cast)
                        .toList(),
                persistedJob.parameter()
        );
        EmbarrassinglyParallelJob<?, ?> job = JobFactory.create(submit, persistedJob.requesterId());
        job.initializeTasks(submit);
        job.configureTransitionPorts(clock, assignmentIdGenerator);
        if (job.getTasks().size() != persistedJob.tasks().size()) {
            LOGGER.warn("event=running_job_not_resumable job_id={} reason=task_count_mismatch expected={} actual={}",
                    persistedJob.jobId(), persistedJob.tasks().size(), job.getTasks().size());
            return null;
        }

        for (JobStateStore.ResumableTaskState taskState : persistedJob.tasks()) {
            TaskUnit.TaskStatus status = TaskUnit.TaskStatus.valueOf(taskState.status());
            if (status == TaskUnit.TaskStatus.COMPLETED && !taskState.resultPayloadPresent()) {
                LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=missing_result_payload",
                        persistedJob.jobId(), taskState.taskId());
                return null;
            }

            int auditedAttempt = lastAssignmentAttempts.getOrDefault(taskState.taskId(), 0);
            int lastAssignmentAttempt = Math.max(taskState.attemptNumber(), auditedAttempt);
            Optional<AssignmentIdentity> currentAssignment = restoredAssignmentIdentity(taskState);
            if (currentAssignment.isPresent() && auditedAttempt > taskState.attemptNumber()) {
                currentAssignment = Optional.empty();
            }
            TaskUnit.TaskStatus restoreStatus = status;
            boolean releaseExpiredLease = false;
            boolean releaseLegacyAssignment = false;
            if (status == TaskUnit.TaskStatus.ASSIGNED) {
                if (currentAssignment.isEmpty() || !hasCompleteLeaseMetadata(taskState)) {
                    restoreStatus = TaskUnit.TaskStatus.PENDING;
                    releaseLegacyAssignment = true;
                } else if (!hasUnexpiredLease(taskState, recoveredAt)) {
                    restoreStatus = TaskUnit.TaskStatus.PENDING;
                    releaseExpiredLease = true;
                }
            }

            if (!job.restoreTaskForResume(
                    taskState.taskId(),
                    restoreStatus,
                    taskState.resultPayload(),
                    taskState.retryCount(),
                    taskState.assignedPeerId(),
                    taskState.startedAt(),
                    taskState.leaseOwnerId(),
                    taskState.leaseExpiresAt(),
                    lastAssignmentAttempt,
                    restoreStatus == TaskUnit.TaskStatus.ASSIGNED
                            ? currentAssignment.orElseThrow().assignmentId()
                            : null
            )) {
                LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=task_restore_failed",
                        persistedJob.jobId(), taskState.taskId());
                return null;
            }
            if (releaseLegacyAssignment) {
                LOGGER.warn("event=legacy_task_assignment_released job_id={} task_id={} reason=incomplete_assignment_state",
                        persistedJob.jobId(), taskState.taskId());
                if (!store.resetTaskForResume(taskState.taskId(), lastAssignmentAttempt)) {
                    LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=legacy_assignment_release_failed",
                            persistedJob.jobId(), taskState.taskId());
                    return null;
                }
            } else if (releaseExpiredLease) {
                if (!store.releaseExpiredTaskLeaseForResume(
                        taskState.taskId(),
                        recoveredAt,
                        lastAssignmentAttempt
                )) {
                    LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=task_lease_release_failed",
                            persistedJob.jobId(), taskState.taskId());
                    return null;
                }
            } else if (status == TaskUnit.TaskStatus.PENDING
                    && !store.resetTaskForResume(taskState.taskId(), lastAssignmentAttempt)) {
                LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=task_reset_failed",
                        persistedJob.jobId(), taskState.taskId());
                return null;
            }
        }
        return job;
    }

    private static Map<String, Integer> lastAssignmentAttempts(
            String jobId,
            List<JobStateStore.TaskAttemptRecord> attempts) {
        if (attempts == null || attempts.isEmpty()) {
            return Map.of();
        }
        Map<String, Integer> latestByTask = new LinkedHashMap<>();
        for (JobStateStore.TaskAttemptRecord attempt : attempts) {
            if (attempt == null
                    || !jobId.equals(attempt.jobId())
                    || !hasText(attempt.taskId())
                    || attempt.attemptNumber() < 1) {
                continue;
            }
            latestByTask.merge(attempt.taskId(), attempt.attemptNumber(), Math::max);
        }
        return Map.copyOf(latestByTask);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static Optional<AssignmentIdentity> restoredAssignmentIdentity(
            JobStateStore.ResumableTaskState taskState) {
        if (taskState.attemptNumber() < 1
                || !hasText(taskState.assignmentId())
                || !hasText(taskState.assignedPeerId())) {
            return Optional.empty();
        }
        try {
            return Optional.of(new AssignmentIdentity(
                    taskState.taskId(),
                    taskState.attemptNumber(),
                    taskState.assignmentId(),
                    taskState.assignedPeerId(),
                    taskState.leaseExpiresAt()
            ));
        } catch (IllegalArgumentException ignored) {
            return Optional.empty();
        }
    }

    private static boolean hasUnexpiredLease(JobStateStore.ResumableTaskState taskState, long recoveredAt) {
        return hasCompleteLeaseMetadata(taskState) && taskState.leaseExpiresAt() > recoveredAt;
    }

    private static boolean hasCompleteLeaseMetadata(JobStateStore.ResumableTaskState taskState) {
        return hasText(taskState.leaseOwnerId()) && taskState.leaseExpiresAt() > 0L;
    }

    private record EpochMillisClock(long epochMillis) implements TaskFlowClock {
        @Override
        public Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public long nowEpochMillis() {
            return epochMillis;
        }
    }

    record RecoveryResult(boolean successful,
                          List<EmbarrassinglyParallelJob<?, ?>> resumedJobs,
                          Map<String, String> requesterTokenHashes,
                          Map<String, String> requesterIdentityKeys,
                          int failedJobs) {
    }
}
