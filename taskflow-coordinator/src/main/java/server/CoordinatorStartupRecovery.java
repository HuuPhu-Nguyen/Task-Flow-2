package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.JobSubmitMessage;
import protocol.RequesterTokens;
import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.job.TaskUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class CoordinatorStartupRecovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorStartupRecovery.class);

    private CoordinatorStartupRecovery() {
    }

    static boolean reconcileAbandonedJobs(JobStateStore store) {
        return reconcileAbandonedJobs(store, System.currentTimeMillis());
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
        return recoverPersistedJobs(store, System.currentTimeMillis());
    }

    static RecoveryResult recoverPersistedJobs(JobStateStore store, long completedAt) {
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
                EmbarrassinglyParallelJob<?, ?> restored = restoreJob(store, persistedJob);
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
                                                              JobStateStore.ResumableJobState persistedJob) {
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

        JobSubmitMessage submit = new JobSubmitMessage(
                persistedJob.requesterId(),
                Instant.now().toString(),
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
        if (job.getTasks().size() != persistedJob.tasks().size()) {
            LOGGER.warn("event=running_job_not_resumable job_id={} reason=task_count_mismatch expected={} actual={}",
                    persistedJob.jobId(), persistedJob.tasks().size(), job.getTasks().size());
            return null;
        }

        for (JobStateStore.ResumableTaskState taskState : persistedJob.tasks()) {
            TaskUnit.TaskStatus status = TaskUnit.TaskStatus.valueOf(taskState.status());
            if (status == TaskUnit.TaskStatus.COMPLETED && taskState.resultPayload() == null) {
                LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=missing_result_payload",
                        persistedJob.jobId(), taskState.taskId());
                return null;
            }
            if (!job.restoreTaskForResume(
                    taskState.taskId(),
                    status,
                    taskState.resultPayload(),
                    taskState.retryCount()
            )) {
                LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=task_restore_failed",
                        persistedJob.jobId(), taskState.taskId());
                return null;
            }
            if ((status == TaskUnit.TaskStatus.PENDING || status == TaskUnit.TaskStatus.ASSIGNED)
                    && !store.resetTaskForResume(taskState.taskId())) {
                LOGGER.warn("event=running_job_not_resumable job_id={} task_id={} reason=task_reset_failed",
                        persistedJob.jobId(), taskState.taskId());
                return null;
            }
        }
        return job;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    record RecoveryResult(boolean successful,
                          List<EmbarrassinglyParallelJob<?, ?>> resumedJobs,
                          Map<String, String> requesterTokenHashes,
                          Map<String, String> requesterIdentityKeys,
                          int failedJobs) {
    }
}
