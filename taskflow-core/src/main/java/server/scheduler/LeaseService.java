package server.scheduler;

import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TransitionDecision;

import java.util.LinkedHashMap;
import java.util.Map;

/** Owns timeout, lease-expiry, and participant-unavailability transition rules. */
final class LeaseService {
    private final SchedulerState state;
    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final TaskTransitionDecisions transitions;
    private final AttemptService attempts;
    private final JobCompletionService jobCompletions;
    private final SchedulerPersistence persistence;
    private final SchedulerEventLog events;

    LeaseService(SchedulerState state,
                 SchedulerConfig config,
                 TaskFlowClock clock,
                 TaskTransitionDecisions transitions,
                 AttemptService attempts,
                 JobCompletionService jobCompletions,
                 SchedulerPersistence persistence,
                 SchedulerEventLog events) {
        this.state = state;
        this.config = config;
        this.clock = clock;
        this.transitions = transitions;
        this.attempts = attempts;
        this.jobCompletions = jobCompletions;
        this.persistence = persistence;
        this.events = events;
    }

    void checkTimeouts() {
        long now = clock.nowEpochMillis();
        Map<String, String> jobsToFail = new LinkedHashMap<>();

        for (EmbarrassinglyParallelJob<?, ?> job : state.activeJobs()) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.ASSIGNED
                        || task.getAssignmentIdentity().isEmpty()) {
                    continue;
                }

                TransitionDecision decision = transitions.taskTimedOut(
                        task,
                        config.maxTaskRetries(),
                        config.taskTimeoutMillis(),
                        now
                );
                if (!decision.accepted()) {
                    continue;
                }

                String assignedPeerId = task.getAssignedPeerId();
                AttemptService.FailureResult failure = attempts.closeFailedAttempt(
                        task,
                        assignedPeerId,
                        decision,
                        config.maxTaskRetries(),
                        "task_timeout",
                        now
                );
                if (!failure.handled()) {
                    recordDurableFailure(job, failure, jobsToFail);
                    continue;
                }
                events.error("task_timeout", events.fields(
                        "job_id", job.getJobId(),
                        "task_id", task.getTaskId(),
                        "assigned_peer_id", assignedPeerId,
                        "retry_count", task.getRetryCount(),
                        "terminal_failure", failure.outcome() == TaskUnit.FailureOutcome.TERMINAL_FAILURE
                ));

                recordTerminalOrPersistenceFailure(job, task, failure, jobsToFail,
                        " exceeded max retries after timeout.");
            }
        }

        failJobs(jobsToFail);
    }

    void checkLeaseExpirations() {
        long now = clock.nowEpochMillis();
        Map<String, String> jobsToFail = new LinkedHashMap<>();

        for (EmbarrassinglyParallelJob<?, ?> job : state.activeJobs()) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                LeaseExpiryResult result = expireTaskLeaseIfNeeded(job, task, now);
                if (result.jobFailureReason() != null) {
                    jobsToFail.putIfAbsent(job.getJobId(), result.jobFailureReason());
                }
            }
        }

        failJobs(jobsToFail);
    }

    LeaseExpiryResult expireTaskLeaseIfNeeded(EmbarrassinglyParallelJob<?, ?> job,
                                               TaskUnit<?> task,
                                               long now) {
        if (task.getStatus() != TaskUnit.TaskStatus.ASSIGNED
                || task.getAssignmentIdentity().isEmpty()) {
            return LeaseExpiryResult.notHandled();
        }

        TransitionDecision decision = transitions.leaseExpired(task, config.maxTaskRetries(), now);
        if (!decision.accepted()) {
            return LeaseExpiryResult.notHandled();
        }

        String assignedPeerId = task.getAssignedPeerId();
        long leaseExpiresAt = task.getLeaseExpiresAtMillis();
        AttemptService.FailureResult failure = attempts.closeFailedAttempt(
                task,
                assignedPeerId,
                decision,
                config.maxTaskRetries(),
                "lease_expired",
                now
        );
        if (!failure.handled()) {
            return LeaseExpiryResult.handled(
                    failure.storageFailed()
                            ? persistence.failureReason(persistence.taskFailureOperation(failure.outcome()))
                            : null
            );
        }
        events.error("task_lease_expired", events.fields(
                "job_id", job.getJobId(),
                "task_id", task.getTaskId(),
                "assigned_peer_id", assignedPeerId,
                "lease_expires_at", leaseExpiresAt,
                "retry_count", task.getRetryCount(),
                "terminal_failure", failure.outcome() == TaskUnit.FailureOutcome.TERMINAL_FAILURE
        ));

        if (failure.outcome() == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
            return LeaseExpiryResult.handled(
                    "Task " + task.getTaskId() + " lease expired before completion."
            );
        }
        return LeaseExpiryResult.handled(null);
    }

    void handlePeerUnavailable(String peerId, String reason) {
        if (peerId == null || peerId.isBlank()) {
            return;
        }

        String normalizedReason = reason == null || reason.isBlank() ? "peer_unavailable" : reason;
        Map<String, String> jobsToFail = new LinkedHashMap<>();
        int retryScheduled = 0;
        int terminalFailures = 0;

        for (EmbarrassinglyParallelJob<?, ?> job : state.activeJobs()) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.ASSIGNED
                        || !peerId.equals(task.getAssignedPeerId())
                        || task.getAssignmentIdentity().isEmpty()) {
                    continue;
                }

                long occurredAt = clock.nowEpochMillis();
                TransitionDecision decision = transitions.workerUnavailable(
                        task,
                        config.maxTaskRetries(),
                        occurredAt
                );
                AttemptService.FailureResult failure = attempts.closeFailedAttempt(
                        task,
                        peerId,
                        decision,
                        config.maxTaskRetries(),
                        normalizedReason,
                        occurredAt
                );
                if (!failure.handled()) {
                    recordDurableFailure(job, failure, jobsToFail);
                    continue;
                }
                if (failure.outcome() == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
                    retryScheduled++;
                } else {
                    terminalFailures++;
                }

                events.error("task_peer_unavailable", events.fields(
                        "job_id", job.getJobId(),
                        "task_id", task.getTaskId(),
                        "peer_id", peerId,
                        "retry_count", task.getRetryCount(),
                        "terminal_failure", failure.outcome() == TaskUnit.FailureOutcome.TERMINAL_FAILURE,
                        "reason", normalizedReason
                ));
                recordTerminalOrPersistenceFailure(job, task, failure, jobsToFail,
                        " exceeded max retries after peer became unavailable.");
            }
        }

        if (retryScheduled > 0 || terminalFailures > 0) {
            events.info("peer_unavailable_tasks_released", events.fields(
                    "peer_id", peerId,
                    "retry_scheduled", retryScheduled,
                    "terminal_failures", terminalFailures,
                    "reason", normalizedReason
            ));
        }
        failJobs(jobsToFail);
    }

    private void recordTerminalOrPersistenceFailure(EmbarrassinglyParallelJob<?, ?> job,
                                                    TaskUnit<?> task,
                                                    AttemptService.FailureResult failure,
                                                    Map<String, String> jobsToFail,
                                                    String terminalSuffix) {
        if (failure.outcome() == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
            jobsToFail.putIfAbsent(
                    job.getJobId(),
                    "Task " + task.getTaskId() + terminalSuffix
            );
        }
    }

    private void recordDurableFailure(EmbarrassinglyParallelJob<?, ?> job,
                                      AttemptService.FailureResult failure,
                                      Map<String, String> jobsToFail) {
        if (failure.storageFailed()) {
            jobsToFail.putIfAbsent(
                    job.getJobId(),
                    persistence.failureReason(persistence.taskFailureOperation(failure.outcome()))
            );
        }
    }

    private void failJobs(Map<String, String> jobsToFail) {
        for (Map.Entry<String, String> entry : jobsToFail.entrySet()) {
            EmbarrassinglyParallelJob<?, ?> job = state.activeJob(entry.getKey());
            if (job != null) {
                jobCompletions.failJob(job, entry.getValue());
            }
        }
    }

    record LeaseExpiryResult(boolean handled, String jobFailureReason) {
        static LeaseExpiryResult notHandled() {
            return new LeaseExpiryResult(false, null);
        }

        static LeaseExpiryResult handled(String jobFailureReason) {
            return new LeaseExpiryResult(true, jobFailureReason);
        }
    }
}
