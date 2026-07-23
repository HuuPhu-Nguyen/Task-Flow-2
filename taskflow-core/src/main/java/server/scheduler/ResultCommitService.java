package server.scheduler;

import protocol.TaskResultMessage;
import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.model.MessageEnvelope;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TransitionDecision;
import transport.DeliveryDisposition;
import transport.TransientDeliveryException;

import java.util.Map;

/** Owns failed-attempt classification and generation-fenced T2 commitment. */
final class ResultCommitService {
    private final SchedulerState state;
    private final SchedulerPersistence persistence;
    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final SchedulerMetrics metrics;
    private final TaskTransitionDecisions transitions;
    private final AttemptService attempts;
    private final LeaseService leases;
    private final JobCompletionService jobCompletions;
    private final SchedulerEventLog events;

    ResultCommitService(SchedulerState state,
                        SchedulerPersistence persistence,
                        SchedulerConfig config,
                        TaskFlowClock clock,
                        SchedulerMetrics metrics,
                        TaskTransitionDecisions transitions,
                        AttemptService attempts,
                        LeaseService leases,
                        JobCompletionService jobCompletions,
                        SchedulerEventLog events) {
        this.state = state;
        this.persistence = persistence;
        this.config = config;
        this.clock = clock;
        this.metrics = metrics;
        this.transitions = transitions;
        this.attempts = attempts;
        this.leases = leases;
        this.jobCompletions = jobCompletions;
        this.events = events;
    }

    DeliveryDisposition handleTaskResult(MessageEnvelope envelope, TaskResultMessage result) {
        EmbarrassinglyParallelJob<?, ?> job = state.activeJob(result.getJobId());
        if (job == null) {
            return DeliveryDisposition.ACK_DUPLICATE_OR_STALE;
        }
        TaskUnit<?> task = job.getTasks().get(result.getTaskId());
        if (task == null) {
            return DeliveryDisposition.ACK_DUPLICATE_OR_STALE;
        }

        LeaseService.LeaseExpiryResult leaseExpiry = leases.expireTaskLeaseIfNeeded(
                job,
                task,
                clock.nowEpochMillis()
        );
        if (leaseExpiry.handled()) {
            if (leaseExpiry.jobFailureReason() != null) {
                jobCompletions.failJob(job, leaseExpiry.jobFailureReason());
            }
            return DeliveryDisposition.ACK_DUPLICATE_OR_STALE;
        }

        if (!result.isSuccessful()) {
            return handleFailedResult(envelope, result, job, task);
        }
        return handleSuccessfulResult(envelope, result, job, task);
    }

    private DeliveryDisposition handleFailedResult(MessageEnvelope envelope,
                                                   TaskResultMessage result,
                                                   EmbarrassinglyParallelJob<?, ?> job,
                                                   TaskUnit<?> task) {
        long failedAt = clock.nowEpochMillis();
        TransitionDecision decision = transitions.executionFailed(
                task,
                envelope.fromNodeId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                config.maxTaskRetries(),
                failedAt
        );
        if (!decision.accepted()) {
            if (decision.disposition() == TransitionDecision.Disposition.STALE) {
                metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT);
                events.info("task_result_stale_rejected", events.assignmentTraceFields(
                        job.getJobId(),
                        task.getTaskId(),
                        result.getAttemptNumber(),
                        result.getAssignmentId(),
                        envelope.fromNodeId(),
                        "commit_outcome", JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT,
                        "successful", false
                ));
                return DeliveryDisposition.ACK_DUPLICATE_OR_STALE;
            }
            events.info("task_failure_result_ignored", events.assignmentTraceFields(
                    job.getJobId(),
                    task.getTaskId(),
                    result.getAttemptNumber(),
                    result.getAssignmentId(),
                    envelope.fromNodeId(),
                    "disposition", decision.disposition(),
                    "reason", decision.detail()
            ));
            return DeliveryDisposition.ACK_DUPLICATE_OR_STALE;
        }

        AttemptService.FailureResult failure = attempts.closeFailedAttempt(
                task,
                envelope.fromNodeId(),
                decision,
                config.maxTaskRetries(),
                result.getErrorMessage(),
                failedAt
        );
        if (!failure.handled()) {
            if (failure.durableOutcome() == JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE) {
                jobCompletions.failJob(
                        job,
                        persistence.failureReason(persistence.taskFailureOperation(failure.outcome()))
                );
                return DeliveryDisposition.RETRY_TRANSIENT;
            } else if (failure.durableOutcome() == JobStateStore.DurableTransitionOutcome.UNKNOWN_ENTITY) {
                jobCompletions.failJob(
                        job,
                        persistence.failureReason(persistence.taskFailureOperation(failure.outcome()))
                );
                return DeliveryDisposition.QUARANTINE_POISON;
            } else {
                metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT);
                events.info("task_result_stale_rejected", events.assignmentTraceFields(
                        job.getJobId(),
                        task.getTaskId(),
                        result.getAttemptNumber(),
                        result.getAssignmentId(),
                        envelope.fromNodeId(),
                        "commit_outcome", failure.durableOutcome(),
                        "successful", false
                ));
            }
            return DeliveryDisposition.ACK_DUPLICATE_OR_STALE;
        }
        events.error("task_failed", events.fields(
                "job_id", job.getJobId(),
                "task_id", task.getTaskId(),
                "peer_id", envelope.fromNodeId(),
                "retry_count", task.getRetryCount(),
                "terminal_failure", failure.outcome() == TaskUnit.FailureOutcome.TERMINAL_FAILURE,
                "error", result.getErrorMessage()
        ));

        if (failure.outcome() == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
            jobCompletions.failJob(job, "Task " + task.getTaskId() + " reached max retries.");
        }
        return DeliveryDisposition.ACK_SUCCESS;
    }

    private DeliveryDisposition handleSuccessfulResult(MessageEnvelope envelope,
                                                       TaskResultMessage result,
                                                       EmbarrassinglyParallelJob<?, ?> job,
                                                       TaskUnit<?> task) {
        long completedAt = clock.nowEpochMillis();
        long startedAt = task.getStartTime();
        long durationMs = startedAt > 0L ? Math.max(0L, completedAt - startedAt) : 0L;
        TransitionDecision decision = transitions.resultReceived(
                task,
                envelope.fromNodeId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                completedAt
        );
        EmbarrassinglyParallelJob.PreparedTaskResult preparedResult = decision.accepted()
                ? job.prepareTaskResult(result.getResultPayload())
                : null;

        JobStateStore.ResultCommitOutcome commitOutcome;
        JobStateStore store = persistence.store();
        if (store == null) {
            commitOutcome = decision.accepted()
                    ? JobStateStore.ResultCommitOutcome.COMMITTED
                    : JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT;
        } else {
            commitOutcome = store.commitTaskResult(
                    task.getTaskId(),
                    result.getAttemptNumber(),
                    result.getAssignmentId(),
                    envelope.fromNodeId(),
                    completedAt,
                    durationMs,
                    result.getResultPayload()
            );
        }
        if (commitOutcome == null) {
            commitOutcome = JobStateStore.ResultCommitOutcome.STORAGE_FAILURE;
        }

        if (commitOutcome != JobStateStore.ResultCommitOutcome.COMMITTED) {
            metrics.recordResultCommitOutcome(commitOutcome);
            logResultCommitDisposition(job, task, envelope.fromNodeId(), result, commitOutcome);
            if (commitOutcome == JobStateStore.ResultCommitOutcome.STORAGE_FAILURE) {
                throw new TransientDeliveryException(
                        "task_result_storage_failure",
                        persistence.failureReason("commitTaskResult"),
                        null
                );
            }
            return DeliveryDisposition.ACK_DUPLICATE_OR_STALE;
        }

        if (!decision.accepted() || preparedResult == null) {
            throw new IllegalStateException(
                    "Authoritative result committed for an assignment rejected by scheduler memory."
            );
        }
        EmbarrassinglyParallelJob.TaskCompletion completion = job.applyCommittedResult(
                result.getTaskId(),
                envelope.fromNodeId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                completedAt,
                preparedResult
        );
        if (!completion.accepted()) {
            throw new IllegalStateException(
                    "Authoritative result commit could not be applied to scheduler memory."
            );
        }

        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.COMMITTED);
        attempts.onAttemptSuccess(envelope.fromNodeId(), completion.durationMs());
        events.info("task_result_committed", events.assignmentTraceFields(
                job.getJobId(),
                task.getTaskId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                envelope.fromNodeId(),
                "commit_outcome", commitOutcome,
                "duration_ms", Math.max(0L, completion.durationMs())
        ));

        if (job.isJobComplete()) {
            jobCompletions.completeJob(job, true, null);
        } else if (job.hasTerminalFailure()) {
            jobCompletions.failJob(job, "Job has one or more terminal failed tasks.");
        }
        return DeliveryDisposition.ACK_SUCCESS;
    }

    private void logResultCommitDisposition(EmbarrassinglyParallelJob<?, ?> job,
                                            TaskUnit<?> task,
                                            String reportingPeerId,
                                            TaskResultMessage result,
                                            JobStateStore.ResultCommitOutcome outcome) {
        Map<String, Object> eventFields = events.assignmentTraceFields(
                job.getJobId(),
                task.getTaskId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                reportingPeerId,
                "commit_outcome", outcome
        );
        switch (outcome) {
            case STALE_ASSIGNMENT -> events.info("task_result_stale_rejected", eventFields);
            case DUPLICATE_ALREADY_COMPLETED -> events.info("task_result_duplicate_ignored", eventFields);
            case UNKNOWN_TASK, STORAGE_FAILURE -> events.error("task_result_not_committed", eventFields);
            case COMMITTED -> throw new IllegalArgumentException(
                    "Committed results must use the task_result_committed event."
            );
        }
    }
}
