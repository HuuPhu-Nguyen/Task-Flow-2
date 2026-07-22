package server.scheduler;

import server.db.JobStateStore;
import server.job.TaskUnit;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.scheduler.transition.TaskState;
import server.scheduler.transition.TransitionDecision;

/** Executes one reducer-approved failed-attempt projection and persistence effect. */
final class AttemptService {
    private final PeerRegistry registry;
    private final SchedulerPersistence persistence;
    private final SchedulerMetrics metrics;

    AttemptService(PeerRegistry registry,
                   SchedulerPersistence persistence,
                   SchedulerMetrics metrics) {
        this.registry = registry;
        this.persistence = persistence;
        this.metrics = metrics;
    }

    FailureResult closeFailedAttempt(TaskUnit<?> task,
                                     String workerId,
                                     TransitionDecision decision,
                                     int maxRetries,
                                     String failureReason,
                                     long finishedAt) {
        if (!decision.accepted()) {
            return new FailureResult(TaskUnit.FailureOutcome.IGNORED, true, decision);
        }

        TaskUnit.FailureOutcome outcome = task.failAttemptBy(workerId, maxRetries);
        TaskUnit.FailureOutcome expected = expectedOutcome(decision);
        if (outcome == TaskUnit.FailureOutcome.IGNORED || outcome != expected) {
            throw new IllegalStateException(
                    "Mutable task projection disagreed with accepted transition decision for "
                            + task.getTaskId()
            );
        }

        if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
            metrics.recordRetry();
        }
        onAttemptFailure(workerId, outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE);
        boolean persisted = persistTaskFailure(task, outcome, failureReason, finishedAt);
        return new FailureResult(outcome, persisted, decision);
    }

    void onAttemptSuccess(String workerId, long durationMs) {
        PeerInfo peer = registry.get(workerId);
        if (peer == null) {
            return;
        }
        peer.recordTaskSuccess(durationMs);
        peer.decrementTasks();
        registry.updateMetricsSnapshot(workerId);
    }

    private void onAttemptFailure(String workerId, boolean terminalFailure) {
        metrics.recordAttemptFailure(terminalFailure);
        PeerInfo peer = registry.get(workerId);
        if (peer == null) {
            return;
        }
        peer.recordTaskFailure();
        peer.decrementTasks();
        registry.updateMetricsSnapshot(workerId);
    }

    private boolean persistTaskFailure(TaskUnit<?> task,
                                       TaskUnit.FailureOutcome outcome,
                                       String failureReason,
                                       long finishedAt) {
        JobStateStore store = persistence.store();
        if (store == null) {
            return true;
        }
        String reason = failureReason == null || failureReason.isBlank() ? "task_failed" : failureReason;
        if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
            return persistence.record(
                    "markTaskFailed",
                    task.getJobId(),
                    task.getTaskId(),
                    store.markTaskFailed(
                            task.getTaskId(),
                            JobStateStore.TaskAttemptOutcome.TERMINAL_FAILURE,
                            reason,
                            finishedAt
                    )
            );
        }
        if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
            return persistence.record(
                    "markTaskRetried",
                    task.getJobId(),
                    task.getTaskId(),
                    store.markTaskRetried(
                            task.getTaskId(),
                            task.getRetryCount(),
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            reason,
                            finishedAt
                    )
            );
        }
        return true;
    }

    private static TaskUnit.FailureOutcome expectedOutcome(TransitionDecision decision) {
        return decision.resultingState().taskStatus() == TaskState.TaskStatus.FAILED
                ? TaskUnit.FailureOutcome.TERMINAL_FAILURE
                : TaskUnit.FailureOutcome.RETRY_SCHEDULED;
    }

    record FailureResult(TaskUnit.FailureOutcome outcome,
                         boolean persisted,
                         TransitionDecision decision) {
        boolean handled() {
            return outcome != TaskUnit.FailureOutcome.IGNORED;
        }
    }
}
