package server.scheduler;

import server.db.JobStateStore;
import server.job.AssignmentIdentity;
import server.job.TaskUnit;
import server.job.EmbarrassinglyParallelJob;
import server.registry.AssignmentCapacityReservation;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.scheduler.transition.TaskState;
import server.scheduler.transition.TransitionDecision;

/** Executes one reducer-approved failed-attempt projection and persistence effect. */
final class AttemptService {
    private final SchedulerState state;
    private final PeerRegistry registry;
    private final SchedulerPersistence persistence;
    private final SchedulerMetrics metrics;

    AttemptService(SchedulerState state,
                   PeerRegistry registry,
                   SchedulerPersistence persistence,
                   SchedulerMetrics metrics) {
        this.state = state;
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
        return closeFailedAttempt(
                task,
                workerId,
                decision,
                maxRetries,
                true,
                failureReason,
                finishedAt
        );
    }

    FailureResult closeFailedAttempt(TaskUnit<?> task,
                                     String workerId,
                                     TransitionDecision decision,
                                     int maxRetries,
                                     boolean retryable,
                                     String failureReason,
                                     long finishedAt) {
        if (!decision.accepted()) {
            return new FailureResult(
                    TaskUnit.FailureOutcome.IGNORED,
                    JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED,
                    decision,
                    false
            );
        }

        TaskUnit.FailureOutcome expected = expectedOutcome(decision);
        AssignmentIdentity assignment = task.getAssignmentIdentity()
                .orElseThrow(() -> new IllegalStateException(
                        "Accepted failed-attempt transition is missing assignment identity for "
                                + task.getTaskId()
                ));
        EmbarrassinglyParallelJob<?, ?> job = state.activeJob(task.getJobId());
        if (job == null) {
            throw new IllegalStateException(
                    "Accepted failed-attempt transition is missing active job "
                            + task.getJobId()
            );
        }
        AssignmentCapacityReservation reservation =
                CapacityReservations.forAssignment(job, task, assignment);
        JobStateStore.DurableTransitionOutcome durableOutcome = persistTaskFailure(
                task,
                assignment,
                expected,
                decision.resultingState().retryCount(),
                failureReason,
                finishedAt
        );
        if (!durableOutcome.projectionAllowed()) {
            return new FailureResult(expected, durableOutcome, decision, false);
        }

        TaskUnit.FailureOutcome outcome = task.failAttemptBy(
                workerId,
                maxRetries,
                retryable
        );
        if (outcome == TaskUnit.FailureOutcome.IGNORED || outcome != expected) {
            throw new IllegalStateException(
                    "Mutable task projection disagreed with accepted transition decision for "
                            + task.getTaskId()
            );
        }
        state.indexClosedAssignment(task, assignment);

        if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
            metrics.recordRetry();
        }
        onAttemptFailure(
                reservation,
                outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE,
                outcome.name()
        );
        return new FailureResult(outcome, durableOutcome, decision, true);
    }

    void onAttemptSuccess(AssignmentCapacityReservation reservation, long durationMs) {
        registry.releaseTaskCapacity(reservation, "SUCCEEDED");
        PeerInfo peer = registry.get(reservation.workerId());
        if (peer != null) {
            peer.recordTaskSuccess(durationMs);
            registry.updateMetricsSnapshot(reservation.workerId());
        }
    }

    void recordLeaseExpiration() {
        metrics.recordLeaseExpiration();
    }

    private void onAttemptFailure(AssignmentCapacityReservation reservation,
                                  boolean terminalFailure,
                                  String releaseReason) {
        metrics.recordAttemptFailure(terminalFailure);
        registry.releaseTaskCapacity(reservation, releaseReason);
        PeerInfo peer = registry.get(reservation.workerId());
        if (peer != null) {
            peer.recordTaskFailure();
            registry.updateMetricsSnapshot(reservation.workerId());
        }
    }

    private JobStateStore.DurableTransitionOutcome persistTaskFailure(
            TaskUnit<?> task,
            AssignmentIdentity assignment,
            TaskUnit.FailureOutcome outcome,
            int retryCount,
            String failureReason,
            long finishedAt) {
        JobStateStore store = persistence.store();
        if (store == null) {
            return JobStateStore.DurableTransitionOutcome.COMMITTED;
        }
        String reason = failureReason == null || failureReason.isBlank() ? "task_failed" : failureReason;
        String operation = outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE
                ? "commitAssignedTaskFailure:terminal"
                : "commitAssignedTaskFailure:retry";
        JobStateStore.TaskAttemptOutcome durableAttemptOutcome =
                outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE
                        ? JobStateStore.TaskAttemptOutcome.TERMINAL_FAILURE
                        : JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED;
        return persistence.record(
                operation,
                task.getJobId(),
                task.getTaskId(),
                store.commitAssignedTaskFailure(
                        task.getTaskId(),
                        assignment.attemptNumber(),
                        assignment.assignmentId(),
                        assignment.workerId(),
                        retryCount,
                        durableAttemptOutcome,
                        reason,
                        finishedAt
                )
        );
    }

    private static TaskUnit.FailureOutcome expectedOutcome(TransitionDecision decision) {
        return decision.resultingState().taskStatus() == TaskState.TaskStatus.FAILED
                ? TaskUnit.FailureOutcome.TERMINAL_FAILURE
                : TaskUnit.FailureOutcome.RETRY_SCHEDULED;
    }

    record FailureResult(TaskUnit.FailureOutcome outcome,
                         JobStateStore.DurableTransitionOutcome durableOutcome,
                         TransitionDecision decision,
                         boolean projected) {
        boolean handled() {
            return projected;
        }

        boolean persisted() {
            return durableOutcome.projectionAllowed();
        }

        boolean storageFailed() {
            return durableOutcome == JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE
                    || durableOutcome == JobStateStore.DurableTransitionOutcome.UNKNOWN_ENTITY;
        }
    }
}
