package server.scheduler;

import server.job.AssignmentIdentity;
import server.job.TaskUnit;
import server.scheduler.transition.SchedulerEvent;
import server.scheduler.transition.TaskState;
import server.scheduler.transition.TaskStateMachine;
import server.scheduler.transition.TransitionDecision;

/**
 * Adapts the mutable compatibility projection to the pure transition reducer.
 * Infrastructure services use the returned decision as their domain guard;
 * SQLite remains authoritative for conditional durable outcomes.
 */
final class TaskTransitionDecisions {
    private final TaskStateMachine stateMachine;

    TaskTransitionDecisions(TaskStateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    TransitionDecision jobSubmitted(long occurredAtMillis) {
        return stateMachine.decide(TaskState.absent(), new SchedulerEvent.JobSubmitted(occurredAtMillis));
    }

    TransitionDecision assignmentRequested(TaskUnit<?> task,
                                            AssignmentIdentity identity,
                                            String leaseOwnerId,
                                            long occurredAtMillis) {
        return stateMachine.decide(
                snapshot(task),
                new SchedulerEvent.AssignmentRequested(assignment(identity, leaseOwnerId), occurredAtMillis)
        );
    }

    TransitionDecision resultReceived(TaskUnit<?> task,
                                      String workerId,
                                      int attemptNumber,
                                      String assignmentId,
                                      long occurredAtMillis) {
        return stateMachine.decide(
                snapshot(task),
                new SchedulerEvent.TaskResultReceived(
                        identity(attemptNumber, assignmentId, workerId),
                        occurredAtMillis
                )
        );
    }

    TransitionDecision executionFailed(TaskUnit<?> task,
                                       String workerId,
                                       int attemptNumber,
                                       String assignmentId,
                                       int maxRetries,
                                       boolean retryable,
                                       long occurredAtMillis) {
        return stateMachine.decide(
                snapshot(task),
                new SchedulerEvent.TaskExecutionFailed(
                        identity(attemptNumber, assignmentId, workerId),
                        maxRetries,
                        retryable,
                        occurredAtMillis
                )
        );
    }

    TransitionDecision leaseExpired(TaskUnit<?> task, int maxRetries, long occurredAtMillis) {
        return stateMachine.decide(
                snapshot(task),
                new SchedulerEvent.LeaseExpired(currentIdentity(task), maxRetries, occurredAtMillis)
        );
    }

    TransitionDecision taskTimedOut(TaskUnit<?> task,
                                   int maxRetries,
                                   long timeoutMillis,
                                   long occurredAtMillis) {
        return stateMachine.decide(
                snapshot(task),
                new SchedulerEvent.TaskTimedOut(
                        currentIdentity(task),
                        maxRetries,
                        timeoutMillis,
                        occurredAtMillis
                )
        );
    }

    TransitionDecision workerUnavailable(TaskUnit<?> task, int maxRetries, long occurredAtMillis) {
        return stateMachine.decide(
                snapshot(task),
                new SchedulerEvent.WorkerUnavailable(currentIdentity(task), maxRetries, occurredAtMillis)
        );
    }

    TransitionDecision coordinatorRecovered(TaskUnit<?> task, long occurredAtMillis) {
        return stateMachine.decide(
                snapshot(task),
                new SchedulerEvent.CoordinatorRecovered(occurredAtMillis)
        );
    }

    private static TaskState snapshot(TaskUnit<?> task) {
        int retryCount = task.getRetryCount();
        int attemptNumber = task.getAttemptNumber();
        return switch (task.getStatus()) {
            case PENDING -> new TaskState(
                    TaskState.JobStatus.RUNNING,
                    TaskState.TaskStatus.PENDING,
                    retryCount,
                    attemptNumber,
                    null,
                    0L,
                    Math.max(0L, task.getPendingSinceMillis()),
                    null
            );
            case ASSIGNED -> assignedSnapshot(task, retryCount, attemptNumber);
            case COMPLETED -> terminalSnapshot(TaskState.TaskStatus.COMPLETED, retryCount, attemptNumber);
            case FAILED -> terminalSnapshot(TaskState.TaskStatus.FAILED, retryCount, attemptNumber);
        };
    }

    private static TaskState assignedSnapshot(TaskUnit<?> task, int retryCount, int attemptNumber) {
        AssignmentIdentity identity = task.getAssignmentIdentity().orElse(null);
        String leaseOwnerId = task.getLeaseOwnerId();
        if (identity == null || leaseOwnerId == null || leaseOwnerId.isBlank()) {
            return TaskState.incompleteAssigned(retryCount, attemptNumber, task.getStartTime());
        }
        return TaskState.assigned(
                retryCount,
                assignment(identity, leaseOwnerId),
                task.getStartTime()
        );
    }

    private static TaskState terminalSnapshot(TaskState.TaskStatus status,
                                              int retryCount,
                                              int attemptNumber) {
        return new TaskState(
                TaskState.JobStatus.RUNNING,
                status,
                retryCount,
                attemptNumber,
                null,
                0L,
                -1L,
                null
        );
    }

    private static TaskState.Assignment currentAssignment(TaskUnit<?> task) {
        AssignmentIdentity current = task.getAssignmentIdentity()
                .orElseThrow(() -> new IllegalStateException(
                        "Assigned task is missing assignment identity: " + task.getTaskId()
                ));
        return assignment(current, task.getLeaseOwnerId());
    }

    private static TaskState.AssignmentIdentity currentIdentity(TaskUnit<?> task) {
        return currentAssignment(task).identity();
    }

    private static TaskState.Assignment assignment(AssignmentIdentity identity, String leaseOwnerId) {
        return new TaskState.Assignment(
                identity(identity.attemptNumber(), identity.assignmentId(), identity.workerId()),
                leaseOwnerId,
                identity.leaseExpiresAtEpochMillis()
        );
    }

    private static TaskState.AssignmentIdentity identity(int attemptNumber,
                                                         String assignmentId,
                                                         String workerId) {
        return new TaskState.AssignmentIdentity(attemptNumber, assignmentId, workerId);
    }
}
