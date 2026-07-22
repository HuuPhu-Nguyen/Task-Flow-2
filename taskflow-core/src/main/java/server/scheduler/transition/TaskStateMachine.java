package server.scheduler.transition;

import java.util.List;
import java.util.Objects;

import static server.scheduler.transition.TaskState.JobStatus;
import static server.scheduler.transition.TaskState.TaskStatus;
import static server.scheduler.transition.TransitionDecision.Disposition;
import static server.scheduler.transition.TransitionDecision.DurableTransition;
import static server.scheduler.transition.TransitionDecision.EventIntent;
import static server.scheduler.transition.TransitionDecision.MetricIntent;
import static server.scheduler.transition.TransitionDecision.OutboxIntent;

/**
 * Pure transition table for task lifecycle decisions. It performs no storage,
 * publication, projection mutation, clock access, random generation, or
 * scheduler orchestration.
 */
public final class TaskStateMachine {

    public TransitionDecision decide(TaskState state, SchedulerEvent event) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(event, "event");

        if (event.equals(state.lastAcceptedEvent())) {
            return replay(state, event);
        }

        return switch (event) {
            case SchedulerEvent.JobSubmitted submitted -> decideSubmission(state, submitted);
            case SchedulerEvent.AssignmentRequested requested -> decideAssignment(state, requested);
            case SchedulerEvent.TaskResultReceived result -> decideResult(state, result);
            case SchedulerEvent.TaskExecutionFailed failure -> decideExecutionFailure(state, failure);
            case SchedulerEvent.LeaseExpired expiry -> decideLeaseExpiry(state, expiry);
            case SchedulerEvent.TaskTimedOut timeout -> decideTimeout(state, timeout);
            case SchedulerEvent.WorkerUnavailable unavailable -> decideWorkerUnavailable(state, unavailable);
            case SchedulerEvent.CoordinatorRecovered recovered -> decideRecovery(state, recovered);
        };
    }

    private TransitionDecision decideSubmission(TaskState state,
                                                SchedulerEvent.JobSubmitted event) {
        if (state.jobStatus() != JobStatus.NOT_CREATED) {
            return unchanged(
                    Disposition.DUPLICATE,
                    state,
                    "The job identifier is already present; submission replay does not repeat J0/T0."
            );
        }

        TaskState result = new TaskState(
                JobStatus.RUNNING,
                TaskStatus.PENDING,
                0,
                0,
                null,
                0L,
                event.occurredAtMillis(),
                event
        );
        return accepted(
                DurableTransition.J0_T0_ACCEPT_JOB_AND_CREATE_TASKS,
                OutboxIntent.NONE,
                result,
                List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                List.of(EventIntent.JOB_STARTED),
                "Create the RUNNING job and its complete PENDING task set atomically."
        );
    }

    private TransitionDecision decideAssignment(TaskState state,
                                                SchedulerEvent.AssignmentRequested event) {
        if (!jobIsRunning(state)) {
            return invalidForNonRunningJob(state);
        }

        TaskState.Assignment requested = event.assignment();
        if (state.taskStatus() == TaskStatus.ASSIGNED) {
            if (requested.equals(state.latestAssignment())) {
                return unchanged(
                        Disposition.DUPLICATE,
                        state,
                        "The exact assignment generation is already active."
                );
            }
            if (requested.attemptNumber() <= state.lastAttemptNumber()) {
                return unchanged(
                        Disposition.STALE,
                        state,
                        "The requested assignment generation is not newer than the active generation."
                );
            }
            return unchanged(
                    Disposition.INVALID,
                    state,
                    "An active assignment must be released before another generation is created."
            );
        }
        if (state.taskStatus() != TaskStatus.PENDING) {
            return unchanged(
                    Disposition.INVALID,
                    state,
                    "Only a PENDING task may cross T1."
            );
        }
        if (state.lastAttemptNumber() == Integer.MAX_VALUE) {
            return unchanged(
                    Disposition.INVALID,
                    state,
                    "The assignment generation cannot advance without integer overflow."
            );
        }
        if (requested.attemptNumber() <= state.lastAttemptNumber()) {
            return unchanged(
                    Disposition.STALE,
                    state,
                    "The requested assignment generation has already been closed or superseded."
            );
        }
        if (requested.attemptNumber() != state.lastAttemptNumber() + 1) {
            return unchanged(
                    Disposition.INVALID,
                    state,
                    "T1 requires exactly the next monotonic assignment generation."
            );
        }
        if (requested.leaseExpiresAtMillis() <= event.occurredAtMillis()) {
            return unchanged(
                    Disposition.INVALID,
                    state,
                    "T1 requires a lease deadline after the assignment start time."
            );
        }

        TaskState result = new TaskState(
                state.jobStatus(),
                TaskStatus.ASSIGNED,
                state.retryCount(),
                requested.attemptNumber(),
                requested,
                event.occurredAtMillis(),
                -1L,
                event
        );
        return accepted(
                DurableTransition.T1_CREATE_ASSIGNMENT,
                OutboxIntent.TASK_ASSIGN,
                result,
                List.of(
                        MetricIntent.ASSIGNMENT_GENERATION_INCREMENT,
                        MetricIntent.DISPATCH_LATENCY_SAMPLE
                ),
                List.of(EventIntent.TASK_ASSIGNMENT_CREATED),
                "Persist one fenced assignment generation and its logical TASK_ASSIGN intent."
        );
    }

    private TransitionDecision decideResult(TaskState state,
                                            SchedulerEvent.TaskResultReceived event) {
        if (state.taskStatus() == TaskStatus.NOT_CREATED) {
            return unchanged(
                    Disposition.INVALID,
                    state,
                    List.of(MetricIntent.TASK_RESULT_UNKNOWN_INCREMENT),
                    List.of(EventIntent.TASK_RESULT_NOT_COMMITTED),
                    "A result cannot target an unknown task."
            );
        }

        boolean exactLatest = event.assignment().equals(latestIdentity(state));
        if (state.taskStatus() == TaskStatus.COMPLETED && exactLatest) {
            return duplicateResult(state);
        }
        if (state.jobStatus() != JobStatus.RUNNING) {
            return staleResult(state, "A terminal job cannot accept another task result.");
        }
        if (state.taskStatus() != TaskStatus.ASSIGNED || !exactLatest) {
            return staleResult(
                    state,
                    "The result does not match the exact currently assigned generation."
            );
        }

        TaskState result = new TaskState(
                state.jobStatus(),
                TaskStatus.COMPLETED,
                state.retryCount(),
                state.lastAttemptNumber(),
                state.latestAssignment(),
                0L,
                -1L,
                event
        );
        return accepted(
                DurableTransition.T2_COMMIT_SUCCESSFUL_RESULT,
                OutboxIntent.NONE,
                result,
                List.of(MetricIntent.TASK_RESULT_COMMITTED_INCREMENT),
                List.of(EventIntent.TASK_RESULT_COMMITTED),
                "Commit T2 only for the exact assignment identity."
        );
    }

    private TransitionDecision decideExecutionFailure(
            TaskState state,
            SchedulerEvent.TaskExecutionFailed event) {
        if (!jobIsRunning(state)) {
            return invalidForNonRunningJob(state);
        }
        if (state.taskStatus() != TaskStatus.ASSIGNED) {
            return staleOrIgnoredClosedAssignment(
                    state,
                    "The failed execution no longer owns an active assignment."
            );
        }
        if (!event.assignment().equals(latestIdentity(state))) {
            return unchanged(
                    Disposition.STALE,
                    state,
                    "The failed execution does not match the active assignment generation."
            );
        }
        return closeFailedAttempt(
                state,
                event,
                event.maxRetries(),
                EventIntent.TASK_FAILED
        );
    }

    private TransitionDecision decideLeaseExpiry(TaskState state,
                                                 SchedulerEvent.LeaseExpired event) {
        if (!jobIsRunning(state)) {
            return invalidForNonRunningJob(state);
        }
        if (state.taskStatus() != TaskStatus.ASSIGNED) {
            return staleOrIgnoredClosedAssignment(
                    state,
                    "The lease belongs to a generation that is no longer active."
            );
        }
        if (!event.assignment().equals(latestIdentity(state))) {
            return unchanged(
                    Disposition.STALE,
                    state,
                    "The lease-expiry event does not match the active assignment generation."
            );
        }
        if (state.latestAssignment().leaseExpiresAtMillis() <= 0L
                || event.occurredAtMillis() < state.latestAssignment().leaseExpiresAtMillis()) {
            return unchanged(
                    Disposition.IGNORED,
                    state,
                    "The active lease has not expired."
            );
        }
        return closeFailedAttempt(
                state,
                event,
                event.maxRetries(),
                EventIntent.TASK_LEASE_EXPIRED
        );
    }

    private TransitionDecision decideTimeout(TaskState state,
                                             SchedulerEvent.TaskTimedOut event) {
        if (!jobIsRunning(state)) {
            return invalidForNonRunningJob(state);
        }
        if (state.taskStatus() != TaskStatus.ASSIGNED) {
            return staleOrIgnoredClosedAssignment(
                    state,
                    "The timeout belongs to a generation that is no longer active."
            );
        }
        if (!event.assignment().equals(latestIdentity(state))) {
            return unchanged(
                    Disposition.STALE,
                    state,
                    "The timeout event does not match the active assignment generation."
            );
        }
        if (!timeoutElapsed(state.startedAtMillis(), event.occurredAtMillis(), event.timeoutMillis())) {
            return unchanged(
                    Disposition.IGNORED,
                    state,
                    "The configured task timeout has not elapsed."
            );
        }
        return closeFailedAttempt(
                state,
                event,
                event.maxRetries(),
                EventIntent.TASK_TIMEOUT
        );
    }

    private TransitionDecision decideWorkerUnavailable(
            TaskState state,
            SchedulerEvent.WorkerUnavailable event) {
        if (!jobIsRunning(state)) {
            return invalidForNonRunningJob(state);
        }
        if (state.taskStatus() != TaskStatus.ASSIGNED) {
            return staleOrIgnoredClosedAssignment(
                    state,
                    "The unavailable participant's assignment is no longer active."
            );
        }
        if (!event.assignment().equals(latestIdentity(state))) {
            return unchanged(
                    Disposition.STALE,
                    state,
                    "The unavailable participant event does not match the active assignment generation."
            );
        }
        return closeFailedAttempt(
                state,
                event,
                event.maxRetries(),
                EventIntent.TASK_PEER_UNAVAILABLE
        );
    }

    private TransitionDecision decideRecovery(TaskState state,
                                              SchedulerEvent.CoordinatorRecovered event) {
        if (state.jobStatus() == JobStatus.NOT_CREATED) {
            return unchanged(
                    Disposition.INVALID,
                    state,
                    "Recovery cannot hydrate a job that has no persisted state."
            );
        }
        if (state.jobStatus() != JobStatus.RUNNING) {
            return unchanged(
                    Disposition.IGNORED,
                    state,
                    "Only persisted RUNNING jobs enter active recovery."
            );
        }

        if (state.taskStatus() == TaskStatus.PENDING) {
            TaskState result = copy(
                    state,
                    TaskStatus.PENDING,
                    state.retryCount(),
                    state.latestAssignment(),
                    0L,
                    event.occurredAtMillis(),
                    event
            );
            return accepted(
                    DurableTransition.R1_NORMALIZE_PENDING,
                    OutboxIntent.NONE,
                    result,
                    List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                    List.of(EventIntent.RUNNING_JOB_RESUMED),
                    "Normalize the pending row, then hydrate its projection."
            );
        }

        if (state.taskStatus() == TaskStatus.ASSIGNED) {
            TaskState.Assignment assignment = state.latestAssignment();
            boolean completeAndUnexpired = assignment != null
                    && assignment.leaseExpiresAtMillis() > event.occurredAtMillis();
            if (completeAndUnexpired) {
                TaskState result = copy(
                        state,
                        TaskStatus.ASSIGNED,
                        state.retryCount(),
                        assignment,
                        state.startedAtMillis(),
                        -1L,
                        event
                );
                return accepted(
                        DurableTransition.NONE,
                        OutboxIntent.NONE,
                        result,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(EventIntent.RUNNING_JOB_RESUMED),
                        "Hydrate the exact unexpired assignment without creating a generation."
                );
            }

            TaskState result = copy(
                    state,
                    TaskStatus.PENDING,
                    state.retryCount(),
                    assignment,
                    0L,
                    event.occurredAtMillis(),
                    event
            );
            List<EventIntent> events = assignment == null
                    ? List.of(
                            EventIntent.LEGACY_TASK_ASSIGNMENT_RELEASED,
                            EventIntent.RUNNING_JOB_RESUMED
                    )
                    : List.of(EventIntent.RUNNING_JOB_RESUMED);
            return accepted(
                    DurableTransition.T3_RELEASE_RECOVERED_ASSIGNMENT,
                    OutboxIntent.NONE,
                    result,
                    List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                    events,
                    "Release an expired or incomplete recovered assignment without incrementing retry count."
            );
        }

        TaskState result = copy(
                state,
                state.taskStatus(),
                state.retryCount(),
                state.latestAssignment(),
                0L,
                -1L,
                event
        );
        return accepted(
                DurableTransition.NONE,
                OutboxIntent.NONE,
                result,
                List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                List.of(EventIntent.RUNNING_JOB_RESUMED),
                "Hydrate the persisted terminal task without reopening it."
        );
    }

    private TransitionDecision closeFailedAttempt(TaskState state,
                                                  SchedulerEvent event,
                                                  int maxRetries,
                                                  EventIntent eventIntent) {
        if (state.retryCount() == Integer.MAX_VALUE) {
            return unchanged(
                    Disposition.INVALID,
                    state,
                    "The retry counter cannot be incremented without overflow."
            );
        }

        int nextRetryCount = state.retryCount() + 1;
        boolean terminal = nextRetryCount >= maxRetries;
        TaskStatus resultingStatus = terminal ? TaskStatus.FAILED : TaskStatus.PENDING;
        long pendingSince = terminal ? -1L : event.occurredAtMillis();
        TaskState result = copy(
                state,
                resultingStatus,
                nextRetryCount,
                state.latestAssignment(),
                0L,
                pendingSince,
                event
        );

        List<MetricIntent> metrics = terminal
                ? List.of(
                        MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                        MetricIntent.TERMINAL_FAILURE_INCREMENT
                )
                : List.of(
                        MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                        MetricIntent.RETRY_INCREMENT
                );
        return accepted(
                terminal
                        ? DurableTransition.T4_TERMINALIZE_ASSIGNED_TASK
                        : DurableTransition.T3_RELEASE_FOR_RETRY,
                OutboxIntent.NONE,
                result,
                metrics,
                List.of(eventIntent),
                terminal
                        ? "Close the current attempt and terminalize the task at retry exhaustion."
                        : "Close the current attempt and release the task for the next generation."
        );
    }

    private TransitionDecision replay(TaskState state, SchedulerEvent event) {
        if (event instanceof SchedulerEvent.TaskResultReceived) {
            return duplicateResult(state);
        }
        return unchanged(
                Disposition.DUPLICATE,
                state,
                "The exact event already produced this projection."
        );
    }

    private TransitionDecision duplicateResult(TaskState state) {
        return unchanged(
                Disposition.DUPLICATE,
                state,
                List.of(MetricIntent.TASK_RESULT_DUPLICATE_INCREMENT),
                List.of(EventIntent.TASK_RESULT_DUPLICATE_IGNORED),
                "The exact assignment result is already committed."
        );
    }

    private TransitionDecision staleResult(TaskState state, String detail) {
        return unchanged(
                Disposition.STALE,
                state,
                List.of(MetricIntent.TASK_RESULT_STALE_INCREMENT),
                List.of(EventIntent.TASK_RESULT_STALE_REJECTED),
                detail
        );
    }

    private TransitionDecision staleOrIgnoredClosedAssignment(TaskState state,
                                                              String detail) {
        Disposition disposition = state.latestAssignment() == null
                ? Disposition.IGNORED
                : Disposition.STALE;
        return unchanged(disposition, state, detail);
    }

    private TransitionDecision invalidForNonRunningJob(TaskState state) {
        return unchanged(
                Disposition.INVALID,
                state,
                "Only a RUNNING job may change task execution state."
        );
    }

    private boolean jobIsRunning(TaskState state) {
        return state.jobStatus() == JobStatus.RUNNING;
    }

    private TaskState.AssignmentIdentity latestIdentity(TaskState state) {
        return state.latestAssignment() == null ? null : state.latestAssignment().identity();
    }

    private boolean timeoutElapsed(long startedAtMillis,
                                   long occurredAtMillis,
                                   long timeoutMillis) {
        if (startedAtMillis <= 0L || occurredAtMillis <= startedAtMillis) {
            return false;
        }
        return occurredAtMillis - startedAtMillis > timeoutMillis;
    }

    private TaskState copy(TaskState state,
                           TaskStatus taskStatus,
                           int retryCount,
                           TaskState.Assignment assignment,
                           long startedAtMillis,
                           long pendingSinceMillis,
                           SchedulerEvent event) {
        return new TaskState(
                state.jobStatus(),
                taskStatus,
                retryCount,
                state.lastAttemptNumber(),
                assignment,
                startedAtMillis,
                pendingSinceMillis,
                event
        );
    }

    private TransitionDecision accepted(DurableTransition durableTransition,
                                        OutboxIntent outboxIntent,
                                        TaskState resultingState,
                                        List<MetricIntent> metrics,
                                        List<EventIntent> events,
                                        String detail) {
        return new TransitionDecision(
                Disposition.ACCEPTED,
                durableTransition,
                outboxIntent,
                resultingState,
                metrics,
                events,
                detail
        );
    }

    private TransitionDecision unchanged(Disposition disposition,
                                         TaskState state,
                                         String detail) {
        return unchanged(disposition, state, List.of(), List.of(), detail);
    }

    private TransitionDecision unchanged(Disposition disposition,
                                         TaskState state,
                                         List<MetricIntent> metrics,
                                         List<EventIntent> events,
                                         String detail) {
        return new TransitionDecision(
                disposition,
                DurableTransition.NONE,
                OutboxIntent.NONE,
                state,
                metrics,
                events,
                detail
        );
    }
}
