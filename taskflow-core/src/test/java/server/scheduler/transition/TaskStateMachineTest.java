package server.scheduler.transition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static server.scheduler.transition.TaskState.JobStatus;
import static server.scheduler.transition.TaskState.TaskStatus;
import static server.scheduler.transition.TransitionDecision.Disposition;
import static server.scheduler.transition.TransitionDecision.DurableTransition;
import static server.scheduler.transition.TransitionDecision.EventIntent;
import static server.scheduler.transition.TransitionDecision.MetricIntent;
import static server.scheduler.transition.TransitionDecision.OutboxIntent;

class TaskStateMachineTest {
    private static final TaskState.Assignment ASSIGNMENT_1 =
            new TaskState.Assignment(
                    new TaskState.AssignmentIdentity(1, "assignment-1", "executor-1"),
                    "coordinator-lease-1",
                    5_000L
            );
    private static final TaskState.Assignment OTHER_ASSIGNMENT_1 =
            new TaskState.Assignment(
                    new TaskState.AssignmentIdentity(1, "assignment-other", "executor-2"),
                    "coordinator-lease-1",
                    5_000L
            );
    private static final TaskState.Assignment ASSIGNMENT_2 =
            new TaskState.Assignment(
                    new TaskState.AssignmentIdentity(2, "assignment-2", "executor-1"),
                    "coordinator-lease-1",
                    9_000L
            );
    private static final TaskState.AssignmentIdentity IDENTITY_1 = ASSIGNMENT_1.identity();
    private static final TaskState.AssignmentIdentity OTHER_IDENTITY_1 =
            OTHER_ASSIGNMENT_1.identity();

    private final TaskStateMachine machine = new TaskStateMachine();

    @ParameterizedTest(name = "{0}")
    @MethodSource("transitionTable")
    void implementsTransitionTable(String description,
                                   TaskState initialState,
                                   SchedulerEvent event,
                                   Disposition disposition,
                                   DurableTransition durableTransition,
                                   OutboxIntent outboxIntent,
                                   TaskStatus resultingStatus,
                                   int resultingRetryCount,
                                   List<MetricIntent> metrics,
                                   List<EventIntent> events) {
        TransitionDecision decision = machine.decide(initialState, event);

        assertEquals(disposition, decision.disposition());
        assertEquals(durableTransition, decision.durableTransition());
        assertEquals(outboxIntent, decision.outboxIntent());
        assertEquals(resultingStatus, decision.resultingState().taskStatus());
        assertEquals(resultingRetryCount, decision.resultingState().retryCount());
        assertEquals(metrics, decision.metrics());
        assertEquals(events, decision.events());
        assertFalse(decision.detail().isBlank());

        if (disposition == Disposition.ACCEPTED) {
            assertTrue(decision.accepted());
            assertEquals(event, decision.resultingState().lastAcceptedEvent());
        } else {
            assertFalse(decision.accepted());
            assertSame(initialState, decision.resultingState());
        }
    }

    private static Stream<Arguments> transitionTable() {
        return Stream.of(
                transition(
                        "J0/T0 accepts a new job and creates pending tasks",
                        TaskState.absent(),
                        new SchedulerEvent.JobSubmitted(100L),
                        Disposition.ACCEPTED,
                        DurableTransition.J0_T0_ACCEPT_JOB_AND_CREATE_TASKS,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        0,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(EventIntent.JOB_STARTED)
                ),
                transition(
                        "T1 creates exactly the next assignment generation",
                        pending(),
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_1, 1_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T1_CREATE_ASSIGNMENT,
                        OutboxIntent.TASK_ASSIGN,
                        TaskStatus.ASSIGNED,
                        0,
                        List.of(
                                MetricIntent.ASSIGNMENT_GENERATION_INCREMENT,
                                MetricIntent.DISPATCH_LATENCY_SAMPLE
                        ),
                        List.of(EventIntent.TASK_ASSIGNMENT_CREATED)
                ),
                transition(
                        "T2 commits an exactly fenced successful result",
                        assigned(0),
                        new SchedulerEvent.TaskResultReceived(IDENTITY_1, 2_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T2_COMMIT_SUCCESSFUL_RESULT,
                        OutboxIntent.NONE,
                        TaskStatus.COMPLETED,
                        0,
                        List.of(MetricIntent.TASK_RESULT_COMMITTED_INCREMENT),
                        List.of(EventIntent.TASK_RESULT_COMMITTED)
                ),
                transition(
                        "T3 releases a retryable execution failure",
                        assigned(0),
                        new SchedulerEvent.TaskExecutionFailed(IDENTITY_1, 3, 2_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T3_RELEASE_FOR_RETRY,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        1,
                        List.of(
                                MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                                MetricIntent.RETRY_INCREMENT
                        ),
                        List.of(EventIntent.TASK_FAILED)
                ),
                transition(
                        "T4 terminalizes an execution failure at retry exhaustion",
                        assigned(2),
                        new SchedulerEvent.TaskExecutionFailed(IDENTITY_1, 3, 2_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T4_TERMINALIZE_ASSIGNED_TASK,
                        OutboxIntent.NONE,
                        TaskStatus.FAILED,
                        3,
                        List.of(
                                MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                                MetricIntent.TERMINAL_FAILURE_INCREMENT
                        ),
                        List.of(EventIntent.TASK_FAILED)
                ),
                transition(
                        "T4 terminalizes a permanent execution failure before retry exhaustion",
                        assigned(0),
                        new SchedulerEvent.TaskExecutionFailed(
                                IDENTITY_1,
                                3,
                                false,
                                2_000L
                        ),
                        Disposition.ACCEPTED,
                        DurableTransition.T4_TERMINALIZE_ASSIGNED_TASK,
                        OutboxIntent.NONE,
                        TaskStatus.FAILED,
                        1,
                        List.of(
                                MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                                MetricIntent.TERMINAL_FAILURE_INCREMENT
                        ),
                        List.of(EventIntent.TASK_FAILED)
                ),
                transition(
                        "lease checks before the deadline are ignored",
                        assigned(0),
                        new SchedulerEvent.LeaseExpired(IDENTITY_1, 3, 4_999L),
                        Disposition.IGNORED,
                        DurableTransition.NONE,
                        OutboxIntent.NONE,
                        TaskStatus.ASSIGNED,
                        0,
                        List.of(),
                        List.of()
                ),
                transition(
                        "T3 releases a lease at its inclusive deadline",
                        assigned(0),
                        new SchedulerEvent.LeaseExpired(IDENTITY_1, 3, 5_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T3_RELEASE_FOR_RETRY,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        1,
                        List.of(
                                MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                                MetricIntent.RETRY_INCREMENT
                        ),
                        List.of(EventIntent.TASK_LEASE_EXPIRED)
                ),
                transition(
                        "timeout checks at the exact boundary are ignored",
                        assigned(0),
                        new SchedulerEvent.TaskTimedOut(IDENTITY_1, 3, 1_000L, 2_000L),
                        Disposition.IGNORED,
                        DurableTransition.NONE,
                        OutboxIntent.NONE,
                        TaskStatus.ASSIGNED,
                        0,
                        List.of(),
                        List.of()
                ),
                transition(
                        "T3 releases a task after the strict timeout boundary",
                        assigned(0),
                        new SchedulerEvent.TaskTimedOut(IDENTITY_1, 3, 1_000L, 2_001L),
                        Disposition.ACCEPTED,
                        DurableTransition.T3_RELEASE_FOR_RETRY,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        1,
                        List.of(
                                MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                                MetricIntent.RETRY_INCREMENT
                        ),
                        List.of(EventIntent.TASK_TIMEOUT)
                ),
                transition(
                        "an unrelated unavailable assignment is stale",
                        assigned(0),
                        new SchedulerEvent.WorkerUnavailable(OTHER_IDENTITY_1, 3, 2_000L),
                        Disposition.STALE,
                        DurableTransition.NONE,
                        OutboxIntent.NONE,
                        TaskStatus.ASSIGNED,
                        0,
                        List.of(),
                        List.of()
                ),
                transition(
                        "T3 releases work owned by an unavailable executor participant",
                        assigned(0),
                        new SchedulerEvent.WorkerUnavailable(IDENTITY_1, 3, 2_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T3_RELEASE_FOR_RETRY,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        1,
                        List.of(
                                MetricIntent.ATTEMPT_FAILURE_INCREMENT,
                                MetricIntent.RETRY_INCREMENT
                        ),
                        List.of(EventIntent.TASK_PEER_UNAVAILABLE)
                ),
                transition(
                        "R1 normalizes a persisted pending task during recovery",
                        TaskState.pending(1, 1, ASSIGNMENT_1, 200L),
                        new SchedulerEvent.CoordinatorRecovered(6_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.R1_NORMALIZE_PENDING,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        1,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(EventIntent.RUNNING_JOB_RESUMED)
                ),
                transition(
                        "H1 preserves a complete unexpired recovered assignment",
                        assigned(1),
                        new SchedulerEvent.CoordinatorRecovered(4_999L),
                        Disposition.ACCEPTED,
                        DurableTransition.NONE,
                        OutboxIntent.NONE,
                        TaskStatus.ASSIGNED,
                        1,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(EventIntent.RUNNING_JOB_RESUMED)
                ),
                transition(
                        "recovery releases an assignment at its inclusive lease deadline",
                        assigned(1),
                        new SchedulerEvent.CoordinatorRecovered(5_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T3_RELEASE_RECOVERED_ASSIGNMENT,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        1,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(EventIntent.RUNNING_JOB_RESUMED)
                ),
                transition(
                        "recovery releases an incomplete legacy assignment",
                        TaskState.incompleteAssigned(1, 1, 1_000L),
                        new SchedulerEvent.CoordinatorRecovered(5_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.T3_RELEASE_RECOVERED_ASSIGNMENT,
                        OutboxIntent.NONE,
                        TaskStatus.PENDING,
                        1,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(
                                EventIntent.LEGACY_TASK_ASSIGNMENT_RELEASED,
                                EventIntent.RUNNING_JOB_RESUMED
                        )
                ),
                transition(
                        "H1 hydrates a completed task without reopening it",
                        TaskState.completed(1, ASSIGNMENT_1),
                        new SchedulerEvent.CoordinatorRecovered(6_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.NONE,
                        OutboxIntent.NONE,
                        TaskStatus.COMPLETED,
                        1,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(EventIntent.RUNNING_JOB_RESUMED)
                ),
                transition(
                        "H1 hydrates a failed task without reopening it",
                        TaskState.failed(3, ASSIGNMENT_1),
                        new SchedulerEvent.CoordinatorRecovered(6_000L),
                        Disposition.ACCEPTED,
                        DurableTransition.NONE,
                        OutboxIntent.NONE,
                        TaskStatus.FAILED,
                        3,
                        List.of(MetricIntent.ACTIVE_JOB_GAUGE_REFRESH),
                        List.of(EventIntent.RUNNING_JOB_RESUMED)
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTransitions")
    void classifiesInvalidTransitionsExplicitly(String description,
                                                TaskState state,
                                                SchedulerEvent event,
                                                List<MetricIntent> metrics,
                                                List<EventIntent> events) {
        TransitionDecision decision = machine.decide(state, event);

        assertEquals(Disposition.INVALID, decision.disposition());
        assertEquals(DurableTransition.NONE, decision.durableTransition());
        assertEquals(OutboxIntent.NONE, decision.outboxIntent());
        assertSame(state, decision.resultingState());
        assertEquals(metrics, decision.metrics());
        assertEquals(events, decision.events());
        assertFalse(decision.detail().isBlank());
    }

    private static Stream<Arguments> invalidTransitions() {
        TaskState completedJob = new TaskState(
                JobStatus.COMPLETED,
                TaskStatus.COMPLETED,
                0,
                1,
                ASSIGNMENT_1,
                0L,
                -1L,
                null
        );
        TaskState retryOverflow = new TaskState(
                JobStatus.RUNNING,
                TaskStatus.ASSIGNED,
                Integer.MAX_VALUE,
                1,
                ASSIGNMENT_1,
                1_000L,
                -1L,
                null
        );

        return Stream.of(
                Arguments.of(
                        "T1 rejects a skipped generation",
                        pending(),
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_2, 1_000L),
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "T1 rejects an exhausted assignment-generation counter",
                        TaskState.pending(0, Integer.MAX_VALUE, null, 100L),
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_2, 1_000L),
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "T1 cannot reopen a completed task",
                        TaskState.completed(0, ASSIGNMENT_1),
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_2, 2_000L),
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "T1 rejects an assignment whose lease is already due",
                        pending(),
                        new SchedulerEvent.AssignmentRequested(
                                new TaskState.Assignment(
                                        new TaskState.AssignmentIdentity(
                                                1,
                                                "expired-assignment",
                                                "executor-1"
                                        ),
                                        "coordinator-lease-1",
                                        1_000L
                                ),
                                1_000L
                        ),
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "a terminal job cannot create another assignment",
                        completedJob,
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_2, 2_000L),
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "an unknown task cannot accept a result",
                        TaskState.absent(),
                        new SchedulerEvent.TaskResultReceived(IDENTITY_1, 2_000L),
                        List.of(MetricIntent.TASK_RESULT_UNKNOWN_INCREMENT),
                        List.of(EventIntent.TASK_RESULT_NOT_COMMITTED)
                ),
                Arguments.of(
                        "an absent job cannot accept an execution failure",
                        TaskState.absent(),
                        new SchedulerEvent.TaskExecutionFailed(IDENTITY_1, 3, 2_000L),
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "recovery cannot hydrate missing persisted state",
                        TaskState.absent(),
                        new SchedulerEvent.CoordinatorRecovered(2_000L),
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "retry counter overflow cannot produce a false transition",
                        retryOverflow,
                        new SchedulerEvent.TaskExecutionFailed(IDENTITY_1, 3, 2_000L),
                        List.of(),
                        List.of()
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("classifiedNoOpEvents")
    void distinguishesDuplicateStaleAndIgnoredEvents(String description,
                                                     TaskState state,
                                                     SchedulerEvent event,
                                                     Disposition expected,
                                                     List<MetricIntent> metrics,
                                                     List<EventIntent> events) {
        TransitionDecision decision = machine.decide(state, event);

        assertEquals(expected, decision.disposition());
        assertEquals(DurableTransition.NONE, decision.durableTransition());
        assertEquals(OutboxIntent.NONE, decision.outboxIntent());
        assertSame(state, decision.resultingState());
        assertEquals(metrics, decision.metrics());
        assertEquals(events, decision.events());
    }

    private static Stream<Arguments> classifiedNoOpEvents() {
        return Stream.of(
                Arguments.of(
                        "the active assignment is a duplicate",
                        assigned(0),
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_1, 1_500L),
                        Disposition.DUPLICATE,
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "a closed assignment request is stale",
                        TaskState.pending(1, 1, ASSIGNMENT_1, 2_000L),
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_1, 2_100L),
                        Disposition.STALE,
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "an exactly committed result is duplicate",
                        TaskState.completed(0, ASSIGNMENT_1),
                        new SchedulerEvent.TaskResultReceived(IDENTITY_1, 2_100L),
                        Disposition.DUPLICATE,
                        List.of(MetricIntent.TASK_RESULT_DUPLICATE_INCREMENT),
                        List.of(EventIntent.TASK_RESULT_DUPLICATE_IGNORED)
                ),
                Arguments.of(
                        "a mismatched result generation is stale",
                        assigned(0),
                        new SchedulerEvent.TaskResultReceived(OTHER_IDENTITY_1, 2_100L),
                        Disposition.STALE,
                        List.of(MetricIntent.TASK_RESULT_STALE_INCREMENT),
                        List.of(EventIntent.TASK_RESULT_STALE_REJECTED)
                ),
                Arguments.of(
                        "a mismatched execution failure is stale",
                        assigned(0),
                        new SchedulerEvent.TaskExecutionFailed(OTHER_IDENTITY_1, 3, 2_100L),
                        Disposition.STALE,
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "a mismatched lease expiry is stale",
                        assigned(0),
                        new SchedulerEvent.LeaseExpired(OTHER_IDENTITY_1, 3, 5_000L),
                        Disposition.STALE,
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "a mismatched timeout is stale",
                        assigned(0),
                        new SchedulerEvent.TaskTimedOut(OTHER_IDENTITY_1, 3, 1_000L, 2_100L),
                        Disposition.STALE,
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "an unavailable non-owner generation is stale",
                        assigned(0),
                        new SchedulerEvent.WorkerUnavailable(OTHER_IDENTITY_1, 3, 2_100L),
                        Disposition.STALE,
                        List.of(),
                        List.of()
                ),
                Arguments.of(
                        "a late failure for a released assignment is stale",
                        TaskState.pending(1, 1, ASSIGNMENT_1, 2_000L),
                        new SchedulerEvent.TaskExecutionFailed(IDENTITY_1, 3, 2_100L),
                        Disposition.STALE,
                        List.of(),
                        List.of()
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("acceptedEvents")
    void replayOfEveryAcceptedEventIsClassifiedDuplicate(String description,
                                                         TaskState initialState,
                                                         SchedulerEvent event) {
        TransitionDecision first = machine.decide(initialState, event);
        assertEquals(Disposition.ACCEPTED, first.disposition());

        TransitionDecision replay = machine.decide(first.resultingState(), event);

        assertEquals(Disposition.DUPLICATE, replay.disposition());
        assertEquals(DurableTransition.NONE, replay.durableTransition());
        assertEquals(OutboxIntent.NONE, replay.outboxIntent());
        assertSame(first.resultingState(), replay.resultingState());
    }

    private static Stream<Arguments> acceptedEvents() {
        return Stream.of(
                Arguments.of(
                        "JobSubmitted replay",
                        TaskState.absent(),
                        new SchedulerEvent.JobSubmitted(100L)
                ),
                Arguments.of(
                        "AssignmentRequested replay",
                        pending(),
                        new SchedulerEvent.AssignmentRequested(ASSIGNMENT_1, 1_000L)
                ),
                Arguments.of(
                        "TaskResultReceived replay",
                        assigned(0),
                        new SchedulerEvent.TaskResultReceived(IDENTITY_1, 2_000L)
                ),
                Arguments.of(
                        "TaskExecutionFailed replay",
                        assigned(0),
                        new SchedulerEvent.TaskExecutionFailed(IDENTITY_1, 3, 2_000L)
                ),
                Arguments.of(
                        "LeaseExpired replay",
                        assigned(0),
                        new SchedulerEvent.LeaseExpired(IDENTITY_1, 3, 5_000L)
                ),
                Arguments.of(
                        "TaskTimedOut replay",
                        assigned(0),
                        new SchedulerEvent.TaskTimedOut(IDENTITY_1, 3, 1_000L, 2_001L)
                ),
                Arguments.of(
                        "WorkerUnavailable replay",
                        assigned(0),
                        new SchedulerEvent.WorkerUnavailable(IDENTITY_1, 3, 2_000L)
                ),
                Arguments.of(
                        "CoordinatorRecovered replay",
                        TaskState.pending(1, 1, ASSIGNMENT_1, 200L),
                        new SchedulerEvent.CoordinatorRecovered(6_000L)
                )
        );
    }

    @Test
    void ignoredEventDecisionIsStableAcrossRepeatedEvaluation() {
        TaskState state = assigned(0);
        SchedulerEvent event = new SchedulerEvent.LeaseExpired(IDENTITY_1, 3, 4_999L);

        assertEquals(machine.decide(state, event), machine.decide(state, event));
    }

    @Test
    void validatesEventAndProjectionInvariantsAtConstruction() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SchedulerEvent.TaskExecutionFailed(IDENTITY_1, 0, 2_000L)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskState.AssignmentIdentity(
                        0,
                        "assignment",
                        "executor"
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskState.Assignment(
                        new TaskState.AssignmentIdentity(1, "assignment", "executor"),
                        "",
                        5_000L
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskState(
                        JobStatus.RUNNING,
                        TaskStatus.PENDING,
                        0,
                        0,
                        null,
                        100L,
                        200L,
                        null
                )
        );
    }

    @Test
    void decisionEffectCollectionsAreImmutable() {
        TransitionDecision decision = machine.decide(
                pending(),
                new SchedulerEvent.AssignmentRequested(ASSIGNMENT_1, 1_000L)
        );

        assertThrows(
                UnsupportedOperationException.class,
                () -> decision.metrics().add(MetricIntent.RETRY_INCREMENT)
        );
        assertThrows(
                UnsupportedOperationException.class,
                () -> decision.events().add(EventIntent.TASK_FAILED)
        );
    }

    private static Arguments transition(String description,
                                        TaskState initialState,
                                        SchedulerEvent event,
                                        Disposition disposition,
                                        DurableTransition durableTransition,
                                        OutboxIntent outboxIntent,
                                        TaskStatus resultingStatus,
                                        int resultingRetryCount,
                                        List<MetricIntent> metrics,
                                        List<EventIntent> events) {
        return Arguments.of(
                description,
                initialState,
                event,
                disposition,
                durableTransition,
                outboxIntent,
                resultingStatus,
                resultingRetryCount,
                metrics,
                events
        );
    }

    private static TaskState pending() {
        return TaskState.pending(0, 0, null, 100L);
    }

    private static TaskState assigned(int retryCount) {
        return TaskState.assigned(retryCount, ASSIGNMENT_1, 1_000L);
    }
}
