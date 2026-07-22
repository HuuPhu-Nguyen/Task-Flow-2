package server.scheduler.transition;

import java.util.List;

/**
 * Complete, infrastructure-free description of how one scheduler event should
 * be handled. Only an ACCEPTED decision may request durable or outbound work.
 */
public record TransitionDecision(Disposition disposition,
                                 DurableTransition durableTransition,
                                 OutboxIntent outboxIntent,
                                 TaskState resultingState,
                                 List<MetricIntent> metrics,
                                 List<EventIntent> events,
                                 String detail) {

    public enum Disposition {
        ACCEPTED,
        DUPLICATE,
        STALE,
        INVALID,
        IGNORED
    }

    public enum DurableTransition {
        NONE,
        J0_T0_ACCEPT_JOB_AND_CREATE_TASKS,
        T1_CREATE_ASSIGNMENT,
        T2_COMMIT_SUCCESSFUL_RESULT,
        T3_RELEASE_FOR_RETRY,
        T4_TERMINALIZE_ASSIGNED_TASK,
        R1_NORMALIZE_PENDING,
        T3_RELEASE_RECOVERED_ASSIGNMENT
    }

    /**
     * Logical outbound intent. TASK_ASSIGN is persisted in RabbitMQ mode and
     * realized through the direct compatibility output otherwise.
     */
    public enum OutboxIntent {
        NONE,
        TASK_ASSIGN
    }

    public enum MetricIntent {
        ACTIVE_JOB_GAUGE_REFRESH,
        ASSIGNMENT_GENERATION_INCREMENT,
        DISPATCH_LATENCY_SAMPLE,
        TASK_RESULT_COMMITTED_INCREMENT,
        TASK_RESULT_DUPLICATE_INCREMENT,
        TASK_RESULT_STALE_INCREMENT,
        TASK_RESULT_UNKNOWN_INCREMENT,
        ATTEMPT_FAILURE_INCREMENT,
        RETRY_INCREMENT,
        TERMINAL_FAILURE_INCREMENT
    }

    public enum EventIntent {
        JOB_STARTED("job_started"),
        TASK_ASSIGNMENT_CREATED("task_assignment_created"),
        TASK_RESULT_COMMITTED("task_result_committed"),
        TASK_RESULT_DUPLICATE_IGNORED("task_result_duplicate_ignored"),
        TASK_RESULT_STALE_REJECTED("task_result_stale_rejected"),
        TASK_RESULT_NOT_COMMITTED("task_result_not_committed"),
        TASK_FAILED("task_failed"),
        TASK_LEASE_EXPIRED("task_lease_expired"),
        TASK_TIMEOUT("task_timeout"),
        TASK_PEER_UNAVAILABLE("task_peer_unavailable"),
        RUNNING_JOB_RESUMED("running_job_resumed"),
        LEGACY_TASK_ASSIGNMENT_RELEASED("legacy_task_assignment_released");

        private final String eventName;

        EventIntent(String eventName) {
            this.eventName = eventName;
        }

        public String eventName() {
            return eventName;
        }
    }

    public TransitionDecision {
        if (disposition == null) {
            throw new IllegalArgumentException("disposition is required.");
        }
        if (durableTransition == null) {
            throw new IllegalArgumentException("durableTransition is required.");
        }
        if (outboxIntent == null) {
            throw new IllegalArgumentException("outboxIntent is required.");
        }
        if (resultingState == null) {
            throw new IllegalArgumentException("resultingState is required.");
        }
        metrics = List.copyOf(metrics == null ? List.of() : metrics);
        events = List.copyOf(events == null ? List.of() : events);
        detail = detail == null ? "" : detail;

        if (disposition != Disposition.ACCEPTED
                && (durableTransition != DurableTransition.NONE
                || outboxIntent != OutboxIntent.NONE)) {
            throw new IllegalArgumentException(
                    "Only accepted decisions may request durable or outbound effects."
            );
        }
    }

    public boolean accepted() {
        return disposition == Disposition.ACCEPTED;
    }
}
