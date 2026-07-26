package server.scheduler;

import server.model.MessageEnvelope;
import server.registry.CapacityMetricsSnapshot;
import server.registry.PeerRegistry;
import server.runtime.TaskFlowClock;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/** Owns scheduler gauge refresh and periodic structured metric snapshots. */
final class SchedulerMetricsService {
    private final BlockingQueue<MessageEnvelope> inboundMailbox;
    private final SchedulerState state;
    private final SchedulerMetrics metrics;
    private final PeerRegistry registry;
    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final SchedulerEventLog events;
    private long lastMetricsLogAtMillis;

    SchedulerMetricsService(BlockingQueue<MessageEnvelope> inboundMailbox,
                            SchedulerState state,
                            SchedulerMetrics metrics,
                            PeerRegistry registry,
                            SchedulerConfig config,
                            TaskFlowClock clock,
                            SchedulerEventLog events) {
        this.inboundMailbox = inboundMailbox;
        this.state = state;
        this.metrics = metrics;
        this.registry = registry;
        this.config = config;
        this.clock = clock;
        this.events = events;
    }

    void updateAndMaybeLog() {
        long now = clock.nowEpochMillis();
        metrics.setQueueDepth(inboundMailbox.size());
        metrics.setActiveJobs(state.activeJobCount());
        if (now - lastMetricsLogAtMillis < config.metricsLogIntervalMillis()) {
            return;
        }
        lastMetricsLogAtMillis = now;
        SchedulerMetrics.Snapshot snapshot = metrics.snapshot();
        SchedulerWorkloadIndex.Snapshot workload = state.workloadSnapshot();
        CapacityMetricsSnapshot capacity = registry.capacityMetricsSnapshot();
        events.info("scheduler_metrics", events.fields(
                "queue_depth", snapshot.queueDepth(),
                "active_jobs", snapshot.activeJobs(),
                "pending_tasks_indexed", workload.pendingTasks(),
                "runnable_jobs_indexed", workload.runnableJobs(),
                "capacity_waiting_jobs_indexed", workload.capacityWaitingJobs(),
                "live_assignments_indexed", workload.liveAssignments(),
                "deadline_entries_indexed", workload.deadlineEntries(),
                "deadline_head_checks_total", workload.deadlineHeadChecks(),
                "deadline_entries_popped_total", workload.deadlineEntriesPopped(),
                "deadline_entries_validated_total", workload.deadlineEntriesValidated(),
                "deadline_stale_rejected_total", workload.staleDeadlineEntriesRejected(),
                "deadline_reschedules_total", workload.deadlineReschedules(),
                "dispatch_latency_ms", String.format(Locale.US, "%.2f", snapshot.avgDispatchLatencyMs()),
                "retry_count", snapshot.retryCount(),
                "task_success_rate", String.format(Locale.US, "%.4f", snapshot.taskSuccessRate()),
                "failure_count", snapshot.failureCount(),
                SchedulerMetrics.TASK_RESULTS_COMMITTED_TOTAL_NAME,
                snapshot.taskResultsCommittedTotal(),
                SchedulerMetrics.TASK_RESULTS_STALE_TOTAL_NAME,
                snapshot.taskResultsStaleTotal(),
                SchedulerMetrics.TASK_RESULTS_DUPLICATE_TOTAL_NAME,
                snapshot.taskResultsDuplicateTotal(),
                SchedulerMetrics.ASSIGNMENT_GENERATIONS_TOTAL_NAME,
                snapshot.assignmentGenerationsTotal(),
                "taskflow_capacity_snapshots_accepted_total",
                capacity.acceptedSnapshots(),
                "taskflow_capacity_snapshots_stale_total",
                capacity.staleSnapshots(),
                "taskflow_capacity_snapshots_incompatible_total",
                capacity.incompatibleSnapshots(),
                "taskflow_capacity_reservations_created_total",
                capacity.reservationsCreated(),
                "taskflow_capacity_reservations_released_total",
                capacity.reservationsReleased(),
                "taskflow_capacity_projection_failures_total",
                capacity.projectionFailures(),
                "taskflow_capacity_active_reservations",
                capacity.activeReservations(),
                "taskflow_capacity_reserved_units",
                capacity.reservedCapacityUnits(),
                "unknown_result_count", snapshot.unknownResultCount(),
                "result_storage_failure_count", snapshot.resultStorageFailureCount()
        ));
    }

    long millisUntilNextUpdate() {
        long now = clock.nowEpochMillis();
        long interval = config.metricsLogIntervalMillis();
        long nextUpdate = lastMetricsLogAtMillis >= Long.MAX_VALUE - interval
                ? Long.MAX_VALUE
                : lastMetricsLogAtMillis + interval;
        if (nextUpdate == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return nextUpdate <= now ? 0L : nextUpdate - now;
    }
}
