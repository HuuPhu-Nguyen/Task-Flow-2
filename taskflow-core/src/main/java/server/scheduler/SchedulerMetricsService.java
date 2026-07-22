package server.scheduler;

import server.model.MessageEnvelope;
import server.runtime.TaskFlowClock;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/** Owns scheduler gauge refresh and periodic structured metric snapshots. */
final class SchedulerMetricsService {
    private final BlockingQueue<MessageEnvelope> inboundMailbox;
    private final SchedulerState state;
    private final SchedulerMetrics metrics;
    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final SchedulerEventLog events;
    private long lastMetricsLogAtMillis;

    SchedulerMetricsService(BlockingQueue<MessageEnvelope> inboundMailbox,
                            SchedulerState state,
                            SchedulerMetrics metrics,
                            SchedulerConfig config,
                            TaskFlowClock clock,
                            SchedulerEventLog events) {
        this.inboundMailbox = inboundMailbox;
        this.state = state;
        this.metrics = metrics;
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
        events.info("scheduler_metrics", events.fields(
                "queue_depth", snapshot.queueDepth(),
                "active_jobs", snapshot.activeJobs(),
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
                "unknown_result_count", snapshot.unknownResultCount(),
                "result_storage_failure_count", snapshot.resultStorageFailureCount()
        ));
    }
}
