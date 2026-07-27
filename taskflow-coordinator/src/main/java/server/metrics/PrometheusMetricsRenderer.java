package server.metrics;

import server.metrics.CoordinatorMetricsCollector.CoordinatorMetricsSnapshot;
import server.scheduler.SchedulerMetrics;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

final class PrometheusMetricsRenderer {
    static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";
    private static final Pattern METRIC_NAME =
            Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Set<String> FORBIDDEN_ENTITY_LABELS = Set.of(
            "job_id",
            "task_id",
            "assignment_id",
            "worker_id"
    );

    private static final List<MetricDefinition> DEFINITIONS = List.of(
            counter(SchedulerMetrics.JOBS_ACCEPTED_TOTAL_NAME,
                    "Jobs durably accepted by this coordinator process."),
            counter(SchedulerMetrics.JOBS_COMPLETED_TOTAL_NAME,
                    "Jobs durably terminalized successfully by this coordinator process."),
            counter(SchedulerMetrics.JOBS_FAILED_TOTAL_NAME,
                    "Jobs durably terminalized unsuccessfully by this coordinator process."),
            counter(SchedulerMetrics.TASKS_ASSIGNED_TOTAL_NAME,
                    "Task assignments installed after an authoritative assignment commit."),
            counter(SchedulerMetrics.TASK_ASSIGNMENT_GENERATIONS_TOTAL_NAME,
                    "Authoritative task assignment generations created."),
            counter(SchedulerMetrics.TASK_RESULTS_COMMITTED_TOTAL_NAME,
                    "Task results committed by the exact assignment fence."),
            counter(SchedulerMetrics.TASK_RESULTS_STALE_TOTAL_NAME,
                    "Task results rejected as stale by the assignment fence."),
            counter(SchedulerMetrics.TASK_RESULTS_DUPLICATE_TOTAL_NAME,
                    "Already-committed task results classified as duplicates."),
            counter(SchedulerMetrics.TASKS_RETRIED_TOTAL_NAME,
                    "Failed attempts durably closed with another logical retry permitted."),
            counter(SchedulerMetrics.TASK_LEASE_EXPIRATIONS_TOTAL_NAME,
                    "Current assignment leases durably closed after expiry."),
            gauge(SchedulerMetrics.SCHEDULER_MAILBOX_DEPTH_NAME,
                    "Messages currently admitted to the scheduler mailbox."),
            gauge(SchedulerMetrics.SCHEDULER_PENDING_TASKS_NAME,
                    "Pending task entries currently indexed for dispatch."),
            gauge(SchedulerMetrics.SCHEDULER_DUE_DEADLINES_NAME,
                    "Timeout and lease deadline entries pending scheduler evaluation."),
            gauge("taskflow_outbox_pending",
                    "Pending durable coordinator broker outbox rows, or NaN when unavailable."),
            gauge("taskflow_outbox_oldest_age_seconds",
                    "Age in seconds of the oldest pending outbox row, or NaN when unavailable."),
            counter("taskflow_broker_redeliveries_total",
                    "Inbound broker deliveries observed beyond their first delivery."),
            counter("taskflow_broker_quarantined_total",
                    "Deliveries successfully handed to automatic terminal quarantine."),
            gauge(SchedulerMetrics.WORKER_CAPACITY_USED_NAME,
                    "Capacity units reserved by current authoritative assignments."),
            counter("taskflow_orphan_outputs_total",
                    "Authoritatively classified orphan output deletion operations completed."),
            histogram(SchedulerMetrics.ASSIGNMENT_LATENCY_SECONDS_NAME,
                    "Seconds from task eligibility to authoritative assignment creation."),
            histogram(SchedulerMetrics.RESULT_COMMIT_LATENCY_SECONDS_NAME,
                    "Seconds from assignment start to authoritative task-result commit."),
            histogram(SchedulerMetrics.RECOVERY_DURATION_SECONDS_NAME,
                    "Seconds spent in successful coordinator startup recovery.")
    );

    String render(CoordinatorMetricsSnapshot snapshot) {
        StringBuilder out = new StringBuilder(8_192);
        SchedulerMetrics.Snapshot scheduler = snapshot.scheduler();
        for (MetricDefinition definition : DEFINITIONS) {
            appendMetadata(out, definition);
            switch (definition.name()) {
                case SchedulerMetrics.JOBS_ACCEPTED_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.jobsAcceptedTotal());
                case SchedulerMetrics.JOBS_COMPLETED_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.jobsCompletedTotal());
                case SchedulerMetrics.JOBS_FAILED_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.jobsFailedTotal());
                case SchedulerMetrics.TASKS_ASSIGNED_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.tasksAssignedTotal());
                case SchedulerMetrics.TASK_ASSIGNMENT_GENERATIONS_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.assignmentGenerationsTotal());
                case SchedulerMetrics.TASK_RESULTS_COMMITTED_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.taskResultsCommittedTotal());
                case SchedulerMetrics.TASK_RESULTS_STALE_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.taskResultsStaleTotal());
                case SchedulerMetrics.TASK_RESULTS_DUPLICATE_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.taskResultsDuplicateTotal());
                case SchedulerMetrics.TASKS_RETRIED_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.retryCount());
                case SchedulerMetrics.TASK_LEASE_EXPIRATIONS_TOTAL_NAME ->
                        appendLong(out, definition.name(), scheduler.taskLeaseExpirationsTotal());
                case SchedulerMetrics.SCHEDULER_MAILBOX_DEPTH_NAME ->
                        appendLong(out, definition.name(), scheduler.queueDepth());
                case SchedulerMetrics.SCHEDULER_PENDING_TASKS_NAME ->
                        appendLong(out, definition.name(), scheduler.pendingTasks());
                case SchedulerMetrics.SCHEDULER_DUE_DEADLINES_NAME ->
                        appendLong(out, definition.name(), scheduler.dueDeadlines());
                case "taskflow_outbox_pending" ->
                        appendDouble(out, definition.name(), snapshot.outboxPending());
                case "taskflow_outbox_oldest_age_seconds" ->
                        appendDouble(out, definition.name(), snapshot.outboxOldestAgeSeconds());
                case "taskflow_broker_redeliveries_total" ->
                        appendLong(out, definition.name(), snapshot.brokerRedeliveriesTotal());
                case "taskflow_broker_quarantined_total" ->
                        appendLong(out, definition.name(), snapshot.brokerQuarantinedTotal());
                case SchedulerMetrics.WORKER_CAPACITY_USED_NAME ->
                        appendLong(out, definition.name(), scheduler.workerCapacityUsed());
                case "taskflow_orphan_outputs_total" ->
                        appendLong(out, definition.name(), snapshot.orphanOutputsTotal());
                case SchedulerMetrics.ASSIGNMENT_LATENCY_SECONDS_NAME ->
                        appendHistogram(out, definition.name(),
                                scheduler.assignmentLatencySeconds());
                case SchedulerMetrics.RESULT_COMMIT_LATENCY_SECONDS_NAME ->
                        appendHistogram(out, definition.name(),
                                scheduler.resultCommitLatencySeconds());
                case SchedulerMetrics.RECOVERY_DURATION_SECONDS_NAME ->
                        appendHistogram(out, definition.name(),
                                scheduler.recoveryDurationSeconds());
                default -> throw new IllegalStateException(
                        "No renderer for metric " + definition.name()
                );
            }
        }
        return out.toString();
    }

    static List<MetricDefinition> definitions() {
        return DEFINITIONS;
    }

    private static void appendMetadata(StringBuilder out, MetricDefinition definition) {
        out.append("# HELP ")
                .append(definition.name())
                .append(' ')
                .append(definition.help())
                .append('\n')
                .append("# TYPE ")
                .append(definition.name())
                .append(' ')
                .append(definition.type().wireName())
                .append('\n');
    }

    private static void appendLong(StringBuilder out, String name, long value) {
        out.append(name).append(' ').append(value).append('\n');
    }

    private static void appendDouble(StringBuilder out, String name, double value) {
        out.append(name).append(' ').append(formatDouble(value)).append('\n');
    }

    private static void appendHistogram(
            StringBuilder out,
            String name,
            SchedulerMetrics.HistogramSnapshot histogram
    ) {
        for (int i = 0; i < histogram.upperBoundsSeconds().size(); i++) {
            out.append(name)
                    .append("_bucket{le=\"")
                    .append(formatDouble(histogram.upperBoundsSeconds().get(i)))
                    .append("\"} ")
                    .append(histogram.cumulativeBucketCounts().get(i))
                    .append('\n');
        }
        out.append(name)
                .append("_bucket{le=\"+Inf\"} ")
                .append(histogram.count())
                .append('\n');
        appendDouble(out, name + "_sum", histogram.sumSeconds());
        appendLong(out, name + "_count", histogram.count());
    }

    private static String formatDouble(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0.0 ? "+Inf" : "-Inf";
        }
        return String.format(Locale.US, "%.9g", value);
    }

    private static MetricDefinition counter(String name, String help) {
        return new MetricDefinition(name, MetricType.COUNTER, help, Set.of());
    }

    private static MetricDefinition gauge(String name, String help) {
        return new MetricDefinition(name, MetricType.GAUGE, help, Set.of());
    }

    private static MetricDefinition histogram(String name, String help) {
        return new MetricDefinition(name, MetricType.HISTOGRAM, help, Set.of("le"));
    }

    enum MetricType {
        COUNTER("counter"),
        GAUGE("gauge"),
        HISTOGRAM("histogram");

        private final String wireName;

        MetricType(String wireName) {
            this.wireName = wireName;
        }

        String wireName() {
            return wireName;
        }
    }

    record MetricDefinition(
            String name,
            MetricType type,
            String help,
            Set<String> labelNames
    ) {
        MetricDefinition {
            if (name == null || !METRIC_NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid Prometheus metric name: " + name);
            }
            if (type == null || help == null || help.isBlank()) {
                throw new IllegalArgumentException(
                        "Metric type and help are required for " + name
                );
            }
            labelNames = Set.copyOf(labelNames);
            if (!java.util.Collections.disjoint(labelNames, FORBIDDEN_ENTITY_LABELS)) {
                throw new IllegalArgumentException(
                        "High-cardinality entity labels are prohibited for " + name
                );
            }
            Set<String> permitted = type == MetricType.HISTOGRAM
                    ? Set.of("le")
                    : Set.of();
            if (!permitted.equals(labelNames)) {
                throw new IllegalArgumentException(
                        "Metric labels must be empty except for histogram le: " + name
                );
            }
        }
    }
}
