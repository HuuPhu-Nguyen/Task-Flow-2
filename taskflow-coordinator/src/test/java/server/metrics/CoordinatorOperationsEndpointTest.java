package server.metrics;

import org.junit.jupiter.api.Test;
import server.db.BrokerOutboxStore;
import server.health.CoordinatorHealth;
import server.runtime.TaskFlowClock;
import server.scheduler.SchedulerMetrics;
import transport.rabbitmq.RabbitMqTransportMetrics;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorOperationsEndpointTest {
    private static final Set<String> MANDATORY_METRICS = Set.of(
            "taskflow_jobs_accepted_total",
            "taskflow_jobs_completed_total",
            "taskflow_jobs_failed_total",
            "taskflow_tasks_assigned_total",
            "taskflow_task_assignment_generations_total",
            "taskflow_task_results_committed_total",
            "taskflow_task_results_stale_total",
            "taskflow_task_results_duplicate_total",
            "taskflow_tasks_retried_total",
            "taskflow_task_lease_expirations_total",
            "taskflow_scheduler_mailbox_depth",
            "taskflow_scheduler_pending_tasks",
            "taskflow_scheduler_due_deadlines",
            "taskflow_outbox_pending",
            "taskflow_outbox_oldest_age_seconds",
            "taskflow_broker_redeliveries_total",
            "taskflow_broker_quarantined_total",
            "taskflow_worker_capacity_used",
            "taskflow_orphan_outputs_total",
            "taskflow_assignment_latency_seconds",
            "taskflow_result_commit_latency_seconds",
            "taskflow_recovery_duration_seconds"
    );

    @Test
    void exactInventoryRendersTypesUnitsAndCurrentValues() {
        CoordinatorMetricsCollector collector = collector(true);
        PrometheusMetricsRenderer renderer = new PrometheusMetricsRenderer();

        String body = renderer.render(collector.snapshot());
        Set<String> actualNames = PrometheusMetricsRenderer.definitions().stream()
                .map(PrometheusMetricsRenderer.MetricDefinition::name)
                .collect(Collectors.toUnmodifiableSet());

        assertEquals(MANDATORY_METRICS, actualNames);
        for (String metric : MANDATORY_METRICS) {
            assertTrue(body.contains("# HELP " + metric + " "));
            assertTrue(body.contains("# TYPE " + metric + " "));
        }
        assertTrue(body.contains("taskflow_jobs_accepted_total 1"));
        assertTrue(body.contains("taskflow_outbox_pending 3.00000000"));
        assertTrue(body.contains("taskflow_outbox_oldest_age_seconds 2.00000000"));
        assertTrue(body.contains("taskflow_broker_redeliveries_total 4"));
        assertTrue(body.contains("taskflow_broker_quarantined_total 5"));
        assertTrue(body.contains("taskflow_orphan_outputs_total 6"));
        assertTrue(body.contains("taskflow_assignment_latency_seconds_count 1"));
        assertTrue(body.contains("taskflow_result_commit_latency_seconds_sum 2.50000000"));
        assertTrue(body.contains("taskflow_recovery_duration_seconds_count 1"));
    }

    @Test
    void cardinalityPolicyForbidsEntityLabelsAndAllowsOnlyFixedHistogramBuckets() {
        Set<String> forbidden = Set.of("job_id", "task_id", "assignment_id", "worker_id");
        for (PrometheusMetricsRenderer.MetricDefinition definition
                : PrometheusMetricsRenderer.definitions()) {
            assertTrue(java.util.Collections.disjoint(
                    definition.labelNames(),
                    forbidden
            ));
            assertEquals(
                    definition.type() == PrometheusMetricsRenderer.MetricType.HISTOGRAM
                            ? Set.of("le")
                            : Set.of(),
                    definition.labelNames()
            );
        }

        String body = new PrometheusMetricsRenderer().render(collector(true).snapshot());
        for (String label : forbidden) {
            assertFalse(body.contains(label + "=\""));
        }
    }

    @Test
    void unavailableOutboxObservationIsNotMisreportedAsZero() {
        String body = new PrometheusMetricsRenderer().render(collector(false).snapshot());

        assertTrue(body.contains("taskflow_outbox_pending NaN"));
        assertTrue(body.contains("taskflow_outbox_oldest_age_seconds NaN"));
    }

    @Test
    void disabledEndpointCreatesNoListener() throws Exception {
        CoordinatorOperationsEndpoint endpoint = new CoordinatorOperationsEndpoint(
                new MetricsEndpointConfig(false, "127.0.0.1", 0),
                new CoordinatorHealth()
        );

        endpoint.start();

        assertEquals(-1, endpoint.boundPort());
        endpoint.close();
    }

    @Test
    void startupReportsLoopNotYetLiveAndUnreadyBeforeMetricsAreInstalled()
            throws Exception {
        CoordinatorOperationsEndpoint endpoint = new CoordinatorOperationsEndpoint(
                new MetricsEndpointConfig(true, "127.0.0.1", 0),
                new CoordinatorHealth()
        );
        endpoint.start();
        int port = endpoint.boundPort();
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
            HttpResponse<String> liveness = get(client, port, "/health/live");
            HttpResponse<String> readiness = get(client, port, "/health/ready");
            HttpResponse<String> metrics = get(client, port, "/metrics");

            assertEquals(503, liveness.statusCode());
            assertTrue(liveness.body().contains("\"state\":\"STARTING\""));
            assertTrue(liveness.body().contains("\"live\":false"));
            assertEquals(503, readiness.statusCode());
            assertTrue(readiness.body().contains("\"ready\":false"));
            assertTrue(readiness.body().contains("\"STARTING\""));
            assertEquals(503, metrics.statusCode());
        } finally {
            endpoint.close();
        }
    }

    @Test
    void httpEndpointServesPrometheusAndHealthContractsThenStopsCleanly() throws Exception {
        CoordinatorHealth health = new CoordinatorHealth();
        health.activate(
                () -> true,
                () -> new CoordinatorHealth.ReadinessInputs(
                        true,
                        true,
                        true,
                        0L,
                        100L,
                        false,
                        false
                )
        );
        CoordinatorOperationsEndpoint endpoint = new CoordinatorOperationsEndpoint(
                new MetricsEndpointConfig(true, "127.0.0.1", 0),
                health
        );
        endpoint.installMetricsCollector(collector(true));
        endpoint.start();
        int port = endpoint.boundPort();
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
            URI uri = URI.create("http://127.0.0.1:" + port + "/metrics");
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder(uri).GET().build(),
                    HttpResponse.BodyHandlers.ofString()
            );
            HttpResponse<String> post = client.send(
                    HttpRequest.newBuilder(uri)
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString()
            );

            assertEquals(200, response.statusCode());
            assertEquals(
                    PrometheusMetricsRenderer.CONTENT_TYPE,
                    response.headers().firstValue("Content-Type").orElseThrow()
            );
            assertTrue(response.body().contains("taskflow_jobs_accepted_total 1"));
            assertEquals(405, post.statusCode());
            assertEquals("GET", post.headers().firstValue("Allow").orElseThrow());
            HttpResponse<String> liveness = get(client, port, "/health/live");
            HttpResponse<String> readiness = get(client, port, "/health/ready");
            HttpResponse<String> nested = get(client, port, "/health/ready/nested");

            assertEquals(200, liveness.statusCode());
            assertEquals(CoordinatorOperationsEndpoint.JSON_CONTENT_TYPE,
                    liveness.headers().firstValue("Content-Type").orElseThrow());
            assertEquals(200, readiness.statusCode());
            assertTrue(readiness.body().contains("\"state\":\"READY\""));
            assertTrue(readiness.body().contains("\"ready\":true"));
            assertEquals(404, nested.statusCode());
        } finally {
            endpoint.close();
        }
        assertEquals(-1, endpoint.boundPort());
    }

    private static HttpResponse<String> get(
            HttpClient client,
            int port,
            String path
    ) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
    }

    private static CoordinatorMetricsCollector collector(boolean outboxAvailable) {
        SchedulerMetrics scheduler = populatedSchedulerMetrics();
        return new CoordinatorMetricsCollector(
                scheduler::snapshot,
                () -> new RabbitMqTransportMetrics.Snapshot(4L, 5L),
                () -> outboxAvailable
                        ? BrokerOutboxStore.PendingOutboxMetrics.observed(3L, 8_000L)
                        : BrokerOutboxStore.PendingOutboxMetrics.storageFailure(),
                () -> 6L,
                new FixedClock(10_000L)
        );
    }

    private static SchedulerMetrics populatedSchedulerMetrics() {
        SchedulerMetrics metrics = new SchedulerMetrics();
        metrics.recordJobAccepted();
        metrics.recordJobTerminal(true);
        metrics.recordJobTerminal(false);
        metrics.recordAssignmentGeneration(20L);
        metrics.recordRetry();
        metrics.recordLeaseExpiration();
        metrics.recordResultCommitOutcome(
                server.db.JobStateStore.ResultCommitOutcome.COMMITTED,
                2_500L
        );
        metrics.recordResultCommitOutcome(
                server.db.JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT
        );
        metrics.recordResultCommitOutcome(
                server.db.JobStateStore.ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED
        );
        metrics.recordRecoveryDuration(100L);
        metrics.setQueueDepth(7L);
        metrics.setPendingTasks(8L);
        metrics.setDueDeadlines(9L);
        metrics.setWorkerCapacityUsed(10L);
        return metrics;
    }

    private record FixedClock(long nowEpochMillis) implements TaskFlowClock {
        @Override
        public Instant now() {
            return Instant.ofEpochMilli(nowEpochMillis);
        }
    }
}
