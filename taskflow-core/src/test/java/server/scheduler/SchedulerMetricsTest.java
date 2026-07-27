package server.scheduler;

import org.junit.jupiter.api.Test;
import server.db.JobStateStore;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchedulerMetricsTest {

    @Test
    void exposesExactFencingMetricNamesAndTypedCounters() {
        assertEquals(
                "taskflow_task_results_committed_total",
                SchedulerMetrics.TASK_RESULTS_COMMITTED_TOTAL_NAME
        );
        assertEquals(
                "taskflow_task_results_stale_total",
                SchedulerMetrics.TASK_RESULTS_STALE_TOTAL_NAME
        );
        assertEquals(
                "taskflow_task_results_duplicate_total",
                SchedulerMetrics.TASK_RESULTS_DUPLICATE_TOTAL_NAME
        );
        assertEquals(
                "taskflow_assignment_generations_total",
                SchedulerMetrics.ASSIGNMENT_GENERATIONS_TOTAL_NAME
        );
        assertEquals(
                "taskflow_payload_integrity_failures_total",
                SchedulerMetrics.PAYLOAD_INTEGRITY_FAILURES_TOTAL_NAME
        );

        SchedulerMetrics metrics = new SchedulerMetrics();
        metrics.recordAssignmentGeneration(12L);
        metrics.recordAssignmentGeneration(18L);
        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.COMMITTED);
        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT);
        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED);
        metrics.recordPayloadIntegrityFailure();

        SchedulerMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(2L, snapshot.assignmentGenerationsTotal());
        assertEquals(1L, snapshot.taskResultsCommittedTotal());
        assertEquals(1L, snapshot.taskResultsStaleTotal());
        assertEquals(1L, snapshot.taskResultsDuplicateTotal());
        assertEquals(1L, snapshot.payloadIntegrityFailuresTotal());
        assertEquals(15.0, snapshot.avgDispatchLatencyMs());

        assertEquals(snapshot.assignmentGenerationsTotal(), snapshot.assignedCount());
        assertEquals(snapshot.taskResultsCommittedTotal(), snapshot.successCount());
        assertEquals(snapshot.taskResultsStaleTotal(), snapshot.staleResultCount());
        assertEquals(snapshot.taskResultsDuplicateTotal(), snapshot.duplicateResultCount());
    }
}
