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
                "taskflow_task_assignment_generations_total",
                SchedulerMetrics.TASK_ASSIGNMENT_GENERATIONS_TOTAL_NAME
        );
        assertEquals(
                "taskflow_payload_integrity_failures_total",
                SchedulerMetrics.PAYLOAD_INTEGRITY_FAILURES_TOTAL_NAME
        );

        SchedulerMetrics metrics = new SchedulerMetrics();
        metrics.recordJobAccepted();
        metrics.recordJobTerminal(true);
        metrics.recordJobTerminal(false);
        metrics.recordAssignmentGeneration(12L);
        metrics.recordAssignmentGeneration(18L);
        metrics.recordRetry();
        metrics.recordLeaseExpiration();
        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.COMMITTED, 2_500L);
        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT);
        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED);
        metrics.recordRecoveryDuration(125L);
        metrics.recordPayloadIntegrityFailure();
        metrics.setQueueDepth(3L);
        metrics.setPendingTasks(4L);
        metrics.setDueDeadlines(5L);
        metrics.setWorkerCapacityUsed(6L);

        SchedulerMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(1L, snapshot.jobsAcceptedTotal());
        assertEquals(1L, snapshot.jobsCompletedTotal());
        assertEquals(1L, snapshot.jobsFailedTotal());
        assertEquals(2L, snapshot.tasksAssignedTotal());
        assertEquals(2L, snapshot.assignmentGenerationsTotal());
        assertEquals(1L, snapshot.retryCount());
        assertEquals(1L, snapshot.taskLeaseExpirationsTotal());
        assertEquals(1L, snapshot.taskResultsCommittedTotal());
        assertEquals(1L, snapshot.taskResultsStaleTotal());
        assertEquals(1L, snapshot.taskResultsDuplicateTotal());
        assertEquals(1L, snapshot.payloadIntegrityFailuresTotal());
        assertEquals(15.0, snapshot.avgDispatchLatencyMs());
        assertEquals(3L, snapshot.queueDepth());
        assertEquals(4L, snapshot.pendingTasks());
        assertEquals(5L, snapshot.dueDeadlines());
        assertEquals(6L, snapshot.workerCapacityUsed());
        assertEquals(2L, snapshot.assignmentLatencySeconds().count());
        assertEquals(0.03, snapshot.assignmentLatencySeconds().sumSeconds());
        assertEquals(1L, snapshot.resultCommitLatencySeconds().count());
        assertEquals(2.5, snapshot.resultCommitLatencySeconds().sumSeconds());
        assertEquals(1L, snapshot.recoveryDurationSeconds().count());
        assertEquals(0.125, snapshot.recoveryDurationSeconds().sumSeconds());

        assertEquals(snapshot.assignmentGenerationsTotal(), snapshot.assignedCount());
        assertEquals(snapshot.taskResultsCommittedTotal(), snapshot.successCount());
        assertEquals(snapshot.taskResultsStaleTotal(), snapshot.staleResultCount());
        assertEquals(snapshot.taskResultsDuplicateTotal(), snapshot.duplicateResultCount());
    }
}
