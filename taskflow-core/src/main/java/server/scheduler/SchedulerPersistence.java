package server.scheduler;

import server.db.JobStateStore;
import server.job.TaskUnit;

final class SchedulerPersistence {
    private final JobStateStore store;
    private final SchedulerEventLog events;

    SchedulerPersistence(JobStateStore store, SchedulerEventLog events) {
        this.store = store;
        this.events = events;
    }

    JobStateStore store() {
        return store;
    }

    boolean enabled() {
        return store != null;
    }

    boolean record(String operation, String jobId, String taskId, boolean success) {
        if (success) {
            return true;
        }
        events.error("scheduler_persistence_failed", events.fields(
                "operation", operation,
                "job_id", jobId,
                "task_id", taskId
        ));
        return false;
    }

    JobStateStore.DurableTransitionOutcome record(
            String operation,
            String jobId,
            String taskId,
            JobStateStore.DurableTransitionOutcome outcome) {
        JobStateStore.DurableTransitionOutcome normalized = outcome == null
                ? JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE
                : outcome;
        if (normalized == JobStateStore.DurableTransitionOutcome.COMMITTED) {
            return normalized;
        }
        if (normalized == JobStateStore.DurableTransitionOutcome.ALREADY_APPLIED) {
            events.info("scheduler_durable_transition_replayed", events.fields(
                    "operation", operation,
                    "job_id", jobId,
                    "task_id", taskId,
                    "outcome", normalized
            ));
            return normalized;
        }
        if (normalized == JobStateStore.DurableTransitionOutcome.STALE_STATE) {
            events.info("scheduler_durable_transition_rejected", events.fields(
                    "operation", operation,
                    "job_id", jobId,
                    "task_id", taskId,
                    "outcome", normalized
            ));
            return normalized;
        }
        events.error("scheduler_persistence_failed", events.fields(
                "operation", operation,
                "job_id", jobId,
                "task_id", taskId,
                "outcome", normalized
        ));
        return normalized;
    }

    String failureReason(String operation) {
        return "Persistence write failed during " + operation + ".";
    }

    String taskFailureOperation(TaskUnit.FailureOutcome outcome) {
        return outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE
                ? "markTaskFailed"
                : "markTaskRetried";
    }
}
