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

    String failureReason(String operation) {
        return "Persistence write failed during " + operation + ".";
    }

    String taskFailureOperation(TaskUnit.FailureOutcome outcome) {
        return outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE
                ? "markTaskFailed"
                : "markTaskRetried";
    }
}
