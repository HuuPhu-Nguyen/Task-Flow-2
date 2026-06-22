package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.db.JobStateStore;

final class CoordinatorStartupRecovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorStartupRecovery.class);

    private CoordinatorStartupRecovery() {
    }

    static boolean reconcileAbandonedJobs(JobStateStore store) {
        return reconcileAbandonedJobs(store, System.currentTimeMillis());
    }

    static boolean reconcileAbandonedJobs(JobStateStore store, long completedAt) {
        int failedJobs = store.markRunningJobsFailedOnStartup(completedAt);
        if (failedJobs > 0) {
            LOGGER.warn("event=abandoned_jobs_marked_failed count={}", failedJobs);
            return true;
        }
        if (failedJobs < 0) {
            LOGGER.error("event=abandoned_job_reconciliation_failed");
            return false;
        }
        return true;
    }
}
