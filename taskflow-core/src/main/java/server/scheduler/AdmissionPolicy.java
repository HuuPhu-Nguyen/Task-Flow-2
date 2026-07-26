package server.scheduler;

import protocol.AdmissionRejection;

/**
 * Pure dynamic admission decision for scheduler-owned active state and the
 * durable pending outbox.
 */
final class AdmissionPolicy {
    private AdmissionPolicy() {
    }

    static Decision evaluate(long activeJobs,
                             long activeTasks,
                             long candidateTasks,
                             long pendingOutboxRows,
                             boolean pendingOutboxCountAvailable,
                             SchedulerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Scheduler config is required.");
        }
        if (activeJobs < 0L || activeTasks < 0L || candidateTasks < 0L
                || pendingOutboxRows < 0L) {
            return Decision.storageFailure();
        }
        if (!pendingOutboxCountAvailable) {
            return Decision.storageFailure();
        }

        long resultingJobs = saturatingAdd(activeJobs, 1L);
        if (resultingJobs > config.maxActiveJobs()) {
            return Decision.rejected(new AdmissionRejection(
                    AdmissionRejection.Limit.MAX_ACTIVE_JOBS,
                    config.maxActiveJobs(),
                    resultingJobs
            ));
        }

        long resultingTasks = saturatingAdd(activeTasks, candidateTasks);
        if (resultingTasks > config.maxActiveTasks()) {
            return Decision.rejected(new AdmissionRejection(
                    AdmissionRejection.Limit.MAX_ACTIVE_TASKS,
                    config.maxActiveTasks(),
                    resultingTasks
            ));
        }

        if (pendingOutboxRows >= config.maxPendingOutboxRows()) {
            return Decision.rejected(new AdmissionRejection(
                    AdmissionRejection.Limit.MAX_PENDING_OUTBOX_ROWS,
                    config.maxPendingOutboxRows(),
                    pendingOutboxRows
            ));
        }
        return Decision.allowed();
    }

    private static long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    enum Outcome {
        ALLOWED,
        LIMIT_EXCEEDED,
        STORAGE_FAILURE
    }

    record Decision(Outcome outcome, AdmissionRejection rejection) {
        Decision {
            if (outcome == null) {
                outcome = Outcome.STORAGE_FAILURE;
            }
            if (outcome == Outcome.LIMIT_EXCEEDED && rejection == null) {
                throw new IllegalArgumentException(
                        "Limit-exceeded admission decision requires rejection detail."
                );
            }
            if (outcome != Outcome.LIMIT_EXCEEDED && rejection != null) {
                throw new IllegalArgumentException(
                        "Non-limit admission decision cannot carry rejection detail."
                );
            }
        }

        static Decision allowed() {
            return new Decision(Outcome.ALLOWED, null);
        }

        static Decision rejected(AdmissionRejection rejection) {
            return new Decision(Outcome.LIMIT_EXCEEDED, rejection);
        }

        static Decision storageFailure() {
            return new Decision(Outcome.STORAGE_FAILURE, null);
        }

        boolean allowedDecision() {
            return outcome == Outcome.ALLOWED;
        }
    }
}
