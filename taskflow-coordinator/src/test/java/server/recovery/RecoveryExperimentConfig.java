package server.recovery;

import java.nio.file.Path;

/**
 * Bounded inputs for the opt-in TF-0708 recovery experiment.
 */
public record RecoveryExperimentConfig(
        Path outputDirectory,
        int coordinatorRestartTaskCount,
        int smallPersistedTaskCount,
        int largePersistedTaskCount,
        int tasksPerJob,
        int outboxMessageCount,
        int orphanObjectCount,
        long workerFailureTimeoutMillis,
        long taskLeaseMillis,
        int batchSize,
        long completionTimeoutSeconds
) {
    public static final int REPORT_SMALL_TASKS = 10_000;
    public static final int REPORT_LARGE_TASKS = 100_000;
    public static final int REPORT_COORDINATOR_RESTART_TASKS = 1_000;
    public static final int REPORT_TASKS_PER_JOB = 250;
    public static final int REPORT_OUTBOX_MESSAGES = 500;
    public static final int REPORT_ORPHAN_OBJECTS = 1_000;
    public static final long REPORT_WORKER_TIMEOUT_MILLIS = 90_000L;
    public static final long REPORT_TASK_LEASE_MILLIS = 1_000L;
    public static final int REPORT_BATCH_SIZE = 100;
    public static final long REPORT_COMPLETION_TIMEOUT_SECONDS = 900L;

    public RecoveryExperimentConfig {
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required.");
        }
        requireRange(
                coordinatorRestartTaskCount,
                1,
                REPORT_SMALL_TASKS,
                "coordinatorRestartTaskCount"
        );
        requireRange(
                smallPersistedTaskCount,
                1,
                REPORT_LARGE_TASKS,
                "smallPersistedTaskCount"
        );
        requireRange(
                largePersistedTaskCount,
                smallPersistedTaskCount,
                200_000,
                "largePersistedTaskCount"
        );
        requireRange(tasksPerJob, 1, 1_000, "tasksPerJob");
        requireRange(outboxMessageCount, 1, 20_000, "outboxMessageCount");
        requireRange(orphanObjectCount, 2, 5_000, "orphanObjectCount");
        requireRange(
                workerFailureTimeoutMillis,
                1L,
                120_000L,
                "workerFailureTimeoutMillis"
        );
        requireRange(taskLeaseMillis, 1L, 120_000L, "taskLeaseMillis");
        requireRange(batchSize, 2, 1_000, "batchSize");
        requireRange(
                completionTimeoutSeconds,
                10L,
                3_600L,
                "completionTimeoutSeconds"
        );
    }

    public static RecoveryExperimentConfig fromSystemProperties() {
        return new RecoveryExperimentConfig(
                Path.of(System.getProperty(
                        "taskflow.recovery.outputDirectory",
                        "target/recovery/run"
                )),
                integer(
                        "taskflow.recovery.coordinatorRestartTasks",
                        REPORT_COORDINATOR_RESTART_TASKS
                ),
                integer(
                        "taskflow.recovery.smallPersistedTasks",
                        REPORT_SMALL_TASKS
                ),
                integer(
                        "taskflow.recovery.largePersistedTasks",
                        REPORT_LARGE_TASKS
                ),
                integer(
                        "taskflow.recovery.tasksPerJob",
                        REPORT_TASKS_PER_JOB
                ),
                integer(
                        "taskflow.recovery.outboxMessages",
                        REPORT_OUTBOX_MESSAGES
                ),
                integer(
                        "taskflow.recovery.orphanObjects",
                        REPORT_ORPHAN_OBJECTS
                ),
                longValue(
                        "taskflow.recovery.workerFailureTimeoutMillis",
                        REPORT_WORKER_TIMEOUT_MILLIS
                ),
                longValue(
                        "taskflow.recovery.taskLeaseMillis",
                        REPORT_TASK_LEASE_MILLIS
                ),
                integer(
                        "taskflow.recovery.batchSize",
                        REPORT_BATCH_SIZE
                ),
                longValue(
                        "taskflow.recovery.completionTimeoutSeconds",
                        REPORT_COMPLETION_TIMEOUT_SECONDS
                )
        );
    }

    public int jobCount(int taskCount) {
        if (taskCount < 1) {
            throw new IllegalArgumentException("taskCount must be positive.");
        }
        return Math.floorDiv(taskCount - 1, tasksPerJob) + 1;
    }

    public int tasksInJob(int taskCount, int jobIndex) {
        int jobs = jobCount(taskCount);
        if (jobIndex < 0 || jobIndex >= jobs) {
            throw new IllegalArgumentException("jobIndex is outside the fixture.");
        }
        return Math.min(tasksPerJob, taskCount - (jobIndex * tasksPerJob));
    }

    public void requireReportGrade() {
        if (coordinatorRestartTaskCount
                != REPORT_COORDINATOR_RESTART_TASKS
                || smallPersistedTaskCount != REPORT_SMALL_TASKS
                || largePersistedTaskCount != REPORT_LARGE_TASKS
                || tasksPerJob != REPORT_TASKS_PER_JOB
                || outboxMessageCount != REPORT_OUTBOX_MESSAGES
                || orphanObjectCount != REPORT_ORPHAN_OBJECTS
                || workerFailureTimeoutMillis
                != REPORT_WORKER_TIMEOUT_MILLIS
                || taskLeaseMillis != REPORT_TASK_LEASE_MILLIS
                || batchSize != REPORT_BATCH_SIZE
                || completionTimeoutSeconds
                != REPORT_COMPLETION_TIMEOUT_SECONDS) {
            throw new IllegalArgumentException(
                    "Report-grade recovery execution requires the fixed "
                            + "TF-0708 workload and timeout constants."
            );
        }
    }

    private static int integer(String property, int fallback) {
        String value = System.getProperty(property);
        return value == null || value.isBlank()
                ? fallback
                : Integer.parseInt(value);
    }

    private static long longValue(String property, long fallback) {
        String value = System.getProperty(property);
        return value == null || value.isBlank()
                ? fallback
                : Long.parseLong(value);
    }

    private static void requireRange(
            long value,
            long minimum,
            long maximum,
            String field
    ) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    field + " must be in [" + minimum + ", " + maximum + "]."
            );
        }
    }
}
