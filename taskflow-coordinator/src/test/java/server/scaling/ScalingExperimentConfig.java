package server.scaling;

import java.nio.file.Path;
import java.util.Set;

/**
 * Bounded configuration for the opt-in TF-0707 scaling matrix.
 */
public record ScalingExperimentConfig(
        int workerCount,
        int taskCount,
        int warmupTaskCount,
        int tasksPerJob,
        int workUnitsPerTask,
        int payloadBytes,
        long completionTimeoutSeconds,
        long sampleIntervalMillis,
        Path outputDirectory
) {
    public static final Set<Integer> WORKER_MATRIX = Set.of(1, 2, 4, 8);
    public static final int REPORT_TASK_MINIMUM = 10_000;
    public static final int MAX_TASK_COUNT = 100_000;
    public static final int MAX_TASKS_PER_JOB = 1_000;
    public static final int MAX_WORK_UNITS = 1_000_000;
    public static final int MAX_PAYLOAD_BYTES = 32 * 1_024;
    public static final long MAX_TIMEOUT_SECONDS = 3_600L;
    public static final long MIN_SAMPLE_INTERVAL_MILLIS = 25L;
    public static final long MAX_SAMPLE_INTERVAL_MILLIS = 1_000L;

    public ScalingExperimentConfig {
        if (!WORKER_MATRIX.contains(workerCount)) {
            throw new IllegalArgumentException(
                    "workerCount must be one of "
                            + WORKER_MATRIX.stream().sorted().toList()
            );
        }
        requireRange(taskCount, 1, MAX_TASK_COUNT, "taskCount");
        requireRange(warmupTaskCount, 1, taskCount, "warmupTaskCount");
        requireRange(tasksPerJob, 1, MAX_TASKS_PER_JOB, "tasksPerJob");
        requireRange(workUnitsPerTask, 1, MAX_WORK_UNITS, "workUnitsPerTask");
        requireRange(payloadBytes, 16, MAX_PAYLOAD_BYTES, "payloadBytes");
        requireRange(
                completionTimeoutSeconds,
                1L,
                MAX_TIMEOUT_SECONDS,
                "completionTimeoutSeconds"
        );
        requireRange(
                sampleIntervalMillis,
                MIN_SAMPLE_INTERVAL_MILLIS,
                MAX_SAMPLE_INTERVAL_MILLIS,
                "sampleIntervalMillis"
        );
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required.");
        }
    }

    public static ScalingExperimentConfig fromSystemProperties() {
        int workers = integer("taskflow.scaling.workers", 1);
        return new ScalingExperimentConfig(
                workers,
                integer("taskflow.scaling.tasks", REPORT_TASK_MINIMUM),
                integer("taskflow.scaling.warmupTasks", 1_000),
                integer("taskflow.scaling.tasksPerJob", 250),
                integer("taskflow.scaling.workUnits", 300_000),
                integer("taskflow.scaling.payloadBytes", 128),
                longValue("taskflow.scaling.timeoutSeconds", 900L),
                longValue("taskflow.scaling.sampleIntervalMillis", 100L),
                Path.of(System.getProperty(
                        "taskflow.scaling.output",
                        "target/scaling/workers-" + workers
                ))
        );
    }

    public int measuredJobCount() {
        return jobCount(taskCount);
    }

    public int warmupJobCount() {
        return jobCount(warmupTaskCount);
    }

    public int measuredTasksInJob(int jobIndex) {
        return tasksInJob(taskCount, jobIndex);
    }

    public int warmupTasksInJob(int jobIndex) {
        return tasksInJob(warmupTaskCount, jobIndex);
    }

    public int maximumSampleCount() {
        long samples = Math.ceilDiv(
                Math.multiplyExact(completionTimeoutSeconds, 1_000L),
                sampleIntervalMillis
        ) + 10L;
        return Math.toIntExact(samples);
    }

    public int maximumWriteSampleCount() {
        return Math.toIntExact(
                Math.multiplyExact((long) taskCount + warmupTaskCount, 4L)
                        + measuredJobCount()
                        + warmupJobCount()
                        + workerCount
                        + 100L
        );
    }

    public void requireReportGrade() {
        if (taskCount < REPORT_TASK_MINIMUM) {
            throw new IllegalStateException(
                    "Report-grade scaling requires at least "
                            + REPORT_TASK_MINIMUM
                            + " measured tasks."
            );
        }
    }

    private int jobCount(int tasks) {
        return Math.ceilDiv(tasks, tasksPerJob);
    }

    private int tasksInJob(int tasks, int jobIndex) {
        int count = jobCount(tasks);
        if (jobIndex < 0 || jobIndex >= count) {
            throw new IllegalArgumentException(
                    "jobIndex is outside the workload."
            );
        }
        int consumed = Math.multiplyExact(jobIndex, tasksPerJob);
        return Math.min(tasksPerJob, tasks - consumed);
    }

    private static int integer(String property, int fallback) {
        return Integer.parseInt(System.getProperty(
                property,
                Integer.toString(fallback)
        ));
    }

    private static long longValue(String property, long fallback) {
        return Long.parseLong(System.getProperty(
                property,
                Long.toString(fallback)
        ));
    }

    private static void requireRange(
            long value,
            long minimum,
            long maximum,
            String name
    ) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    name + " must be in [" + minimum + ", " + maximum + "]."
            );
        }
    }
}
