package server.chaos;

import java.nio.file.Path;

/**
 * Bounded, seed-driven configuration for the opt-in TF-0706 experiment.
 */
public record CorrectnessChaosConfig(
        long seed,
        int taskCount,
        int tasksPerJob,
        int workerCount,
        int duplicateBasisPoints,
        int delayedResultCount,
        int workerTerminationCount,
        int brokerRestartAfterCompletions,
        int coordinatorRestartAfterCompletions,
        long leaseMillis,
        long delayedResultMillis,
        long completionTimeoutSeconds,
        Path outputDirectory
) {
    public static final long DEFAULT_SEED = 0x3520706L;
    public static final int REPORT_TASK_MINIMUM = 100_000;
    public static final int REQUIRED_DUPLICATE_BASIS_POINTS = 500;
    private static final int BASIS_POINTS = 10_000;

    public CorrectnessChaosConfig {
        requirePositive(taskCount, "taskCount");
        requirePositive(tasksPerJob, "tasksPerJob");
        requirePositive(workerCount, "workerCount");
        requireRange(
                duplicateBasisPoints,
                0,
                BASIS_POINTS,
                "duplicateBasisPoints"
        );
        requireRange(delayedResultCount, 1, taskCount, "delayedResultCount");
        requireRange(
                workerTerminationCount,
                1,
                taskCount,
                "workerTerminationCount"
        );
        requireRange(
                brokerRestartAfterCompletions,
                1,
                taskCount - 1,
                "brokerRestartAfterCompletions"
        );
        requireRange(
                coordinatorRestartAfterCompletions,
                1,
                taskCount - 1,
                "coordinatorRestartAfterCompletions"
        );
        requirePositive(leaseMillis, "leaseMillis");
        if (delayedResultMillis <= leaseMillis) {
            throw new IllegalArgumentException(
                    "delayedResultMillis must be greater than leaseMillis."
            );
        }
        requirePositive(completionTimeoutSeconds, "completionTimeoutSeconds");
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required.");
        }
    }

    public static CorrectnessChaosConfig fromSystemProperties() {
        int tasks = integer("taskflow.chaos.tasks", REPORT_TASK_MINIMUM);
        return new CorrectnessChaosConfig(
                longValue("taskflow.chaos.seed", DEFAULT_SEED),
                tasks,
                integer("taskflow.chaos.tasksPerJob", 250),
                integer("taskflow.chaos.workers", 4),
                integer(
                        "taskflow.chaos.duplicateBasisPoints",
                        REQUIRED_DUPLICATE_BASIS_POINTS
                ),
                integer("taskflow.chaos.delayedResults", Math.max(1, tasks / 100)),
                integer(
                        "taskflow.chaos.workerTerminations",
                        Math.max(1, tasks / 10_000)
                ),
                integer(
                        "taskflow.chaos.brokerRestartAfter",
                        Math.max(1, tasks / 3)
                ),
                integer(
                        "taskflow.chaos.coordinatorRestartAfter",
                        Math.max(1, tasks * 2 / 3)
                ),
                longValue("taskflow.chaos.leaseMillis", 2_000L),
                longValue("taskflow.chaos.delayedResultMillis", 2_500L),
                longValue("taskflow.chaos.timeoutSeconds", 1_800L),
                Path.of(System.getProperty(
                        "taskflow.chaos.output",
                        "target/correctness-chaos"
                ))
        );
    }

    public int jobCount() {
        return (taskCount + tasksPerJob - 1) / tasksPerJob;
    }

    public int tasksInJob(int jobIndex) {
        if (jobIndex < 0 || jobIndex >= jobCount()) {
            throw new IllegalArgumentException("jobIndex is outside the workload.");
        }
        int consumed = jobIndex * tasksPerJob;
        return Math.min(tasksPerJob, taskCount - consumed);
    }

    public void requireReportGrade() {
        if (taskCount < REPORT_TASK_MINIMUM) {
            throw new IllegalStateException(
                    "Report-grade chaos requires at least "
                            + REPORT_TASK_MINIMUM + " tasks."
            );
        }
        if (duplicateBasisPoints != REQUIRED_DUPLICATE_BASIS_POINTS) {
            throw new IllegalStateException(
                    "Report-grade chaos requires exactly 5% duplicate "
                            + "assignment/result publication."
            );
        }
    }

    public boolean duplicateAssignment(long assignmentOrdinal) {
        return duplicateSelected("assignment-duplicate", assignmentOrdinal);
    }

    public boolean duplicateResult(long resultOrdinal) {
        return duplicateSelected("result-duplicate", resultOrdinal);
    }

    public boolean delayResult(long resultOrdinal) {
        return exactSelection(
                "result-delay",
                resultOrdinal,
                delayedResultCount
        );
    }

    public boolean terminateWorker(long assignmentOrdinal) {
        return exactSelection(
                "worker-termination",
                assignmentOrdinal,
                workerTerminationCount
        );
    }

    private boolean exactSelection(
            String family,
            long ordinal,
            int selectedCount
    ) {
        if (ordinal < 0L || ordinal >= taskCount) {
            return false;
        }
        long offset = Integer.toUnsignedLong(rank(family, seed)) % taskCount;
        long multiplier = coprimeMultiplier(family);
        long shifted = (multiplier * ordinal + offset) % taskCount;
        return shifted < selectedCount;
    }

    private long coprimeMultiplier(String family) {
        long candidate = 1L + Integer.toUnsignedLong(
                rank(family + "-permutation", seed)
        ) % Math.max(1, taskCount - 1);
        while (greatestCommonDivisor(candidate, taskCount) != 1L) {
            candidate++;
            if (candidate >= taskCount) {
                candidate = 1L;
            }
        }
        return candidate;
    }

    private static long greatestCommonDivisor(long left, long right) {
        while (right != 0L) {
            long remainder = left % right;
            left = right;
            right = remainder;
        }
        return left;
    }

    private boolean duplicateSelected(String family, long ordinal) {
        if (duplicateBasisPoints == REQUIRED_DUPLICATE_BASIS_POINTS) {
            long offset = Integer.toUnsignedLong(rank(family, seed));
            return Long.remainderUnsigned(ordinal + offset, 20L) == 0L;
        }
        return rank(family, ordinal) < duplicateBasisPoints;
    }

    private int rank(String family, long ordinal) {
        long value = seed ^ ((long) family.hashCode() << 32) ^ ordinal;
        value ^= value >>> 30;
        value *= 0xbf58476d1ce4e5b9L;
        value ^= value >>> 27;
        value *= 0x94d049bb133111ebL;
        value ^= value >>> 31;
        return (int) Long.remainderUnsigned(value, BASIS_POINTS);
    }

    private static int integer(String property, int fallback) {
        return Integer.parseInt(System.getProperty(
                property,
                Integer.toString(fallback)
        ));
    }

    private static long longValue(String property, long fallback) {
        return Long.decode(System.getProperty(
                property,
                Long.toString(fallback)
        ));
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " must be positive.");
        }
    }

    private static void requireRange(
            int value,
            int minimum,
            int maximum,
            String name
    ) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    name + " must be in [" + minimum + ", " + maximum + "]."
            );
        }
    }
}
