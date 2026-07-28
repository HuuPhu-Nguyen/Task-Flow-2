package server.chaos;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CorrectnessChaosConfigTest {
    @Test
    void reportGradeDefaultsFixRequiredWorkloadAndFailureRates() {
        CorrectnessChaosConfig config = config(100_000, 250, 500);

        config.requireReportGrade();

        assertEquals(400, config.jobCount());
        assertEquals(250, config.tasksInJob(0));
        assertEquals(250, config.tasksInJob(399));
        assertTrue(config.delayedResultMillis() > config.leaseMillis());
    }

    @Test
    void partialFinalJobStillAccountsForEveryConfiguredTask() {
        CorrectnessChaosConfig config = config(1_001, 250, 500);

        assertEquals(5, config.jobCount());
        assertEquals(250, config.tasksInJob(3));
        assertEquals(1, config.tasksInJob(4));
        assertEquals(
                1_001,
                IntStream.range(0, config.jobCount())
                        .map(config::tasksInJob)
                        .sum()
        );
    }

    @Test
    void seededScheduleIsStableAndSeparatesFailureFamilies() {
        CorrectnessChaosConfig first = config(100_000, 250, 500);
        CorrectnessChaosConfig second = config(100_000, 250, 500);

        for (int ordinal = 0; ordinal < 20_000; ordinal++) {
            assertEquals(
                    first.duplicateAssignment(ordinal),
                    second.duplicateAssignment(ordinal)
            );
            assertEquals(
                    first.duplicateResult(ordinal),
                    second.duplicateResult(ordinal)
            );
            assertEquals(first.delayResult(ordinal), second.delayResult(ordinal));
            assertEquals(
                    first.terminateWorker(ordinal),
                    second.terminateWorker(ordinal)
            );
        }

        assertTrue(IntStream.range(0, 20_000)
                .anyMatch(first::duplicateAssignment));
        assertTrue(IntStream.range(0, 20_000)
                .anyMatch(first::duplicateResult));
        assertTrue(IntStream.range(0, 20_000)
                .anyMatch(first::delayResult));
        assertTrue(IntStream.range(0, 20_000)
                .anyMatch(first::terminateWorker));
        assertTrue(IntStream.range(0, 20_000)
                .anyMatch(ordinal ->
                        first.duplicateAssignment(ordinal)
                                != first.duplicateResult(ordinal)));
        assertEquals(
                5_000,
                IntStream.range(0, 100_000)
                        .filter(first::duplicateAssignment)
                        .count()
        );
        assertEquals(
                5_000,
                IntStream.range(0, 100_000)
                        .filter(first::duplicateResult)
                        .count()
        );
        assertEquals(
                first.delayedResultCount(),
                IntStream.range(0, first.taskCount())
                        .filter(first::delayResult)
                        .count()
        );
        assertEquals(
                first.workerTerminationCount(),
                IntStream.range(0, first.taskCount())
                        .filter(first::terminateWorker)
                        .count()
        );
    }

    @Test
    void calibrationCannotBeReportedAsQueueEvidence() {
        CorrectnessChaosConfig tooSmall = config(10_000, 250, 500);
        CorrectnessChaosConfig wrongRate = config(100_000, 250, 499);

        assertThrows(IllegalStateException.class, tooSmall::requireReportGrade);
        assertThrows(IllegalStateException.class, wrongRate::requireReportGrade);
        assertFalse(tooSmall.taskCount()
                >= CorrectnessChaosConfig.REPORT_TASK_MINIMUM);
    }

    @Test
    void invalidBoundsFailBeforeInfrastructureStarts() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CorrectnessChaosConfig(
                        1L, 100, 10, 1, 500, 1, 1,
                        33, 66, 2_000L, 2_000L, 60L, Path.of("target")
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new CorrectnessChaosConfig(
                        1L, 100, 10, 0, 500, 1, 1,
                        33, 66, 2_000L, 2_500L, 60L, Path.of("target")
                )
        );
    }

    private static CorrectnessChaosConfig config(
            int taskCount,
            int tasksPerJob,
            int duplicateBasisPoints
    ) {
        return new CorrectnessChaosConfig(
                CorrectnessChaosConfig.DEFAULT_SEED,
                taskCount,
                tasksPerJob,
                4,
                duplicateBasisPoints,
                Math.max(1, taskCount / 100),
                Math.max(1, taskCount / 10_000),
                Math.max(1, taskCount / 3),
                Math.max(1, taskCount * 2 / 3),
                2_000L,
                2_500L,
                1_800L,
                Path.of("target/correctness-chaos")
        );
    }
}
