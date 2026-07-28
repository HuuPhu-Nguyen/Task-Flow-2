package server.scaling;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScalingExperimentConfigTest {
    @TempDir
    Path tempDirectory;

    @Test
    void fixedMatrixAndDerivedBoundsAreDeterministic() {
        for (int workers : List.of(1, 2, 4, 8)) {
            ScalingExperimentConfig config = config(
                    workers,
                    10_001,
                    1_001,
                    250
            );
            assertEquals(41, config.measuredJobCount());
            assertEquals(5, config.warmupJobCount());
            assertEquals(250, config.measuredTasksInJob(0));
            assertEquals(1, config.measuredTasksInJob(40));
            assertEquals(1, config.warmupTasksInJob(4));
            assertEquals(9_010, config.maximumSampleCount());
            assertEquals(
                    (10_001 + 1_001) * 4 + 41 + 5 + workers + 100,
                    config.maximumWriteSampleCount()
            );
            config.requireReportGrade();
        }
    }

    @Test
    void rejectsNonMatrixWorkersAndUnboundedInputs() {
        assertThrows(
                IllegalArgumentException.class,
                () -> config(3, 10_000, 1_000, 250)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> config(1, 100_001, 1_000, 250)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> config(1, 10_000, 10_001, 250)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> config(1, 10_000, 1_000, 1_001)
        );
        assertThrows(
                IllegalStateException.class,
                () -> config(1, 9_999, 1_000, 250).requireReportGrade()
        );
    }

    @Test
    void percentileAndEfficiencyCalculationsUseDocumentedDefinitions() {
        List<Long> samples = List.of(50L, 10L, 40L, 20L, 30L);
        assertEquals(30L, ScalingMetrics.nearestRank(samples, 0.50D));
        assertEquals(50L, ScalingMetrics.nearestRank(samples, 0.95D));
        assertEquals(50L, ScalingMetrics.nearestRank(samples, 0.99D));
        assertEquals(
                0.625D,
                ScalingMetrics.parallelEfficiency(
                        100.0D,
                        250.0D,
                        4
                )
        );
        assertEquals(1.5D, ScalingMetrics.nanosToMillis(1_500_000L));
        assertThrows(
                IllegalArgumentException.class,
                () -> ScalingMetrics.nearestRank(List.of(), 0.5D)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ScalingMetrics.parallelEfficiency(0.0D, 1.0D, 1)
        );
    }

    private ScalingExperimentConfig config(
            int workers,
            int tasks,
            int warmupTasks,
            int tasksPerJob
    ) {
        return new ScalingExperimentConfig(
                workers,
                tasks,
                warmupTasks,
                tasksPerJob,
                64,
                128,
                900L,
                100L,
                tempDirectory.resolve("workers-" + workers)
        );
    }
}
