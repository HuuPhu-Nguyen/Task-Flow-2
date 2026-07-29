package server.recovery;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecoveryExperimentConfigTest {
    @Test
    void reportGradeDefaultsCoverEveryRequiredMeasurement() {
        RecoveryExperimentConfig config =
                RecoveryExperimentConfig.fromSystemProperties();

        config.requireReportGrade();
        assertEquals(4, config.jobCount(1_000));
        assertEquals(40, config.jobCount(10_000));
        assertEquals(400, config.jobCount(100_000));
        assertEquals(250, config.tasksInJob(10_000, 39));
        assertEquals(250, config.tasksInJob(100_000, 399));
    }

    @Test
    void partialFinalJobRetainsTheExactTaskCardinality() {
        RecoveryExperimentConfig config = configuration(10, 13, 5);

        assertEquals(3, config.jobCount(13));
        assertEquals(5, config.tasksInJob(13, 0));
        assertEquals(5, config.tasksInJob(13, 1));
        assertEquals(3, config.tasksInJob(13, 2));
        assertThrows(
                IllegalArgumentException.class,
                () -> config.tasksInJob(13, 3)
        );
    }

    @Test
    void invalidBoundsAndNonReportInputsAreRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> configuration(0, 13, 5)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> configuration(14, 13, 5)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> configuration(10, 13, 0)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> configuration(10, 13, 5).requireReportGrade()
        );
    }

    @Test
    void metricConversionsRejectInvalidDurations() {
        assertEquals(1.5D, RecoveryMetrics.nanosToMillis(1_500_000L));
        assertEquals(
                2_000.0D,
                RecoveryMetrics.ratePerSecond(2L, 1_000_000L)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> RecoveryMetrics.nanosToMillis(-1L)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> RecoveryMetrics.ratePerSecond(1L, 0L)
        );
    }

    private static RecoveryExperimentConfig configuration(
            int smallTasks,
            int largeTasks,
            int tasksPerJob
    ) {
        return new RecoveryExperimentConfig(
                Path.of("target/recovery/test"),
                5,
                smallTasks,
                largeTasks,
                tasksPerJob,
                10,
                10,
                1_000L,
                100L,
                10,
                60L
        );
    }
}
