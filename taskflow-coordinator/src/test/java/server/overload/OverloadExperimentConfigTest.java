package server.overload;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OverloadExperimentConfigTest {
    @Test
    void reportDefaultsFixTheWorkloadAndThresholds() {
        OverloadExperimentConfig config =
                OverloadExperimentConfig.fromSystemProperties();

        config.requireReportGrade();
        assertEquals(1_000L, config.totalFloodSubmissions());
        assertEquals(16L * 1024L * 1024L, config.heapPlateauSpanBytes());
        assertEquals(128L * 1024L * 1024L, config.heapCeilingBytes());
    }

    @Test
    void plateauUsesOnlyTheFinalThreeNonNegativeSamples() {
        List<Long> samples = List.of(1_000L, 9_000L, 8_000L, 10_000L);

        assertEquals(2_000L, OverloadMetrics.plateauSpan(samples));
        assertEquals(10_000L, OverloadMetrics.plateauMaximum(samples));
        assertThrows(
                IllegalArgumentException.class,
                () -> OverloadMetrics.plateauSpan(List.of(1L, 2L))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> OverloadMetrics.plateauMaximum(List.of(1L, -1L, 2L))
        );
    }

    @Test
    void invalidAndNonReportInputsAreRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> configuration(2, 20)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> configuration(3, 9)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> configuration(3, 20).requireReportGrade()
        );
    }

    private static OverloadExperimentConfig configuration(int waves, int submissions) {
        return new OverloadExperimentConfig(
                Path.of("target/overload/test"),
                waves,
                submissions,
                2,
                4,
                4,
                200L,
                30L,
                1024L,
                2048L
        );
    }
}
