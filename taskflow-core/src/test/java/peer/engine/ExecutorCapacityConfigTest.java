package peer.engine;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExecutorCapacityConfigTest {
    @Test
    void defaultsUseAvailableProcessorsAndExecutionPoolSize() {
        ExecutorCapacityConfig config = ExecutorCapacityConfig.fromEnvironment(
                Map.of(),
                Set.of("TEXT_ANALYSIS"),
                4
        );

        assertEquals(
                Math.max(1, Runtime.getRuntime().availableProcessors()),
                config.totalCapacityUnits()
        );
        assertEquals(4, config.maxConcurrencyByTaskType().get("TEXT_ANALYSIS"));
    }

    @Test
    void parsesExplicitTotalAndNormalizedTypeOverrides() {
        ExecutorCapacityConfig config = ExecutorCapacityConfig.fromEnvironment(
                Map.of(
                        ExecutorCapacityConfig.TOTAL_CAPACITY_UNITS_ENV, "8",
                        ExecutorCapacityConfig.TYPE_CONCURRENCY_LIMITS_ENV,
                        " text_analysis : 3,video_transcoding:1"
                ),
                Set.of("TEXT_ANALYSIS", "VIDEO_TRANSCODING"),
                4
        );

        assertEquals(8, config.totalCapacityUnits());
        assertEquals(3, config.maxConcurrencyByTaskType().get("TEXT_ANALYSIS"));
        assertEquals(1, config.maxConcurrencyByTaskType().get("VIDEO_TRANSCODING"));
    }

    @Test
    void requesterOnlyCapacityIgnoresExecutorOverrides() {
        ExecutorCapacityConfig config = ExecutorCapacityConfig.fromEnvironment(
                Map.of(ExecutorCapacityConfig.TOTAL_CAPACITY_UNITS_ENV, "not-a-number"),
                Set.of(),
                2
        );

        assertEquals(0, config.totalCapacityUnits());
        assertEquals(Map.of(), config.maxConcurrencyByTaskType());
    }

    @Test
    void rejectsInvalidBoundsAndConcurrencySyntax() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ExecutorCapacityConfig.fromEnvironment(
                        Map.of(ExecutorCapacityConfig.TOTAL_CAPACITY_UNITS_ENV, "0"),
                        Set.of("TEXT_ANALYSIS"),
                        4
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExecutorCapacityConfig.fromEnvironment(
                        Map.of(
                                ExecutorCapacityConfig.TYPE_CONCURRENCY_LIMITS_ENV,
                                "UNKNOWN:1"
                        ),
                        Set.of("TEXT_ANALYSIS"),
                        4
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExecutorCapacityConfig.fromEnvironment(
                        Map.of(
                                ExecutorCapacityConfig.TYPE_CONCURRENCY_LIMITS_ENV,
                                "TEXT_ANALYSIS:5"
                        ),
                        Set.of("TEXT_ANALYSIS"),
                        4
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExecutorCapacityConfig.fromEnvironment(
                        Map.of(
                                ExecutorCapacityConfig.TYPE_CONCURRENCY_LIMITS_ENV,
                                "TEXT_ANALYSIS:1,TEXT_ANALYSIS:2"
                        ),
                        Set.of("TEXT_ANALYSIS"),
                        4
                )
        );
    }
}
