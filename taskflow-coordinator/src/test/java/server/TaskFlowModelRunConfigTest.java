package server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskFlowModelRunConfigTest {
    @AfterEach
    void clearProperties() {
        System.clearProperty(TaskFlowModelRunConfig.SEED_START_PROPERTY);
        System.clearProperty(TaskFlowModelRunConfig.SEED_COUNT_PROPERTY);
        System.clearProperty(TaskFlowModelRunConfig.STEPS_PER_SEED_PROPERTY);
    }

    @Test
    void defaultsPreserveCheckedInPushFastSeedsAndSteps() {
        TaskFlowModelRunConfig config =
                TaskFlowModelRunConfig.fromSystemProperties();

        assertEquals(List.of(
                3_520_704_001L,
                3_520_704_017L,
                3_520_704_033L,
                3_520_704_049L,
                3_520_704_065L,
                3_520_704_081L,
                3_520_704_097L,
                3_520_704_113L
        ), config.generatedSeeds());
        assertEquals(3_520_704_129L, config.duplicateScenarioSeed());
        assertEquals(32, config.generatedStepsPerSeed());
    }

    @Test
    void explicitScheduledRangeIsDeterministicAndBounded() {
        System.setProperty(
                TaskFlowModelRunConfig.SEED_START_PROPERTY,
                "256"
        );
        System.setProperty(
                TaskFlowModelRunConfig.SEED_COUNT_PROPERTY,
                "4"
        );
        System.setProperty(
                TaskFlowModelRunConfig.STEPS_PER_SEED_PROPERTY,
                "128"
        );

        TaskFlowModelRunConfig config =
                TaskFlowModelRunConfig.fromSystemProperties();

        assertEquals(
                List.of(256L, 272L, 288L, 304L),
                config.generatedSeeds()
        );
        assertEquals(320L, config.duplicateScenarioSeed());
        assertEquals(128, config.generatedStepsPerSeed());
    }

    @Test
    void invalidCountsStepsAndSeedOverflowFailBeforeHarnessConstruction() {
        assertThrows(
                IllegalArgumentException.class,
                () -> TaskFlowModelRunConfig.create(1L, 0, 32)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> TaskFlowModelRunConfig.create(
                        1L,
                        TaskFlowModelRunConfig.MAX_SEED_COUNT + 1,
                        32
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> TaskFlowModelRunConfig.create(1L, 8, 0)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> TaskFlowModelRunConfig.create(
                        1L,
                        8,
                        TaskFlowModelRunConfig.MAX_STEPS_PER_SEED + 1
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> TaskFlowModelRunConfig.create(Long.MAX_VALUE, 2, 32)
        );
    }
}
