package server;

import java.util.ArrayList;
import java.util.List;

/**
 * Bounded system-property configuration for model-based scheduler evidence.
 */
record TaskFlowModelRunConfig(
        List<Long> generatedSeeds,
        long duplicateScenarioSeed,
        int generatedStepsPerSeed
) {
    static final String SEED_START_PROPERTY = "taskflow.model.seedStart";
    static final String SEED_COUNT_PROPERTY = "taskflow.model.seedCount";
    static final String STEPS_PER_SEED_PROPERTY =
            "taskflow.model.stepsPerSeed";

    static final long DEFAULT_SEED_START = 3_520_704_001L;
    static final int DEFAULT_SEED_COUNT = 8;
    static final int DEFAULT_STEPS_PER_SEED = 32;
    static final int MAX_SEED_COUNT = 256;
    static final int MAX_STEPS_PER_SEED = 1_024;
    private static final long SEED_STRIDE = 16L;

    TaskFlowModelRunConfig {
        generatedSeeds = List.copyOf(generatedSeeds);
        if (generatedSeeds.isEmpty()) {
            throw new IllegalArgumentException(
                    "generatedSeeds must not be empty."
            );
        }
        if (generatedSeeds.size() > MAX_SEED_COUNT) {
            throw new IllegalArgumentException(
                    "generatedSeeds exceeds " + MAX_SEED_COUNT + "."
            );
        }
        requireRange(
                generatedStepsPerSeed,
                1,
                MAX_STEPS_PER_SEED,
                "generatedStepsPerSeed"
        );
    }

    static TaskFlowModelRunConfig fromSystemProperties() {
        return create(
                longProperty(SEED_START_PROPERTY, DEFAULT_SEED_START),
                intProperty(SEED_COUNT_PROPERTY, DEFAULT_SEED_COUNT),
                intProperty(
                        STEPS_PER_SEED_PROPERTY,
                        DEFAULT_STEPS_PER_SEED
                )
        );
    }

    static TaskFlowModelRunConfig create(
            long seedStart,
            int seedCount,
            int generatedStepsPerSeed
    ) {
        requireRange(seedCount, 1, MAX_SEED_COUNT, "seedCount");
        requireRange(
                generatedStepsPerSeed,
                1,
                MAX_STEPS_PER_SEED,
                "generatedStepsPerSeed"
        );

        List<Long> seeds = new ArrayList<>(seedCount);
        try {
            for (int index = 0; index < seedCount; index++) {
                seeds.add(Math.addExact(
                        seedStart,
                        Math.multiplyExact(SEED_STRIDE, index)
                ));
            }
            long duplicateSeed = Math.addExact(
                    seedStart,
                    Math.multiplyExact(SEED_STRIDE, seedCount)
            );
            return new TaskFlowModelRunConfig(
                    seeds,
                    duplicateSeed,
                    generatedStepsPerSeed
            );
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "Configured model seed range overflows a signed long.",
                    overflow
            );
        }
    }

    private static int intProperty(String name, int fallback) {
        return Integer.parseInt(System.getProperty(
                name,
                Integer.toString(fallback)
        ));
    }

    private static long longProperty(String name, long fallback) {
        return Long.parseLong(System.getProperty(
                name,
                Long.toString(fallback)
        ));
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
