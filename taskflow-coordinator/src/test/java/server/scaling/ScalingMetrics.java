package server.scaling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Deterministic calculations shared by the scaling experiment and its tests.
 */
public final class ScalingMetrics {
    private ScalingMetrics() {
    }

    public static long nearestRank(
            Collection<Long> samples,
            double percentile
    ) {
        if (samples == null || samples.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one sample is required."
            );
        }
        if (!(percentile > 0.0D && percentile <= 1.0D)) {
            throw new IllegalArgumentException("percentile must be in (0, 1].");
        }
        List<Long> sorted = new ArrayList<>(samples);
        if (sorted.stream().anyMatch(value -> value == null || value < 0L)) {
            throw new IllegalArgumentException("Samples must be nonnegative.");
        }
        sorted.sort(Comparator.naturalOrder());
        int rank = (int) Math.ceil(percentile * sorted.size());
        return sorted.get(Math.max(0, rank - 1));
    }

    public static double nanosToMillis(long nanos) {
        if (nanos < 0L) {
            throw new IllegalArgumentException("nanos must be nonnegative.");
        }
        return nanos / 1_000_000.0D;
    }

    public static double parallelEfficiency(
            double oneWorkerThroughput,
            double candidateThroughput,
            int workerCount
    ) {
        if (!Double.isFinite(oneWorkerThroughput)
                || oneWorkerThroughput <= 0.0D) {
            throw new IllegalArgumentException(
                    "oneWorkerThroughput must be finite and positive."
            );
        }
        if (!Double.isFinite(candidateThroughput)
                || candidateThroughput < 0.0D) {
            throw new IllegalArgumentException(
                    "candidateThroughput must be finite and nonnegative."
            );
        }
        if (!ScalingExperimentConfig.WORKER_MATRIX.contains(workerCount)) {
            throw new IllegalArgumentException(
                    "workerCount is outside the scaling matrix."
            );
        }
        return candidateThroughput / (oneWorkerThroughput * workerCount);
    }
}
