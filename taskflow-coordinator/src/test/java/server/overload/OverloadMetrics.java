package server.overload;

import java.util.List;

/** Deterministic calculations shared by the TF-0709 harness and report. */
public final class OverloadMetrics {
    private OverloadMetrics() {
    }

    public static long plateauSpan(List<Long> samples) {
        if (samples == null || samples.size() < 3) {
            throw new IllegalArgumentException("At least three heap samples are required.");
        }
        List<Long> tail = samples.subList(samples.size() - 3, samples.size());
        long minimum = tail.stream().mapToLong(OverloadMetrics::requireNonNegative)
                .min().orElseThrow();
        long maximum = tail.stream().mapToLong(OverloadMetrics::requireNonNegative)
                .max().orElseThrow();
        return maximum - minimum;
    }

    public static long plateauMaximum(List<Long> samples) {
        if (samples == null || samples.size() < 3) {
            throw new IllegalArgumentException("At least three heap samples are required.");
        }
        return samples.subList(samples.size() - 3, samples.size()).stream()
                .mapToLong(OverloadMetrics::requireNonNegative)
                .max().orElseThrow();
    }

    private static long requireNonNegative(Long value) {
        if (value == null || value < 0L) {
            throw new IllegalArgumentException("Heap samples must be non-negative.");
        }
        return value;
    }
}
