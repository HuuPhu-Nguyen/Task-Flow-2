package server.recovery;

/**
 * Unit conversions shared by the TF-0708 experiment and verifier.
 */
public final class RecoveryMetrics {
    private RecoveryMetrics() {
    }

    public static double nanosToMillis(long nanos) {
        if (nanos < 0L) {
            throw new IllegalArgumentException("nanos must not be negative.");
        }
        return nanos / 1_000_000.0D;
    }

    public static double ratePerSecond(long completed, long durationNanos) {
        if (completed < 0L) {
            throw new IllegalArgumentException(
                    "completed must not be negative."
            );
        }
        if (durationNanos <= 0L) {
            throw new IllegalArgumentException(
                    "durationNanos must be positive."
            );
        }
        return completed * 1_000_000_000.0D / durationNanos;
    }
}
