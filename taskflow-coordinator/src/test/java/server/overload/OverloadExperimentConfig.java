package server.overload;

import java.nio.file.Path;

/** Bounded inputs for the opt-in TF-0709 overload experiment. */
public record OverloadExperimentConfig(
        Path outputDirectory,
        int waveCount,
        int submissionsPerWave,
        int mailboxCapacity,
        int activeJobLimit,
        int maxPendingOutboxRows,
        long taskLeaseMillis,
        long completionTimeoutSeconds,
        long heapPlateauSpanBytes,
        long heapCeilingBytes
) {
    public static final int REPORT_WAVES = 5;
    public static final int REPORT_SUBMISSIONS_PER_WAVE = 200;
    public static final int REPORT_MAILBOX_CAPACITY = 1;
    public static final int REPORT_ACTIVE_JOB_LIMIT = 32;
    public static final int REPORT_MAX_PENDING_OUTBOX_ROWS = 16;
    public static final long REPORT_TASK_LEASE_MILLIS = 5_000L;
    public static final long REPORT_COMPLETION_TIMEOUT_SECONDS = 300L;
    public static final long REPORT_HEAP_PLATEAU_SPAN_BYTES = 16L * 1024L * 1024L;
    public static final long REPORT_HEAP_CEILING_BYTES = 128L * 1024L * 1024L;

    public OverloadExperimentConfig {
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required.");
        }
        requireRange(waveCount, 3L, 20L, "waveCount");
        requireRange(submissionsPerWave, 10L, 100_000L, "submissionsPerWave");
        requireRange(mailboxCapacity, 1L, 10_000L, "mailboxCapacity");
        requireRange(activeJobLimit, 2L, 10_000L, "activeJobLimit");
        requireRange(maxPendingOutboxRows, 2L, 100_000L, "maxPendingOutboxRows");
        requireRange(taskLeaseMillis, 100L, 120_000L, "taskLeaseMillis");
        requireRange(completionTimeoutSeconds, 10L, 3_600L, "completionTimeoutSeconds");
        requireRange(heapPlateauSpanBytes, 1L, 1024L * 1024L * 1024L,
                "heapPlateauSpanBytes");
        requireRange(heapCeilingBytes, heapPlateauSpanBytes, 2L * 1024L * 1024L * 1024L,
                "heapCeilingBytes");
    }

    public static OverloadExperimentConfig fromSystemProperties() {
        return new OverloadExperimentConfig(
                Path.of(System.getProperty(
                        "taskflow.overload.outputDirectory",
                        "target/overload/run"
                )),
                integer("taskflow.overload.waves", REPORT_WAVES),
                integer("taskflow.overload.submissionsPerWave", REPORT_SUBMISSIONS_PER_WAVE),
                integer("taskflow.overload.mailboxCapacity", REPORT_MAILBOX_CAPACITY),
                integer("taskflow.overload.activeJobLimit", REPORT_ACTIVE_JOB_LIMIT),
                integer("taskflow.overload.maxPendingOutboxRows",
                        REPORT_MAX_PENDING_OUTBOX_ROWS),
                longValue("taskflow.overload.taskLeaseMillis", REPORT_TASK_LEASE_MILLIS),
                longValue("taskflow.overload.completionTimeoutSeconds",
                        REPORT_COMPLETION_TIMEOUT_SECONDS),
                longValue("taskflow.overload.heapPlateauSpanBytes",
                        REPORT_HEAP_PLATEAU_SPAN_BYTES),
                longValue("taskflow.overload.heapCeilingBytes", REPORT_HEAP_CEILING_BYTES)
        );
    }

    public long totalFloodSubmissions() {
        return Math.multiplyExact((long) waveCount, submissionsPerWave);
    }

    public void requireReportGrade() {
        if (waveCount != REPORT_WAVES
                || submissionsPerWave != REPORT_SUBMISSIONS_PER_WAVE
                || mailboxCapacity != REPORT_MAILBOX_CAPACITY
                || activeJobLimit != REPORT_ACTIVE_JOB_LIMIT
                || maxPendingOutboxRows != REPORT_MAX_PENDING_OUTBOX_ROWS
                || taskLeaseMillis != REPORT_TASK_LEASE_MILLIS
                || completionTimeoutSeconds != REPORT_COMPLETION_TIMEOUT_SECONDS
                || heapPlateauSpanBytes != REPORT_HEAP_PLATEAU_SPAN_BYTES
                || heapCeilingBytes != REPORT_HEAP_CEILING_BYTES) {
            throw new IllegalArgumentException(
                    "Report-grade overload execution requires the fixed TF-0709 inputs."
            );
        }
    }

    private static int integer(String property, int fallback) {
        String value = System.getProperty(property);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value);
    }

    private static long longValue(String property, long fallback) {
        String value = System.getProperty(property);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value);
    }

    private static void requireRange(long value, long minimum, long maximum, String field) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    field + " must be in [" + minimum + ", " + maximum + "]."
            );
        }
    }
}
