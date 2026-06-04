package server.scheduler;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchedulerConfigTest {

    @Test
    void usesCurrentRuntimeDefaults() {
        SchedulerConfig config = SchedulerConfig.defaults();

        assertEquals(60_000L, config.taskTimeoutMillis());
        assertEquals(3, config.maxTasksPerPeer());
        assertEquals(20, config.maxTaskRetries());
        assertEquals(10_000L, config.metricsLogIntervalMillis());
        assertEquals(6.0, config.peerScoreLoadWeight());
        assertEquals(2.0, config.peerScoreLatencyWeight());
        assertEquals(1.5, config.peerScoreDurationWeight());
        assertEquals(4.0, config.peerScoreFailureWeight());
        assertEquals(200.0, config.peerScoreLatencyBaselineMillis());
        assertEquals(5_000.0, config.peerScoreDurationBaselineMillis());
        assertEquals(0.2, config.peerScoreEwmaAlpha());
    }

    @Test
    void readsEnvironmentOverrides() {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.ofEntries(
                Map.entry("TASKFLOW_TASK_TIMEOUT_MS", "120000"),
                Map.entry("TASKFLOW_MAX_TASKS_PER_PEER", "7"),
                Map.entry("TASKFLOW_MAX_TASK_RETRIES", "5"),
                Map.entry("TASKFLOW_METRICS_LOG_INTERVAL_MS", "3000"),
                Map.entry("TASKFLOW_SCORE_LOAD_WEIGHT", "4.5"),
                Map.entry("TASKFLOW_SCORE_LATENCY_WEIGHT", "1.25"),
                Map.entry("TASKFLOW_SCORE_DURATION_WEIGHT", "2.75"),
                Map.entry("TASKFLOW_SCORE_FAILURE_WEIGHT", "8.5"),
                Map.entry("TASKFLOW_SCORE_LATENCY_BASELINE_MS", "400"),
                Map.entry("TASKFLOW_SCORE_DURATION_BASELINE_MS", "9000"),
                Map.entry("TASKFLOW_SCORE_EWMA_ALPHA", "0.75")
        ));

        assertEquals(120_000L, config.taskTimeoutMillis());
        assertEquals(7, config.maxTasksPerPeer());
        assertEquals(5, config.maxTaskRetries());
        assertEquals(3_000L, config.metricsLogIntervalMillis());
        assertEquals(4.5, config.peerScoreLoadWeight());
        assertEquals(1.25, config.peerScoreLatencyWeight());
        assertEquals(2.75, config.peerScoreDurationWeight());
        assertEquals(8.5, config.peerScoreFailureWeight());
        assertEquals(400.0, config.peerScoreLatencyBaselineMillis());
        assertEquals(9_000.0, config.peerScoreDurationBaselineMillis());
        assertEquals(0.75, config.peerScoreEwmaAlpha());
    }

    @Test
    void rejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASKS_PER_PEER", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCORE_FAILURE_WEIGHT", "-1")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCORE_EWMA_ALPHA", "1.5")));
    }
}
