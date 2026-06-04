package server.registry;

import org.junit.jupiter.api.Test;
import server.scheduler.SchedulerConfig;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerInfoMetricsTest {

    @Test
    void activeTaskCounterNeverGoesNegative() {
        PeerInfo peer = new PeerInfo("peer-1");

        assertEquals(0, peer.decrementTasks());
        assertEquals(0, peer.getActiveTasks());

        peer.incrementTasks();
        peer.decrementTasks();
        assertEquals(0, peer.decrementTasks());
        assertEquals(0, peer.getActiveTasks());
    }

    @Test
    void selectionScoreReflectsFailuresAndLoad() {
        PeerInfo peer = new PeerInfo("peer-2");
        double baseline = peer.getSelectionScore();

        peer.recordTaskFailure();
        peer.recordTaskFailure();
        double withFailures = peer.getSelectionScore();
        assertTrue(withFailures > baseline);

        peer.incrementTasks();
        double withLoad = peer.getSelectionScore();
        assertTrue(withLoad > withFailures);
    }

    @Test
    void ewmaMetricsUpdatePredictably() {
        PeerInfo peer = new PeerInfo("peer-3");
        peer.updateLatency(100);
        assertEquals(100L, peer.getLatency());

        peer.updateLatency(300);
        assertTrue(peer.getLatency() > 100L && peer.getLatency() < 300L);

        peer.recordTaskSuccess(1_000);
        assertEquals(1_000L, peer.getAvgTaskDuration());

        peer.recordTaskSuccess(3_000);
        assertTrue(peer.getAvgTaskDuration() > 1_000L && peer.getAvgTaskDuration() < 3_000L);
    }

    @Test
    void selectionScoreUsesConfiguredLoadLimitAndWeight() {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_TASKS_PER_PEER", "1",
                "TASKFLOW_SCORE_LOAD_WEIGHT", "10",
                "TASKFLOW_SCORE_LATENCY_WEIGHT", "0",
                "TASKFLOW_SCORE_DURATION_WEIGHT", "0",
                "TASKFLOW_SCORE_FAILURE_WEIGHT", "0"
        ));
        PeerInfo peer = new PeerInfo("peer-4", config);

        peer.incrementTasks();

        assertEquals(10.0, peer.getSelectionScore());
    }

    @Test
    void ewmaUsesConfiguredAlpha() {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_SCORE_EWMA_ALPHA", "1.0"
        ));
        PeerInfo peer = new PeerInfo("peer-5", config);

        peer.updateLatency(100);
        peer.updateLatency(300);

        assertEquals(300L, peer.getLatency());
    }
}
