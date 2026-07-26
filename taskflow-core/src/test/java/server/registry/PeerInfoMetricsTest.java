package server.registry;

import org.junit.jupiter.api.Test;
import protocol.PongMessage;
import server.scheduler.SchedulerConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void selectionScoreReflectsFailuresAndReservedCapacityLoad() {
        PeerInfo peer = capacityPeer("peer-2", 4);
        double baseline = peer.getSelectionScore();

        peer.recordTaskFailure();
        peer.recordTaskFailure();
        double withFailures = peer.getSelectionScore();
        assertTrue(withFailures > baseline);

        double withLoad = peer.getSelectionScore(1L);
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
    void selectionScoreUsesAdvertisedCapacityAndConfiguredLoadWeight() {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_SCORE_LOAD_WEIGHT", "10",
                "TASKFLOW_SCORE_LATENCY_WEIGHT", "0",
                "TASKFLOW_SCORE_DURATION_WEIGHT", "0",
                "TASKFLOW_SCORE_FAILURE_WEIGHT", "0"
        ));
        PeerInfo peer = new PeerInfo("peer-4", config);
        peer.applyCapacityHeartbeat(new PongMessage(
                "peer-4",
                "2026-07-26T00:00:00Z",
                List.of("TEST"),
                "550e8400-e29b-41d4-a716-446655440004",
                1L,
                4,
                4,
                Map.of("TEST", 4)
        ));

        assertEquals(5.0, peer.getSelectionScore(2L));
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

    @Test
    void normalizesAndMatchesSupportedTaskTypes() {
        PeerInfo peer = new PeerInfo("peer-6", SchedulerConfig.defaults(), List.of(
                " image_conversion ",
                "IMAGE_CONVERSION",
                "video_transcoding"
        ));

        assertEquals(Set.of("IMAGE_CONVERSION", "VIDEO_TRANSCODING"), peer.getSupportedTaskTypes());
        assertTrue(peer.supportsTaskType("image_conversion"));
        assertTrue(peer.supportsTaskType("VIDEO_TRANSCODING"));
        assertFalse(peer.supportsTaskType("UNKNOWN_TASK"));
    }

    private static PeerInfo capacityPeer(String peerId, int totalCapacityUnits) {
        PeerInfo peer = new PeerInfo(peerId);
        peer.applyCapacityHeartbeat(new PongMessage(
                peerId,
                "2026-07-26T00:00:00Z",
                List.of("TEST"),
                "550e8400-e29b-41d4-a716-446655440002",
                1L,
                totalCapacityUnits,
                totalCapacityUnits,
                Map.of("TEST", totalCapacityUnits)
        ));
        return peer;
    }
}
