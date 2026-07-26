package peer.engine;

import org.junit.jupiter.api.Test;
import plugin.TaskResourceCatalog;
import plugin.TaskResourceProfile;
import protocol.TaskAssignMessage;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutorCapacityTrackerTest {
    private static final String INSTANCE_ID = "550e8400-e29b-41d4-a716-446655440099";

    @Test
    void costEightReservationConsumesEightUnitsAndReleasesExactlyOnce() {
        ExecutorCapacityTracker tracker = tracker(8, 2);
        TaskAssignMessage video = task(
                "task-video",
                "VIDEO_TRANSCODING",
                "550e8400-e29b-41d4-a716-446655440001"
        );

        assertEquals(8, tracker.snapshot().availableCapacityUnits());
        assertEquals(ExecutorCapacityTracker.ReserveOutcome.RESERVED, tracker.reserve(video));
        assertEquals(0, tracker.snapshot().availableCapacityUnits());
        assertEquals(
                ExecutorCapacityTracker.ReserveOutcome.ALREADY_RESERVED,
                tracker.reserve(video)
        );
        assertEquals(8L, tracker.reservedUnits());
        assertEquals(ExecutorCapacityTracker.ReleaseOutcome.RELEASED, tracker.release(video));
        assertEquals(
                ExecutorCapacityTracker.ReleaseOutcome.ALREADY_RELEASED,
                tracker.release(video)
        );
        assertEquals(8, tracker.snapshot().availableCapacityUnits());
    }

    @Test
    void lowerCostReservationsFillRemainingUnitsAndSequenceIsMonotonic() {
        ExecutorCapacityTracker tracker = tracker(8, 4);
        long firstSequence = tracker.snapshot().sequence();

        tracker.reserve(task(
                "image",
                "IMAGE_CONVERSION",
                "550e8400-e29b-41d4-a716-446655440002"
        ));
        tracker.reserve(task(
                "text",
                "TEXT_ANALYSIS",
                "550e8400-e29b-41d4-a716-446655440003"
        ));
        ExecutorCapacitySnapshot snapshot = tracker.snapshot();

        assertEquals(firstSequence + 1L, snapshot.sequence());
        assertEquals(5, snapshot.availableCapacityUnits());
        assertEquals(INSTANCE_ID, snapshot.executorInstanceId());
        assertFalse(tracker.overcommitted());
    }

    @Test
    void localOvercommitIsRetainedButAdvertisedAsZero() {
        ExecutorCapacityTracker tracker = tracker(2, 4);
        tracker.reserve(task(
                "video",
                "VIDEO_TRANSCODING",
                "550e8400-e29b-41d4-a716-446655440004"
        ));

        assertTrue(tracker.overcommitted());
        assertEquals(0, tracker.snapshot().availableCapacityUnits());
        assertEquals(1, tracker.reservationCount());
    }

    private static ExecutorCapacityTracker tracker(int totalUnits, int poolSize) {
        TaskResourceCatalog catalog = TaskResourceCatalog.capture(Map.of(
                "TEXT_ANALYSIS", TaskResourceProfile.ofCapacityUnits(1),
                "IMAGE_CONVERSION", TaskResourceProfile.ofCapacityUnits(2),
                "VIDEO_TRANSCODING", TaskResourceProfile.ofCapacityUnits(8)
        ));
        ExecutorCapacityConfig config = new ExecutorCapacityConfig(
                totalUnits,
                poolSize,
                Map.of(
                        "TEXT_ANALYSIS", poolSize,
                        "IMAGE_CONVERSION", poolSize,
                        "VIDEO_TRANSCODING", poolSize
                )
        );
        return new ExecutorCapacityTracker(config, catalog, INSTANCE_ID);
    }

    private static TaskAssignMessage task(String taskId, String taskType, String assignmentId) {
        return new TaskAssignMessage(
                "peer-1",
                "2026-07-26T00:00:00Z",
                taskId,
                "job-1",
                taskType,
                1,
                assignmentId,
                1_780_000_000_000L,
                "payload",
                ""
        );
    }
}
