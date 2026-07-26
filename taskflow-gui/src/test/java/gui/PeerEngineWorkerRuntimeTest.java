package gui;

import org.junit.jupiter.api.Test;
import protocol.MessageValidator;
import protocol.PongMessage;
import protocol.ProtocolVersions;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerEngineWorkerRuntimeTest {
    @Test
    void productionRuntimeBuildsVersionThreeCapacityHeartbeat() {
        PeerEngineWorkerRuntime runtime =
                new PeerEngineWorkerRuntime("gui-capacity-peer");
        try {
            PongMessage heartbeat = runtime.capacityHeartbeat(
                    "gui-capacity-peer",
                    "2026-07-26T00:00:00Z"
            );

            assertEquals(
                    ProtocolVersions.CAPACITY_ADVERTISEMENT,
                    heartbeat.getProtocolVersion()
            );
            assertFalse(heartbeat.getSupportedTaskTypes().isEmpty());
            assertTrue(heartbeat.getTotalCapacityUnits() > 0);
            assertEquals(
                    heartbeat.getSupportedTaskTypes().stream().sorted().toList(),
                    heartbeat.getMaxConcurrencyByTaskType().keySet().stream().sorted().toList()
            );
            assertDoesNotThrow(() -> MessageValidator.validate(heartbeat));
        } finally {
            runtime.shutdown();
        }
    }
}
