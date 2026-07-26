package peer.engine;

import java.util.Map;

public record ExecutorCapacitySnapshot(
        String executorInstanceId,
        long sequence,
        int totalCapacityUnits,
        int availableCapacityUnits,
        Map<String, Integer> maxConcurrencyByTaskType
) {
    public ExecutorCapacitySnapshot {
        maxConcurrencyByTaskType = maxConcurrencyByTaskType == null
                ? Map.of()
                : Map.copyOf(maxConcurrencyByTaskType);
    }
}
