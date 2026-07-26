package protocol;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class PongMessage extends Message {
    private List<String> supportedTaskTypes = List.of();
    private String executorInstanceId;
    private long capacitySnapshotSequence;
    private int totalCapacityUnits;
    private int availableCapacityUnits;
    private Map<String, Integer> maxConcurrencyByTaskType = Map.of();

    public PongMessage(String nodeId, String time) {
        this(nodeId, time, List.of());
    }

    public PongMessage(String nodeId, String time, Collection<String> supportedTaskTypes) {
        this.type = MessageType.PONG;
        this.nodeId = nodeId;
        this.time = time;
        this.supportedTaskTypes = normalizeTaskTypes(supportedTaskTypes);
    }

    public PongMessage(String nodeId,
                       String time,
                       Collection<String> supportedTaskTypes,
                       String executorInstanceId,
                       long capacitySnapshotSequence,
                       int totalCapacityUnits,
                       int availableCapacityUnits,
                       Map<String, Integer> maxConcurrencyByTaskType) {
        this.type = MessageType.PONG;
        this.protocolVersion = ProtocolVersions.CAPACITY_ADVERTISEMENT;
        this.nodeId = nodeId;
        this.time = time;
        this.supportedTaskTypes = normalizeTaskTypes(supportedTaskTypes);
        this.executorInstanceId = executorInstanceId;
        this.capacitySnapshotSequence = capacitySnapshotSequence;
        this.totalCapacityUnits = totalCapacityUnits;
        this.availableCapacityUnits = availableCapacityUnits;
        this.maxConcurrencyByTaskType = normalizeConcurrency(maxConcurrencyByTaskType);
    }

    public PongMessage(){
        this.type = MessageType.PONG;
    }

    public List<String> getSupportedTaskTypes() {
        return supportedTaskTypes == null ? List.of() : List.copyOf(supportedTaskTypes);
    }

    public String getExecutorInstanceId() {
        return executorInstanceId;
    }

    public long getCapacitySnapshotSequence() {
        return capacitySnapshotSequence;
    }

    public int getTotalCapacityUnits() {
        return totalCapacityUnits;
    }

    public int getAvailableCapacityUnits() {
        return availableCapacityUnits;
    }

    public Map<String, Integer> getMaxConcurrencyByTaskType() {
        return maxConcurrencyByTaskType == null
                ? Map.of()
                : Map.copyOf(maxConcurrencyByTaskType);
    }

    private static List<String> normalizeTaskTypes(Collection<String> taskTypes) {
        if (taskTypes == null) {
            return List.of();
        }
        return taskTypes.stream()
                .filter(taskType -> taskType != null && !taskType.isBlank())
                .map(taskType -> taskType.trim().toUpperCase(Locale.ROOT))
                .distinct()
                .sorted()
                .toList();
    }

    private static Map<String, Integer> normalizeConcurrency(Map<String, Integer> concurrency) {
        if (concurrency == null || concurrency.isEmpty()) {
            return Map.of();
        }
        Map<String, Integer> normalized = new TreeMap<>();
        for (Map.Entry<String, Integer> entry : concurrency.entrySet()) {
            String taskType = entry.getKey() == null
                    ? ""
                    : entry.getKey().trim().toUpperCase(Locale.ROOT);
            if (normalized.containsKey(taskType)) {
                throw new IllegalArgumentException(
                        "Duplicate normalized concurrency task type " + taskType + "."
                );
            }
            normalized.put(taskType, entry.getValue());
        }
        return Map.copyOf(new LinkedHashMap<>(normalized));
    }
}
