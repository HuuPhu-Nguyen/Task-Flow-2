package protocol;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class PongMessage extends Message {
    private List<String> supportedTaskTypes = List.of();

    public PongMessage(String nodeId, String time) {
        this(nodeId, time, List.of());
    }

    public PongMessage(String nodeId, String time, Collection<String> supportedTaskTypes) {
        this.type = MessageType.PONG;
        this.nodeId = nodeId;
        this.time = time;
        this.supportedTaskTypes = normalizeTaskTypes(supportedTaskTypes);
    }

    public PongMessage(){
        this.type = MessageType.PONG;
    }

    public List<String> getSupportedTaskTypes() {
        return supportedTaskTypes == null ? List.of() : List.copyOf(supportedTaskTypes);
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
}
