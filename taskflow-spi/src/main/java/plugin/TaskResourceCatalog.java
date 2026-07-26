package plugin;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Startup-captured immutable task-resource profiles keyed by normalized type.
 */
public final class TaskResourceCatalog {
    private final Map<String, TaskResourceProfile> profiles;

    private TaskResourceCatalog(Map<String, TaskResourceProfile> profiles) {
        this.profiles = profiles;
    }

    public static TaskResourceCatalog capture(Map<String, TaskResourceProfile> profiles) {
        Objects.requireNonNull(profiles, "profiles");
        Map<String, TaskResourceProfile> captured = new LinkedHashMap<>();
        for (Map.Entry<String, TaskResourceProfile> entry : profiles.entrySet()) {
            String taskType = normalizeTaskType(entry.getKey());
            TaskResourceProfile profile = Objects.requireNonNull(
                    entry.getValue(),
                    "Resource profile is required for " + taskType
            );
            TaskResourceProfile previous = captured.putIfAbsent(taskType, profile);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate normalized resource profile for task type " + taskType + "."
                );
            }
        }
        return new TaskResourceCatalog(Map.copyOf(captured));
    }

    public TaskResourceProfile require(String taskType) {
        String normalized = normalizeTaskType(taskType);
        TaskResourceProfile profile = profiles.get(normalized);
        if (profile == null) {
            throw new IllegalArgumentException(
                    "No resource profile for task type " + normalized
                            + ". Available types: " + profiles.keySet()
            );
        }
        return profile;
    }

    public boolean contains(String taskType) {
        return profiles.containsKey(normalizeTaskType(taskType));
    }

    public Set<String> taskTypes() {
        return profiles.keySet();
    }

    public Map<String, TaskResourceProfile> asMap() {
        return profiles;
    }

    public static String normalizeTaskType(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("Task type is required.");
        }
        return taskType.trim().toUpperCase(Locale.ROOT);
    }
}
