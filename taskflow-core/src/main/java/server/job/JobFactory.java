package server.job;

import plugin.RetrySafety;
import protocol.JobSubmitMessage;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Creates jobs from framework plugins discovered on the runtime classpath.
 */
public class JobFactory {
    private static final Map<String, TaskPlugin> PLUGINS = loadPlugins();

    public static EmbarrassinglyParallelJob<?,?> create(JobSubmitMessage msg, String requesterId) {
        return create(msg, requesterId, PLUGINS);
    }

    static EmbarrassinglyParallelJob<?,?> create(JobSubmitMessage msg,
                                                String requesterId,
                                                Map<String, TaskPlugin> plugins) {
        TaskPlugin plugin = requirePlugin(msg.getTaskType(), plugins);
        plugin.validateSubmission(msg);
        return plugin.createJob(msg, requesterId);
    }

    public static RetrySafety retrySafety(String taskType) {
        return retrySafety(taskType, PLUGINS);
    }

    static RetrySafety retrySafety(String taskType, Map<String, TaskPlugin> plugins) {
        return requireRetrySafety(requirePlugin(taskType, plugins));
    }

    public static Map<String, TaskPlugin> availablePlugins() {
        return Map.copyOf(PLUGINS);
    }

    private static Map<String, TaskPlugin> loadPlugins() {
        return loadPlugins(ServiceLoader.load(TaskPlugin.class));
    }

    static Map<String, TaskPlugin> loadPlugins(Iterable<TaskPlugin> discoveredPlugins) {
        Map<String, TaskPlugin> plugins = new LinkedHashMap<>();
        for (TaskPlugin plugin : discoveredPlugins) {
            Objects.requireNonNull(plugin, "Discovered task plugin is required.");
            String taskType = normalize(plugin.taskType());
            requireRetrySafety(plugin);
            TaskPlugin existing = plugins.putIfAbsent(taskType, plugin);
            if (existing != null) {
                throw new IllegalStateException("Duplicate task plugin for type " + taskType
                        + ": " + existing.getClass().getName()
                        + " and " + plugin.getClass().getName());
            }
        }
        return Map.copyOf(plugins);
    }

    private static TaskPlugin requirePlugin(String taskType, Map<String, TaskPlugin> plugins) {
        String normalized = normalize(taskType);
        TaskPlugin plugin = plugins.get(normalized);
        if (plugin == null) {
            throw new IllegalArgumentException(
                    "Unsupported job type: " + taskType + ". Available types: " + plugins.keySet());
        }
        return plugin;
    }

    private static RetrySafety requireRetrySafety(TaskPlugin plugin) {
        RetrySafety retrySafety = plugin.retrySafety();
        if (retrySafety == null) {
            throw new IllegalStateException(
                    "Task plugin " + plugin.getClass().getName() + " must declare retry safety."
            );
        }
        return retrySafety;
    }

    private static String normalize(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("Task type is required.");
        }
        return taskType.trim().toUpperCase(Locale.ROOT);
    }
}
