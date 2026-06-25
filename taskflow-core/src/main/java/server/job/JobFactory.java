package server.job;

import protocol.JobSubmitMessage;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
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
        String type = normalize(msg.getTaskType());
        TaskPlugin plugin = plugins.get(type);
        if (plugin == null) {
            throw new IllegalArgumentException(
                    "Unsupported job type: " + msg.getTaskType() + ". Available types: " + plugins.keySet());
        }
        plugin.validateSubmission(msg);
        return plugin.createJob(msg, requesterId);
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
            String taskType = normalize(plugin.taskType());
            TaskPlugin existing = plugins.putIfAbsent(taskType, plugin);
            if (existing != null) {
                throw new IllegalStateException("Duplicate task plugin for type " + taskType
                        + ": " + existing.getClass().getName()
                        + " and " + plugin.getClass().getName());
            }
        }
        return Map.copyOf(plugins);
    }

    private static String normalize(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("Task type is required.");
        }
        return taskType.trim().toUpperCase(Locale.ROOT);
    }
}
