package server.job;

import protocol.JobSubmitMessage;

import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates jobs from framework plugins discovered on the runtime classpath.
 */
public class JobFactory {
    private static final Map<String, TaskPlugin> PLUGINS = loadPlugins();

    public static EmbarrassinglyParallelJob<?,?> create(JobSubmitMessage msg, String requesterId) {
        String type = normalize(msg.getTaskType());
        TaskPlugin plugin = PLUGINS.get(type);
        if (plugin == null) {
            throw new IllegalArgumentException(
                    "Unsupported job type: " + msg.getTaskType() + ". Available types: " + PLUGINS.keySet());
        }
        return plugin.createJob(msg, requesterId);
    }

    public static Map<String, TaskPlugin> availablePlugins() {
        return Map.copyOf(PLUGINS);
    }

    private static Map<String, TaskPlugin> loadPlugins() {
        Map<String, TaskPlugin> plugins = new ConcurrentHashMap<>();
        for (TaskPlugin plugin : ServiceLoader.load(TaskPlugin.class)) {
            plugins.put(normalize(plugin.taskType()), plugin);
        }
        return plugins;
    }

    private static String normalize(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("Task type is required.");
        }
        return taskType.trim().toUpperCase(Locale.ROOT);
    }
}
