package client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

public final class ClientJobPlugins {
    private ClientJobPlugins() {
    }

    public static List<ClientJobPlugin> discover() {
        List<ClientJobPlugin> plugins = new ArrayList<>();
        for (ClientJobPlugin plugin : ServiceLoader.load(ClientJobPlugin.class)) {
            plugins.add(plugin);
        }
        plugins.sort(Comparator.comparing(ClientJobPlugin::displayName));
        return List.copyOf(plugins);
    }

    public static Map<String, ClientJobPlugin> byTaskType(Collection<ClientJobPlugin> plugins) {
        Map<String, ClientJobPlugin> byTaskType = new LinkedHashMap<>();
        for (ClientJobPlugin plugin : plugins) {
            String key = normalizeTaskType(plugin.taskType());
            ClientJobPlugin previous = byTaskType.putIfAbsent(key, plugin);
            if (previous != null) {
                throw new IllegalStateException("Duplicate client job plugin for task type: " + key);
            }
        }
        return Map.copyOf(byTaskType);
    }

    public static Optional<ClientJobPlugin> findByTaskType(Collection<ClientJobPlugin> plugins, String taskType) {
        String key = normalizeTaskType(taskType);
        return plugins.stream()
                .filter(plugin -> normalizeTaskType(plugin.taskType()).equals(key))
                .findFirst();
    }

    public static String normalizeTaskType(String taskType) {
        if (taskType == null) {
            return "";
        }
        return taskType.trim().toUpperCase(Locale.ROOT);
    }
}
