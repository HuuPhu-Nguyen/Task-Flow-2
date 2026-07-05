package gui;

import client.ClientJobPlugin;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class GuiDefaultTaskSelection {
    static final String DEFAULT_TASK_TYPE_ENV = "TASKFLOW_GUI_DEFAULT_TASK_TYPE";
    static final String DEFAULT_TASK_TYPE_PROPERTY = "taskflow.gui.defaultTaskType";

    private GuiDefaultTaskSelection() {
    }

    static ClientJobPlugin choose(List<ClientJobPlugin> plugins, Map<String, String> environment) {
        Objects.requireNonNull(plugins, "plugins");
        Objects.requireNonNull(environment, "environment");
        if (plugins.isEmpty()) {
            return null;
        }

        String configuredTaskType = configuredTaskType(environment);
        if (configuredTaskType != null && !configuredTaskType.isBlank()) {
            String requested = configuredTaskType.trim().toLowerCase(Locale.ROOT);
            for (ClientJobPlugin plugin : plugins) {
                if (matches(plugin.taskType(), requested) || matches(plugin.displayName(), requested)) {
                    return plugin;
                }
            }
        }

        return plugins.get(0);
    }

    private static String configuredTaskType(Map<String, String> environment) {
        String property = System.getProperty(DEFAULT_TASK_TYPE_PROPERTY);
        if (property != null && !property.isBlank()) {
            return property;
        }
        return environment.get(DEFAULT_TASK_TYPE_ENV);
    }

    private static boolean matches(String value, String requested) {
        return value != null && value.trim().toLowerCase(Locale.ROOT).equals(requested);
    }
}
