package gui;

import java.io.File;
import java.util.Map;
import java.util.Objects;

final class GuiChooserDefaults {
    static final String INITIAL_INPUT_DIR_ENV = "TASKFLOW_GUI_INITIAL_INPUT_DIR";
    static final String INITIAL_OUTPUT_DIR_ENV = "TASKFLOW_GUI_INITIAL_OUTPUT_DIR";

    private GuiChooserDefaults() {
    }

    static File existingDirectory(Map<String, String> environment, String envName) {
        Objects.requireNonNull(environment, "environment");
        Objects.requireNonNull(envName, "envName");
        String configured = environment.get(envName);
        if (configured == null || configured.isBlank()) {
            return null;
        }
        File directory = new File(configured.trim());
        return directory.isDirectory() ? directory : null;
    }
}
