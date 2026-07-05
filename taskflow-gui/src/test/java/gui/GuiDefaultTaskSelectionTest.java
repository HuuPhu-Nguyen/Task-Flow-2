package gui;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class GuiDefaultTaskSelectionTest {
    private final ClientJobPlugin image = plugin("IMAGE_CONVERSION", "Image Conversion");
    private final ClientJobPlugin text = plugin("TEXT_ANALYSIS", "Text Analysis");

    @Test
    void defaultsToFirstPluginWithoutEnvironmentOverride() {
        ClientJobPlugin selected = GuiDefaultTaskSelection.choose(List.of(image, text), Map.of());

        assertSame(image, selected);
    }

    @Test
    void selectsConfiguredTaskType() {
        ClientJobPlugin selected = GuiDefaultTaskSelection.choose(
                List.of(image, text),
                Map.of(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_ENV, "TEXT_ANALYSIS"));

        assertSame(text, selected);
    }

    @Test
    void systemPropertyOverridesEnvironment() {
        String previous = System.getProperty(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_PROPERTY);
        try {
            System.setProperty(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_PROPERTY, "TEXT_ANALYSIS");

            ClientJobPlugin selected = GuiDefaultTaskSelection.choose(
                    List.of(image, text),
                    Map.of(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_ENV, "IMAGE_CONVERSION"));

            assertSame(text, selected);
        } finally {
            if (previous == null) {
                System.clearProperty(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_PROPERTY);
            } else {
                System.setProperty(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_PROPERTY, previous);
            }
        }
    }

    @Test
    void selectsConfiguredDisplayNameCaseInsensitively() {
        ClientJobPlugin selected = GuiDefaultTaskSelection.choose(
                List.of(image, text),
                Map.of(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_ENV, "text analysis"));

        assertSame(text, selected);
    }

    @Test
    void unknownConfiguredTaskFallsBackToFirstPlugin() {
        ClientJobPlugin selected = GuiDefaultTaskSelection.choose(
                List.of(image, text),
                Map.of(GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_ENV, "unknown"));

        assertSame(image, selected);
    }

    @Test
    void emptyPluginListHasNoSelection() {
        assertNull(GuiDefaultTaskSelection.choose(List.of(), Map.of(
                GuiDefaultTaskSelection.DEFAULT_TASK_TYPE_ENV, "TEXT_ANALYSIS")));
    }

    private static ClientJobPlugin plugin(String taskType, String displayName) {
        return new ClientJobPlugin() {
            @Override
            public String taskType() {
                return taskType;
            }

            @Override
            public String displayName() {
                return displayName;
            }

            @Override
            public List<String> supportedInputExtensions() {
                return List.of();
            }

            @Override
            public List<String> parameterOptions() {
                return List.of("csv");
            }

            @Override
            public String defaultParameter() {
                return "csv";
            }

            @Override
            public List<Object> buildPayloads(List<Path> inputPaths, String parameter) {
                return List.of();
            }

            @Override
            public void saveResults(List<Object> results, Path outputDir) {
            }
        };
    }
}
