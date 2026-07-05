package gui;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class GuiChooserDefaultsTest {
    @TempDir
    Path tempDir;

    @Test
    void resolvesExistingDirectory() {
        File selected = GuiChooserDefaults.existingDirectory(
                Map.of(GuiChooserDefaults.INITIAL_INPUT_DIR_ENV, tempDir.toString()),
                GuiChooserDefaults.INITIAL_INPUT_DIR_ENV);

        assertEquals(tempDir.toFile(), selected);
    }

    @Test
    void ignoresMissingDirectory() {
        File selected = GuiChooserDefaults.existingDirectory(
                Map.of(GuiChooserDefaults.INITIAL_INPUT_DIR_ENV, tempDir.resolve("missing").toString()),
                GuiChooserDefaults.INITIAL_INPUT_DIR_ENV);

        assertNull(selected);
    }

    @Test
    void ignoresBlankConfiguration() {
        File selected = GuiChooserDefaults.existingDirectory(
                Map.of(GuiChooserDefaults.INITIAL_INPUT_DIR_ENV, " "),
                GuiChooserDefaults.INITIAL_INPUT_DIR_ENV);

        assertNull(selected);
    }
}
