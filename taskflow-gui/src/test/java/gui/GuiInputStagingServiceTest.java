package gui;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GuiInputStagingServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void prepareDirectoriesCreatesAndClearsInputAndOutputRoots() throws Exception {
        Path inputRoot = tempDir.resolve("input");
        Path outputRoot = tempDir.resolve("output");
        Files.createDirectories(inputRoot);
        Files.createDirectories(outputRoot);
        Files.writeString(inputRoot.resolve("stale-in.txt"), "old", StandardCharsets.UTF_8);
        Files.writeString(outputRoot.resolve("stale-out.txt"), "old", StandardCharsets.UTF_8);

        GuiInputStagingService service = new GuiInputStagingService(inputRoot, outputRoot);

        service.prepareDirectories();

        assertTrue(Files.isDirectory(inputRoot));
        assertTrue(Files.isDirectory(outputRoot));
        assertFalse(Files.exists(inputRoot.resolve("stale-in.txt")));
        assertFalse(Files.exists(outputRoot.resolve("stale-out.txt")));
    }

    @Test
    void stageListAndClearUseConfiguredInputRoot() throws Exception {
        Path inputRoot = tempDir.resolve("input");
        Path outputRoot = tempDir.resolve("output");
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "payload", StandardCharsets.UTF_8);
        GuiInputStagingService service = new GuiInputStagingService(inputRoot, outputRoot);
        service.prepareDirectories();

        List<InputStaging.StagedInput> staged = service.stageFiles(List.of(source), () -> false);

        assertEquals(1, staged.size());
        assertEquals(
                List.of(inputRoot.resolve("000001").resolve("source.txt").toAbsolutePath().normalize()),
                service.stagedInputFiles());

        service.clearStagedInputs();

        assertTrue(service.stagedInputFiles().isEmpty());
    }

    @Test
    void fileChooserPatternsNormalizeSupportedExtensions() {
        assertEquals("*.*", GuiInputStagingService.fileChooserPattern(null));
        assertEquals("*.*", GuiInputStagingService.fileChooserPattern(" "));
        assertEquals("*.txt", GuiInputStagingService.fileChooserPattern("txt"));
        assertEquals("*.mp4", GuiInputStagingService.fileChooserPattern(".MP4"));
        assertEquals("*.jpg", GuiInputStagingService.fileChooserPattern("*.JPG"));
    }
}
