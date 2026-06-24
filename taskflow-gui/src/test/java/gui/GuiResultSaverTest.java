package gui;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GuiResultSaverTest {
    @TempDir
    Path tempDir;

    @Test
    void savesResultsWithMatchingClientPlugin() {
        RecordingClientJobPlugin plugin = new RecordingClientJobPlugin();
        JobResultMessage result = result("TEXT_ANALYSIS", List.of("one", "two"));

        GuiResultSaver.SaveResult saveResult = GuiResultSaver.save(
                result,
                tempDir.toString(),
                Map.of("TEXT_ANALYSIS", plugin));

        assertTrue(saveResult.successful());
        assertNull(saveResult.errorMessage());
        assertEquals(tempDir.toAbsolutePath().normalize(), saveResult.outputDir());
        assertEquals(List.of("one", "two"), plugin.savedResults.get());
        assertEquals(tempDir.toAbsolutePath().normalize(), plugin.outputDir.get());
    }

    @Test
    void nullResultPayloadsAreSavedAsEmptyList() {
        RecordingClientJobPlugin plugin = new RecordingClientJobPlugin();

        GuiResultSaver.SaveResult saveResult = GuiResultSaver.save(
                result("TEXT_ANALYSIS", null),
                tempDir.toString(),
                Map.of("TEXT_ANALYSIS", plugin));

        assertTrue(saveResult.successful());
        assertEquals(List.of(), plugin.savedResults.get());
    }

    @Test
    void missingClientPluginReturnsFailure() {
        GuiResultSaver.SaveResult saveResult = GuiResultSaver.save(
                result("UNKNOWN_TASK", List.of("payload")),
                tempDir.toString(),
                Map.of());

        assertFalse(saveResult.successful());
        assertEquals(
                "No client job plugin can save results for task type: UNKNOWN_TASK",
                saveResult.errorMessage());
        assertNull(saveResult.cause());
    }

    @Test
    void pluginSaveFailureReturnsFailure() {
        RecordingClientJobPlugin plugin = new RecordingClientJobPlugin();
        plugin.saveFailure = new IllegalStateException("disk full");

        GuiResultSaver.SaveResult saveResult = GuiResultSaver.save(
                result("TEXT_ANALYSIS", List.of("payload")),
                tempDir.toString(),
                Map.of("TEXT_ANALYSIS", plugin));

        assertFalse(saveResult.successful());
        assertEquals("Could not save files: disk full", saveResult.errorMessage());
        assertEquals(plugin.saveFailure, saveResult.cause());
    }

    private static JobResultMessage result(String taskType, List<Object> payloads) {
        return new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                taskType,
                true,
                payloads);
    }

    private static final class RecordingClientJobPlugin implements ClientJobPlugin {
        private final AtomicReference<List<Object>> savedResults = new AtomicReference<>();
        private final AtomicReference<Path> outputDir = new AtomicReference<>();
        private RuntimeException saveFailure;

        @Override
        public String taskType() {
            return "TEXT_ANALYSIS";
        }

        @Override
        public String displayName() {
            return "Text Analysis";
        }

        @Override
        public List<String> supportedInputExtensions() {
            return List.of("txt");
        }

        @Override
        public List<String> parameterOptions() {
            return List.of("summary");
        }

        @Override
        public String defaultParameter() {
            return "summary";
        }

        @Override
        public List<Object> buildPayloads(List<Path> inputPaths, String parameter) {
            return List.copyOf(inputPaths);
        }

        @Override
        public void saveResults(List<Object> results, Path outputDir) {
            if (saveFailure != null) {
                throw saveFailure;
            }
            this.savedResults.set(List.copyOf(results));
            this.outputDir.set(outputDir);
        }
    }
}
