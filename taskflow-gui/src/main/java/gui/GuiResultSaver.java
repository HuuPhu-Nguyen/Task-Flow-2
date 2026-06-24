package gui;

import client.ClientJobPlugin;
import client.ClientJobPlugins;
import protocol.JobResultMessage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class GuiResultSaver {
    private GuiResultSaver() {
    }

    static SaveResult save(JobResultMessage result,
                           String folderPath,
                           Map<String, ClientJobPlugin> clientJobPluginsByType) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(clientJobPluginsByType, "clientJobPluginsByType");
        Path outputDir = Paths.get(folderPath).toAbsolutePath().normalize();

        ClientJobPlugin plugin = clientJobPluginsByType.get(ClientJobPlugins.normalizeTaskType(result.getTaskType()));
        if (plugin == null) {
            return SaveResult.failure(
                    "No client job plugin can save results for task type: " + result.getTaskType(),
                    outputDir);
        }

        List<Object> payloads = result.getResultsByTaskId() == null
                ? List.of()
                : result.getResultsByTaskId();
        try {
            plugin.saveResults(payloads, outputDir);
            return SaveResult.success(outputDir);
        } catch (Exception ex) {
            return SaveResult.failure("Could not save files: " + ex.getMessage(), outputDir, ex);
        }
    }

    record SaveResult(boolean successful, String errorMessage, Path outputDir, Throwable cause) {
        static SaveResult success(Path outputDir) {
            return new SaveResult(true, null, outputDir, null);
        }

        static SaveResult failure(String errorMessage, Path outputDir) {
            return failure(errorMessage, outputDir, null);
        }

        static SaveResult failure(String errorMessage, Path outputDir, Throwable cause) {
            return new SaveResult(false, errorMessage, outputDir, cause);
        }
    }
}
