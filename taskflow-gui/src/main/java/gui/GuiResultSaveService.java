package gui;

import client.ClientJobPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.JobResultMessage;

import java.util.Map;
import java.util.Objects;

final class GuiResultSaveService {
    private static final Logger LOGGER = LoggerFactory.getLogger(GuiResultSaveService.class);

    private final Map<String, ClientJobPlugin> clientJobPluginsByType;

    GuiResultSaveService(Map<String, ClientJobPlugin> clientJobPluginsByType) {
        this.clientJobPluginsByType = Map.copyOf(Objects.requireNonNull(
                clientJobPluginsByType,
                "clientJobPluginsByType"));
    }

    GuiResultSaver.SaveResult save(JobResultMessage result, String folderPath) {
        GuiResultSaver.SaveResult saveResult = GuiResultSaver.save(result, folderPath, clientJobPluginsByType);
        if (!saveResult.successful()) {
            logSaveFailure(result, saveResult);
            return saveResult;
        }

        LOGGER.info("event=gui_results_saved job_id={} task_type={} output_dir={}",
                result.getJobId(), result.getTaskType(), saveResult.outputDir());
        return saveResult;
    }

    private void logSaveFailure(JobResultMessage result, GuiResultSaver.SaveResult saveResult) {
        if (saveResult.cause() == null) {
            LOGGER.warn("event=gui_results_save_failed job_id={} task_type={} error={}",
                    result.getJobId(), result.getTaskType(), saveResult.errorMessage());
            return;
        }

        LOGGER.warn("event=gui_results_save_failed job_id={} task_type={} error={}",
                result.getJobId(), result.getTaskType(), saveResult.errorMessage(), saveResult.cause());
    }
}
