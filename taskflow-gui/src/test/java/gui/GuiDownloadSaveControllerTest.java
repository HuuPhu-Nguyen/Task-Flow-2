package gui;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GuiDownloadSaveControllerTest {
    @Test
    void successfulSaveClosesWindowAndReportsSuccess() {
        RecordingSaveService saveService = new RecordingSaveService();
        saveService.saveResult = GuiResultSaver.SaveResult.success(Path.of("out"));
        RecordingView view = new RecordingView();
        view.selectedFolder = "target/out";
        JobResultMessage result = result();
        GuiDownloadSaveController controller = new GuiDownloadSaveController(saveService);

        controller.chooseFolderAndSave(result, view);

        assertSame(result, saveService.result.get());
        assertEquals("target/out", saveService.folderPath.get());
        assertTrue(view.closed);
        assertTrue(view.savedShown);
        assertNull(view.errorMessage);
    }

    @Test
    void saveFailureKeepsWindowOpenAndReportsError() {
        RecordingSaveService saveService = new RecordingSaveService();
        saveService.saveResult = GuiResultSaver.SaveResult.failure("Could not save files: disk full", Path.of("out"));
        RecordingView view = new RecordingView();
        view.selectedFolder = "target/out";
        JobResultMessage result = result();
        GuiDownloadSaveController controller = new GuiDownloadSaveController(saveService);

        controller.chooseFolderAndSave(result, view);

        assertSame(result, saveService.result.get());
        assertEquals("target/out", saveService.folderPath.get());
        assertFalse(view.closed);
        assertFalse(view.savedShown);
        assertEquals("Could not save files: disk full", view.errorMessage);
    }

    @Test
    void cancelledFolderSelectionDoesNothing() {
        RecordingSaveService saveService = new RecordingSaveService();
        RecordingView view = new RecordingView();
        view.selectedFolder = null;
        GuiDownloadSaveController controller = new GuiDownloadSaveController(saveService);

        controller.chooseFolderAndSave(result(), view);

        assertNull(saveService.result.get());
        assertNull(saveService.folderPath.get());
        assertFalse(view.closed);
        assertFalse(view.savedShown);
        assertNull(view.errorMessage);
    }

    private static JobResultMessage result() {
        return new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                List.of("done"));
    }

    private static final class RecordingSaveService implements GuiDownloadSaveController.SaveService {
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();
        private final AtomicReference<String> folderPath = new AtomicReference<>();
        private GuiResultSaver.SaveResult saveResult = GuiResultSaver.SaveResult.success(Path.of("out"));

        @Override
        public GuiResultSaver.SaveResult save(JobResultMessage result, String folderPath) {
            this.result.set(result);
            this.folderPath.set(folderPath);
            return saveResult;
        }
    }

    private static final class RecordingView implements GuiDownloadSaveController.View {
        private String selectedFolder;
        private boolean closed;
        private boolean savedShown;
        private String errorMessage;

        @Override
        public String chooseOutputFolder() {
            return selectedFolder;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void showSaved() {
            savedShown = true;
        }

        @Override
        public void showError(String message) {
            errorMessage = message;
        }
    }
}
