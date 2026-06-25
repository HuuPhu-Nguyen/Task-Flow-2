package gui;

import protocol.JobResultMessage;

import java.util.Objects;

final class GuiDownloadSaveController {
    interface SaveService {
        GuiResultSaver.SaveResult save(JobResultMessage result, String folderPath);
    }

    interface View {
        String chooseOutputFolder();

        void close();

        void showSaved();

        void showError(String message);
    }

    private final SaveService saveService;

    GuiDownloadSaveController(SaveService saveService) {
        this.saveService = Objects.requireNonNull(saveService, "saveService");
    }

    void chooseFolderAndSave(JobResultMessage result, View view) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(view, "view");

        String folderPath = view.chooseOutputFolder();
        if (folderPath == null) {
            return;
        }

        GuiResultSaver.SaveResult saveResult = saveService.save(result, folderPath);
        if (!saveResult.successful()) {
            view.showError(saveResult.errorMessage());
            return;
        }

        view.close();
        view.showSaved();
    }
}
