package gui;

import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.VBox;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;
import protocol.JobResultMessage;

import java.io.File;
import java.util.Objects;

final class GuiDownloadWindow {
    private final GuiDownloadSaveController saveController;

    GuiDownloadWindow(GuiDownloadSaveController saveController) {
        this.saveController = Objects.requireNonNull(saveController, "saveController");
    }

    void show(JobResultMessage result) {
        Stage downloadStage = new Stage();
        VBox layout = new VBox(15);
        layout.setPadding(new Insets(20));

        layout.getChildren().add(new Label("Job Complete! Select a location to save your files."));

        Button saveBtn = new Button("Choose Folder & Save");
        saveBtn.setStyle("-fx-base: #2ecc71; -fx-text-fill: white;");
        saveBtn.setOnAction(event -> saveController.chooseFolderAndSave(
                result,
                new JavaFxDownloadView(downloadStage)));

        layout.getChildren().addAll(saveBtn);
        downloadStage.setScene(new Scene(layout, 300, 200));
        downloadStage.setTitle("Download Converted Files");
        downloadStage.show();
    }

    private static final class JavaFxDownloadView implements GuiDownloadSaveController.View {
        private final Stage downloadStage;

        private JavaFxDownloadView(Stage downloadStage) {
            this.downloadStage = downloadStage;
        }

        @Override
        public String chooseOutputFolder() {
            DirectoryChooser directoryChooser = new DirectoryChooser();
            directoryChooser.setTitle("Select Save Location");
            File selectedDirectory = directoryChooser.showDialog(downloadStage);
            return selectedDirectory == null ? null : selectedDirectory.getAbsolutePath();
        }

        @Override
        public void close() {
            downloadStage.close();
        }

        @Override
        public void showSaved() {
            new Alert(Alert.AlertType.INFORMATION, "Files saved successfully!").show();
        }

        @Override
        public void showError(String message) {
            new Alert(Alert.AlertType.ERROR, message).show();
        }
    }
}
