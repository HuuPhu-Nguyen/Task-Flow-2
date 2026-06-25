package gui;

import javafx.concurrent.Task;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;

import java.util.List;
import java.util.Objects;

final class GuiBackgroundTaskControls implements GuiBackgroundTaskRunner.View {
    private final List<? extends Node> disabledNodes;
    private final ProgressIndicator busyIndicator;
    private final Label statusLabel;
    private final Button cancelButton;

    GuiBackgroundTaskControls(
            List<? extends Node> disabledNodes,
            ProgressIndicator busyIndicator,
            Label statusLabel,
            Button cancelButton) {
        this.disabledNodes = List.copyOf(Objects.requireNonNull(disabledNodes, "disabledNodes"));
        this.busyIndicator = Objects.requireNonNull(busyIndicator, "busyIndicator");
        this.statusLabel = Objects.requireNonNull(statusLabel, "statusLabel");
        this.cancelButton = Objects.requireNonNull(cancelButton, "cancelButton");
    }

    @Override
    public void begin(Task<?> task, Runnable cancelAction) {
        setControlsDisabled(true);
        busyIndicator.progressProperty().bind(task.progressProperty());
        busyIndicator.setVisible(true);
        statusLabel.textProperty().bind(task.messageProperty());
        cancelButton.setVisible(true);
        cancelButton.setDisable(false);
        cancelButton.setOnAction(event -> cancelAction.run());
    }

    @Override
    public void finish(Task<?> task) {
        setControlsDisabled(false);
        busyIndicator.progressProperty().unbind();
        busyIndicator.setVisible(false);
        statusLabel.textProperty().unbind();
        cancelButton.setOnAction(null);
        cancelButton.setDisable(true);
        cancelButton.setVisible(false);
    }

    @Override
    public void setStatus(String message) {
        statusLabel.setText(message);
    }

    @Override
    public void showError(String message) {
        new Alert(Alert.AlertType.ERROR, message).show();
    }

    private void setControlsDisabled(boolean disabled) {
        disabledNodes.forEach(node -> node.setDisable(disabled));
    }
}
