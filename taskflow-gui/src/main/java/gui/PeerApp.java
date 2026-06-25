package gui;

import client.ClientJobPlugin;
import client.ClientJobPlugins;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.JobResultMessage;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

public class PeerApp extends Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeerApp.class);

    private Stage window;
    private GuiWorkerRuntime workerRuntime;
    private JobSubmissionClient jobSubmissionClient;
    private final java.util.Set<String> myActiveJobIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private volatile CoordinatorConnection networkConnection;
    private volatile Task<?> activeBackgroundTask;
    private volatile boolean stopping;

    private String currentInPath;
    private String currentOutPath;
    private String sessionId;
    private TilePane gallery;
    private GuiHistoryStore historyStore;
    private List<ClientJobPlugin> clientJobPlugins = List.of();
    private Map<String, ClientJobPlugin> clientJobPluginsByType = Map.of();

    @Override
    public void init() {
        try {
            // Generate unique folders for this GUI instance
            this.sessionId = "PEER_" + (System.currentTimeMillis() % 100000);
            this.currentInPath = "java/in_" + sessionId;
            this.currentOutPath = "java/out_" + sessionId;

            FileUtils.prepareDirectories(currentInPath, currentOutPath);

            workerRuntime = new PeerEngineWorkerRuntime(sessionId);
            jobSubmissionClient = new TcpJobSubmissionClient(sessionId);
            LOGGER.info("event=gui_processors_registered peer_id={} task_types={}",
                    sessionId, workerRuntime.supportedTaskTypes());
            clientJobPlugins = ClientJobPlugins.discover();
            clientJobPluginsByType = ClientJobPlugins.byTaskType(clientJobPlugins);
            LOGGER.info("event=gui_client_plugins_registered task_types={}", clientJobPluginsByType.keySet());
        } catch (Exception e) {
            LOGGER.error("event=gui_init_failed error={}", e.getMessage(), e);
        }
    }

    @Override
    public void start(Stage primaryStage) {
        this.window = primaryStage;
        showConnectionScreen();
    }

    private void showConnectionScreen() {
        VBox root = new VBox(10);
        root.setPadding(new Insets(20));

        TextField hostField = new TextField("localhost");
        TextField portField = new TextField("6789");
        Button connectBtn = new Button("Connect to Coordinator");

        connectBtn.setOnAction(e -> {
            connectBtn.setDisable(true);
            int port;
            try {
                port = Integer.parseInt(portField.getText().trim());
            } catch (NumberFormatException invalidPort) {
                connectBtn.setDisable(false);
                new Alert(Alert.AlertType.ERROR, "Port must be a number.").show();
                return;
            }
            startNetworkThread(
                    hostField.getText(),
                    port,
                    () -> Platform.runLater(this::showMainGallery),
                    error -> Platform.runLater(() -> {
                        connectBtn.setDisable(false);
                        new Alert(Alert.AlertType.ERROR,
                                "Could not connect to coordinator: " + error).show();
                    }));
        });

        root.getChildren().addAll(new Label("Host:"), hostField, new Label("Port:"), portField, connectBtn);
        window.setScene(new Scene(root, 300, 250));
        window.setTitle("Connect Peer");
        window.show();
    }

    private void showMainGallery() {
        TabPane tabPane = new TabPane();

        Tab convertTab = new Tab("Convert");
        convertTab.setClosable(false);
        convertTab.setContent(buildConversionPane());

        Tab historyTab = new Tab("Job History");
        historyTab.setClosable(false);
        historyTab.setContent(buildHistoryPane());

        tabPane.getTabs().addAll(convertTab, historyTab);

        window.setScene(new Scene(tabPane, 900, 650));
        window.show();
    }

    private Node buildConversionPane() {
        BorderPane root = new BorderPane();
        root.setPadding(new Insets(10));

        if (clientJobPlugins.isEmpty()) {
            Label message = new Label("No client job plugins were found. Add a plugin with META-INF/services/client.ClientJobPlugin.");
            message.setWrapText(true);
            VBox emptyState = new VBox(10, message);
            emptyState.setPadding(new Insets(20));
            root.setCenter(emptyState);
            return root;
        }

        HBox topBar = new HBox(10);

        ComboBox<ClientJobPlugin> jobTypeBox = new ComboBox<>();
        configurePluginComboBox(jobTypeBox);
        jobTypeBox.getItems().addAll(clientJobPlugins);
        jobTypeBox.getSelectionModel().selectFirst();

        ComboBox<String> formatBox = new ComboBox<>();
        refreshParameterOptions(jobTypeBox.getValue(), formatBox);

        Button uploadBtn = new Button("Upload Files");
        Button startBtn = new Button("Start Job");
        ProgressIndicator busyIndicator = new ProgressIndicator();
        busyIndicator.setMaxSize(18, 18);
        busyIndicator.setVisible(false);
        Label statusLabel = new Label();
        Button cancelBtn = new Button("Cancel");
        cancelBtn.setVisible(false);
        cancelBtn.setDisable(true);
        topBar.getChildren().addAll(
                new Label("Job Type:"), jobTypeBox,
                new Label("Target:"), formatBox,
                uploadBtn, startBtn, busyIndicator, statusLabel, cancelBtn);
        List<Node> busyControls = List.of(jobTypeBox, formatBox, uploadBtn, startBtn);

        jobTypeBox.setOnAction(e -> {
            refreshParameterOptions(jobTypeBox.getValue(), formatBox);
            clearStagedInputs();
            gallery.getChildren().clear();
        });

        gallery = new TilePane();
        gallery.setHgap(10);
        gallery.setVgap(10);
        gallery.setPadding(new Insets(15));
        ScrollPane scroll = new ScrollPane(gallery);
        scroll.setFitToWidth(true);

        root.setTop(topBar);
        root.setCenter(scroll);

        startBtn.setOnAction(e -> {
            try {
                CoordinatorConnection connection = networkConnection;
                PrintWriter out = connection == null ? null : connection.writer();
                if (connection == null || out == null || !connection.isOpen()) {
                    new Alert(Alert.AlertType.ERROR, "Not connected to the coordinator yet.").show();
                    return;
                }

                List<Path> inputPaths = stagedInputFiles();
                if (inputPaths.isEmpty()) {
                    new Alert(Alert.AlertType.WARNING, "No files to process. Upload files first.").show();
                    return;
                }

                ClientJobPlugin plugin = jobTypeBox.getValue();
                String selectedFormat = formatBox.getValue();
                if (plugin == null || selectedFormat == null || selectedFormat.isBlank()) {
                    new Alert(Alert.AlertType.ERROR, "Select a job type and target before starting.").show();
                    return;
                }

                String targetFormat = plugin.normalizeParameter(selectedFormat);
                Task<GuiJobSubmitter.SubmittedJob> submitTask =
                        createSubmitJobTask(connection, out, plugin, inputPaths, targetFormat);
                runGuiBackgroundTask(
                        "Submit job",
                        submitTask,
                        busyControls,
                        busyIndicator,
                        statusLabel,
                        cancelBtn,
                        submittedJob -> {
                            if (submittedJob == null || submittedJob.jobId() == null) {
                                return;
                            }
                            gallery.getChildren().clear();
                            clearStagedInputs();
                            if (submittedJob.activeAfterSend()) {
                                new Alert(Alert.AlertType.CONFIRMATION,
                                        submittedJob.plugin().displayName()
                                                + " started. The gallery will be cleared.").show();
                            }
                        });
            } catch (Exception ex) {
                LOGGER.error("event=gui_job_submit_failed error={}", ex.getMessage(), ex);
                new Alert(Alert.AlertType.ERROR, "Error: " + ex.getMessage()).show();
            }
        });

        uploadBtn.setOnAction(e -> {
            ClientJobPlugin plugin = jobTypeBox.getValue();
            if (plugin == null) {
                new Alert(Alert.AlertType.ERROR, "No job type is selected.").show();
                return;
            }

            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Select " + plugin.displayName() + " Inputs");
            List<String> extensionPatterns = plugin.supportedInputExtensions().stream()
                    .map(PeerApp::fileChooserPattern)
                    .toList();
            if (extensionPatterns.isEmpty()) {
                fileChooser.getExtensionFilters().add(
                        new FileChooser.ExtensionFilter("All Files", "*.*")
                );
            } else {
                fileChooser.getExtensionFilters().add(
                        new FileChooser.ExtensionFilter(plugin.displayName() + " Inputs", extensionPatterns)
                );
            }

            List<File> selectedFiles = fileChooser.showOpenMultipleDialog(window);
            if (selectedFiles != null) {
                try {
                    List<Path> sources = selectedFiles.stream()
                            .map(File::toPath)
                            .toList();
                    Task<List<InputStaging.StagedInput>> stageTask = createStageInputsTask(sources);
                    runGuiBackgroundTask(
                            "Stage inputs",
                            stageTask,
                            busyControls,
                            busyIndicator,
                            statusLabel,
                            cancelBtn,
                            stagedInputs -> stagedInputs.forEach(this::addStagedInputCard));
                } catch (Exception ex) {
                    LOGGER.warn("event=gui_upload_stage_start_failed error={}", ex.getMessage(), ex);
                    new Alert(Alert.AlertType.ERROR, "Could not stage selected files: " + ex.getMessage()).show();
                }
            }
        });

        return root;
    }

    private Task<List<InputStaging.StagedInput>> createStageInputsTask(List<Path> sources) {
        return new Task<>() {
            @Override
            protected List<InputStaging.StagedInput> call() throws Exception {
                updateMessage("Staging " + sources.size() + " file(s)...");
                return InputStaging.stageFiles(sources, Paths.get(currentInPath), this::isCancelled);
            }
        };
    }

    private Task<GuiJobSubmitter.SubmittedJob> createSubmitJobTask(
            CoordinatorConnection connection,
            PrintWriter out,
            ClientJobPlugin plugin,
            List<Path> inputPaths,
            String targetFormat) {
        return new Task<>() {
            @Override
            protected GuiJobSubmitter.SubmittedJob call() throws Exception {
                updateMessage("Preparing " + inputPaths.size() + " input(s)...");
                List<Object> payloads = plugin.buildPayloads(inputPaths, targetFormat);
                if (isCancelled()) {
                    return null;
                }

                updateMessage("Submitting job...");
                GuiJobSubmitter.SubmittedJob submittedJob = GuiJobSubmitter.submitPreparedPayloads(
                        jobSubmissionClient,
                        plugin,
                        payloads,
                        targetFormat,
                        out,
                        () -> networkConnection == connection,
                        () -> clearNetworkState(connection, true),
                        myActiveJobIds);
                if (submittedJob.jobId() != null) {
                    LOGGER.info("event=gui_job_submitted job_id={} task_type={}",
                            submittedJob.jobId(), plugin.taskType());
                }
                return submittedJob;
            }
        };
    }

    private <T> void runGuiBackgroundTask(
            String taskName,
            Task<T> task,
            List<? extends Node> disabledNodes,
            ProgressIndicator busyIndicator,
            Label statusLabel,
            Button cancelBtn,
            Consumer<T> onSucceeded) {
        activeBackgroundTask = task;
        setControlsDisabled(disabledNodes, true);
        busyIndicator.progressProperty().bind(task.progressProperty());
        busyIndicator.setVisible(true);
        statusLabel.textProperty().bind(task.messageProperty());
        cancelBtn.setVisible(true);
        cancelBtn.setDisable(false);
        cancelBtn.setOnAction(event -> task.cancel(true));

        task.setOnSucceeded(event -> {
            finishGuiBackgroundTask(task, disabledNodes, busyIndicator, statusLabel, cancelBtn);
            statusLabel.setText(taskName + " complete.");
            onSucceeded.accept(task.getValue());
        });
        task.setOnFailed(event -> {
            finishGuiBackgroundTask(task, disabledNodes, busyIndicator, statusLabel, cancelBtn);
            Throwable error = task.getException();
            String message = error == null || error.getMessage() == null
                    ? "Unknown error"
                    : error.getMessage();
            statusLabel.setText(taskName + " failed.");
            LOGGER.error("event=gui_background_task_failed task={} error={}", taskName, message, error);
            new Alert(Alert.AlertType.ERROR, taskName + " failed: " + message).show();
        });
        task.setOnCancelled(event -> {
            finishGuiBackgroundTask(task, disabledNodes, busyIndicator, statusLabel, cancelBtn);
            statusLabel.setText(taskName + " cancelled.");
        });

        Thread worker = new Thread(task, "gui-" + taskName.toLowerCase(Locale.ROOT).replace(' ', '-'));
        worker.setDaemon(true);
        worker.start();
    }

    private void finishGuiBackgroundTask(
            Task<?> task,
            List<? extends Node> disabledNodes,
            ProgressIndicator busyIndicator,
            Label statusLabel,
            Button cancelBtn) {
        if (activeBackgroundTask == task) {
            activeBackgroundTask = null;
        }
        setControlsDisabled(disabledNodes, false);
        busyIndicator.progressProperty().unbind();
        busyIndicator.setVisible(false);
        statusLabel.textProperty().unbind();
        cancelBtn.setOnAction(null);
        cancelBtn.setDisable(true);
        cancelBtn.setVisible(false);
    }

    private void setControlsDisabled(List<? extends Node> nodes, boolean disabled) {
        nodes.forEach(node -> node.setDisable(disabled));
    }

    private void addStagedInputCard(InputStaging.StagedInput stagedInput) {
        VBox fileCard = new VBox(5);
        fileCard.setStyle("-fx-border-color: #ccc; -fx-padding: 5; -fx-background-color: #eee;");
        Label fileName = new Label(stagedInput.displayName());
        fileName.setTooltip(new Tooltip(stagedInput.sourcePath().toString()));
        fileCard.getChildren().add(fileName);
        gallery.getChildren().add(fileCard);
    }

    private void configurePluginComboBox(ComboBox<ClientJobPlugin> jobTypeBox) {
        jobTypeBox.setCellFactory(listView -> pluginListCell());
        jobTypeBox.setButtonCell(pluginListCell());
    }

    private ListCell<ClientJobPlugin> pluginListCell() {
        return new ListCell<>() {
            @Override
            protected void updateItem(ClientJobPlugin plugin, boolean empty) {
                super.updateItem(plugin, empty);
                setText(empty || plugin == null ? null : plugin.displayName());
            }
        };
    }

    private void refreshParameterOptions(ClientJobPlugin plugin, ComboBox<String> formatBox) {
        formatBox.getItems().clear();
        if (plugin == null) {
            return;
        }
        formatBox.getItems().addAll(plugin.parameterOptions());
        String defaultParameter = plugin.defaultParameter();
        if (defaultParameter != null && formatBox.getItems().contains(defaultParameter)) {
            formatBox.setValue(defaultParameter);
        } else if (!formatBox.getItems().isEmpty()) {
            formatBox.getSelectionModel().selectFirst();
        }
    }

    private List<Path> stagedInputFiles() throws IOException {
        return InputStaging.stagedInputFiles(Paths.get(currentInPath));
    }

    private void clearStagedInputs() {
        try {
            InputStaging.clear(Paths.get(currentInPath));
        } catch (IOException deleteError) {
            LOGGER.warn("event=temp_inputs_clear_failed error={}", deleteError.getMessage(), deleteError);
        }
    }

    private static String fileChooserPattern(String extension) {
        if (extension == null || extension.isBlank()) {
            return "*.*";
        }
        String value = extension.trim().toLowerCase(Locale.ROOT);
        if (value.startsWith("*.")) {
            return value;
        }
        if (value.startsWith(".")) {
            return "*" + value;
        }
        return "*." + value;
    }

    private Node buildHistoryPane() {
        try {
            if (historyStore == null) {
                historyStore = new DatabaseGuiHistoryStore();
            }
        } catch (Exception e) {
            Label err = new Label("Job history unavailable: " + e.getMessage());
            err.setPadding(new Insets(20));
            return err;
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

        // ---- Jobs table ----
        TableView<GuiHistoryStore.JobRecord> jobTable = new TableView<>();
        jobTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        jobTable.setPlaceholder(new Label("No jobs recorded yet. Run the coordinator and submit a job."));

        TableColumn<GuiHistoryStore.JobRecord, String> colType = new TableColumn<>("Type");
        colType.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().taskType()));

        TableColumn<GuiHistoryStore.JobRecord, String> colStatus = new TableColumn<>("Status");
        colStatus.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().status()));

        TableColumn<GuiHistoryStore.JobRecord, Number> colFiles = new TableColumn<>("Files");
        colFiles.setCellValueFactory(d -> new SimpleIntegerProperty(d.getValue().fileCount()));
        colFiles.setMaxWidth(60);

        TableColumn<GuiHistoryStore.JobRecord, String> colSubmitted = new TableColumn<>("Submitted");
        colSubmitted.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().submittedAt() == 0 ? "-" : fmt.format(Instant.ofEpochMilli(d.getValue().submittedAt()))
        ));

        TableColumn<GuiHistoryStore.JobRecord, String> colDuration = new TableColumn<>("Duration");
        colDuration.setCellValueFactory(d -> {
            long s = d.getValue().submittedAt();
            long c = d.getValue().completedAt();
            String val = (s > 0 && c > 0) ? ((c - s) / 1000.0) + " s" : "-";
            return new SimpleStringProperty(val);
        });

        TableColumn<GuiHistoryStore.JobRecord, String> colJobId = new TableColumn<>("Job ID");
        colJobId.setCellValueFactory(d -> {
            String id = d.getValue().jobId();
            return new SimpleStringProperty(id.length() > 12 ? id.substring(0, 12) + "..." : id);
        });

        jobTable.getColumns().add(colType);
        jobTable.getColumns().add(colStatus);
        jobTable.getColumns().add(colFiles);
        jobTable.getColumns().add(colSubmitted);
        jobTable.getColumns().add(colDuration);
        jobTable.getColumns().add(colJobId);

        // ---- Tasks table ----
        TableView<GuiHistoryStore.TaskRecord> taskTable = new TableView<>();
        taskTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        taskTable.setPlaceholder(new Label("Select a job above to see its tasks."));

        TableColumn<GuiHistoryStore.TaskRecord, String> tColPeer = new TableColumn<>("Peer");
        tColPeer.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().assignedPeerId() != null ? d.getValue().assignedPeerId() : "-"
        ));

        TableColumn<GuiHistoryStore.TaskRecord, String> tColStatus = new TableColumn<>("Status");
        tColStatus.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().status()));

        TableColumn<GuiHistoryStore.TaskRecord, String> tColDuration = new TableColumn<>("Duration");
        tColDuration.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().durationMs() > 0 ? d.getValue().durationMs() + " ms" : "-"
        ));

        TableColumn<GuiHistoryStore.TaskRecord, Number> tColRetries = new TableColumn<>("Retries");
        tColRetries.setCellValueFactory(d -> new SimpleIntegerProperty(d.getValue().retryCount()));
        tColRetries.setMaxWidth(70);

        TableColumn<GuiHistoryStore.TaskRecord, String> tColTaskId = new TableColumn<>("Task ID");
        tColTaskId.setCellValueFactory(d -> {
            String id = d.getValue().taskId();
            return new SimpleStringProperty(id.length() > 12 ? id.substring(0, 12) + "..." : id);
        });

        taskTable.getColumns().add(tColPeer);
        taskTable.getColumns().add(tColStatus);
        taskTable.getColumns().add(tColDuration);
        taskTable.getColumns().add(tColRetries);
        taskTable.getColumns().add(tColTaskId);

        // When a job row is selected, populate the task table
        jobTable.getSelectionModel().selectedItemProperty().addListener((obs, old, sel) -> {
            if (sel != null) {
                taskTable.getItems().setAll(historyStore.getTasksForJob(sel.jobId()));
            }
        });

        // ---- Layout ----
        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> {
            jobTable.getItems().setAll(historyStore.getJobHistory());
            taskTable.getItems().clear();
        });

        Label title = new Label("Job History");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");
        HBox topBar = new HBox(10, title, refreshBtn);
        topBar.setPadding(new Insets(0, 0, 8, 0));

        Label jobsLabel  = new Label("Jobs");
        Label tasksLabel = new Label("Tasks for Selected Job");

        VBox jobsSection  = new VBox(4, jobsLabel,  jobTable);
        VBox tasksSection = new VBox(4, tasksLabel, taskTable);
        VBox.setVgrow(jobTable,  Priority.ALWAYS);
        VBox.setVgrow(taskTable, Priority.ALWAYS);

        SplitPane split = new SplitPane(jobsSection, tasksSection);
        split.setOrientation(Orientation.VERTICAL);
        split.setDividerPositions(0.55);
        VBox.setVgrow(split, Priority.ALWAYS);

        VBox root = new VBox(10, topBar, split);
        root.setPadding(new Insets(12));

        // Load immediately when the pane is built
        jobTable.getItems().setAll(historyStore.getJobHistory());

        return root;
    }

    private void startNetworkThread(String host, int port, Runnable onConnected, java.util.function.Consumer<String> onFailed) {
        stopping = false;
        TcpCoordinatorConnection.Listener listener = new TcpCoordinatorConnection.Listener() {
            @Override
            public void onConnected(CoordinatorConnection connection) {
                onConnected.run();
            }

            @Override
            public void onConnectionFailed(CoordinatorConnection connection, String error) {
                clearNetworkState(connection, false);
                onFailed.accept(error);
            }

            @Override
            public void onDisconnected(CoordinatorConnection connection, String message) {
                clearNetworkState(connection, false);
                if (stopping) {
                    return;
                }
                LOGGER.warn("event=gui_connection_lost message={}", message);
                Platform.runLater(() -> new Alert(Alert.AlertType.WARNING, message).show());
            }

            @Override
            public void onJobResult(CoordinatorConnection connection, JobResultMessage result) {
                handleJobResult(result);
            }
        };

        TcpCoordinatorConnection connection = new TcpCoordinatorConnection(host, port, workerRuntime, listener);
        networkConnection = connection;
        connection.start();
    }

    private void clearNetworkState(CoordinatorConnection connection, boolean closeConnection) {
        if (connection == null) {
            return;
        }
        if (networkConnection == connection) {
            networkConnection = null;
        }
        if (closeConnection) {
            connection.close();
        }
    }

    private void handleJobResult(JobResultMessage result) {
        GuiJobResultRouter.RoutedJobResult routed = GuiJobResultRouter.route(result, myActiveJobIds);
        if (routed.action() == GuiJobResultRouter.Action.IGNORE) {
            return;
        }

        Platform.runLater(() -> {
            if (routed.action() == GuiJobResultRouter.Action.SHOW_DOWNLOAD) {
                showDownloadWindow(routed.result());
            } else {
                showFailureWindow(routed.result());
            }
        });
    }

    private void showFailureWindow(JobResultMessage result) {
        String message = GuiJobResultRouter.failureMessage(result);
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Job Failed");
        alert.setHeaderText("Job " + result.getJobId() + " failed");
        alert.setContentText(message);
        alert.show();
    }

    private void showDownloadWindow(JobResultMessage result) {
        Stage downloadStage = new Stage();
        VBox layout = new VBox(15);
        layout.setPadding(new Insets(20));

        layout.getChildren().add(new Label("Job Complete! Select a location to save your files."));

        Button saveBtn = new Button("Choose Folder & Save");
        saveBtn.setStyle("-fx-base: #2ecc71; -fx-text-fill: white;");

        saveBtn.setOnAction(e -> {
            // 1. Let the user pick where to save
            DirectoryChooser directoryChooser = new DirectoryChooser();
            directoryChooser.setTitle("Select Save Location");
            File selectedDirectory = directoryChooser.showDialog(downloadStage);

            if (selectedDirectory != null) {
                if (saveFilesToDisk(result, selectedDirectory.getAbsolutePath())) {
                    downloadStage.close();
                    new Alert(Alert.AlertType.INFORMATION, "Files saved successfully!").show();
                }
            }
        });

        layout.getChildren().addAll(saveBtn);
        downloadStage.setScene(new Scene(layout, 300, 200));
        downloadStage.setTitle("Download Converted Files");
        downloadStage.show();
    }

    private boolean saveFilesToDisk(JobResultMessage result, String folderPath) {
        GuiResultSaver.SaveResult saveResult = GuiResultSaver.save(result, folderPath, clientJobPluginsByType);
        if (!saveResult.successful()) {
            if (saveResult.cause() == null) {
                LOGGER.warn("event=gui_results_save_failed job_id={} task_type={} error={}",
                        result.getJobId(), result.getTaskType(), saveResult.errorMessage());
            } else {
                LOGGER.warn("event=gui_results_save_failed job_id={} task_type={} error={}",
                        result.getJobId(), result.getTaskType(), saveResult.errorMessage(), saveResult.cause());
            }
            new Alert(Alert.AlertType.ERROR, saveResult.errorMessage()).show();
            return false;
        }

        LOGGER.info("event=gui_results_saved job_id={} task_type={} output_dir={}",
                result.getJobId(), result.getTaskType(), saveResult.outputDir());
        return true;
    }

    @Override
    public void stop() throws Exception {
        stopping = true;
        Task<?> backgroundTask = activeBackgroundTask;
        if (backgroundTask != null) {
            backgroundTask.cancel(true);
        }
        CoordinatorConnection connection = networkConnection;
        if (connection != null) {
            clearNetworkState(connection, true);
        }
        if (workerRuntime != null) {
            workerRuntime.shutdown();
            if (!workerRuntime.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                LOGGER.warn("event=gui_engine_shutdown_timeout peer_id={}", sessionId);
            }
        }
        if (historyStore != null) {
            historyStore.close();
        }
        super.stop();
    }

    public static void main(String[] args) {
        launch(args);
    }

}
