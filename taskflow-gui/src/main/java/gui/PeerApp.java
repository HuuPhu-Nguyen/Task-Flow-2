package gui;

import client.ClientJobPlugin;
import client.ClientJobPlugins;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.JobResultMessage;
import protocol.PeerIdentity;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.Map;

public class PeerApp extends Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeerApp.class);

    private Stage window;
    private GuiWorkerRuntime workerRuntime;
    private JobSubmissionClient jobSubmissionClient;
    private GuiJobSubmissionService jobSubmissionService;
    private final java.util.Set<String> myActiveJobIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private GuiCoordinatorConnectionService connectionService;
    private GuiInputStagingService inputStagingService;
    private final GuiBackgroundTaskRunner backgroundTaskRunner = new GuiBackgroundTaskRunner();

    private String sessionId;
    private TilePane gallery;
    private GuiHistoryView historyView;
    private List<ClientJobPlugin> clientJobPlugins = List.of();
    private GuiResultSaveService resultSaveService = new GuiResultSaveService(Map.of());
    private GuiDownloadWindow downloadWindow =
            new GuiDownloadWindow(new GuiDownloadSaveController(resultSaveService::save));
    private GuiTransportMode transportMode = GuiTransportMode.TCP;
    private String peerNodeId;

    @Override
    public void init() {
        try {
            // Generate unique folders for this GUI instance
            this.sessionId = "PEER_" + (System.currentTimeMillis() % 100000);
            this.transportMode = GuiTransportMode.fromEnvironment();
            this.peerNodeId = PeerIdentity.configuredOrGenerated("GUI_PEER");
            inputStagingService = GuiInputStagingService.forSession(sessionId);
            inputStagingService.prepareDirectories();

            workerRuntime = new PeerEngineWorkerRuntime(peerNodeId);
            GuiRequesterTokenStore tokenStore = FileGuiRequesterTokenStore.openDefault();
            if (transportMode == GuiTransportMode.RABBITMQ) {
                jobSubmissionClient = new RabbitMqJobSubmissionClient(peerNodeId, tokenStore);
                connectionService = new GuiCoordinatorConnectionService(
                        workerRuntime,
                        (host, port, runtime, listener) ->
                                new RabbitMqCoordinatorConnection(peerNodeId, host, port, runtime, listener));
            } else {
                jobSubmissionClient = new TcpJobSubmissionClient(peerNodeId, tokenStore);
                connectionService = new GuiCoordinatorConnectionService(
                        workerRuntime,
                        (host, port, runtime, listener) ->
                                new TcpCoordinatorConnection(peerNodeId, host, port, runtime, listener));
            }
            jobSubmissionService = new GuiJobSubmissionService(jobSubmissionClient, myActiveJobIds);
            LOGGER.info("event=gui_processors_registered transport={} peer_id={} task_types={}",
                    transportMode.name().toLowerCase(), peerNodeId, workerRuntime.supportedTaskTypes());
            clientJobPlugins = ClientJobPlugins.discover();
            Map<String, ClientJobPlugin> clientJobPluginsByType = ClientJobPlugins.byTaskType(clientJobPlugins);
            resultSaveService = new GuiResultSaveService(clientJobPluginsByType);
            downloadWindow = new GuiDownloadWindow(new GuiDownloadSaveController(resultSaveService::save));
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
        TextField portField = new TextField(Integer.toString(transportMode.defaultPort()));
        Button connectBtn = new Button(transportMode.connectButtonText());

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
            startCoordinatorConnection(
                    hostField.getText(),
                    port,
                    () -> Platform.runLater(this::showMainGallery),
                    error -> Platform.runLater(() -> {
                        connectBtn.setDisable(false);
                        new Alert(Alert.AlertType.ERROR,
                                "Could not connect to coordinator: " + error).show();
                    }));
        });

        root.getChildren().addAll(
                new Label(transportMode.hostLabel() + ":"),
                hostField,
                new Label(transportMode.portLabel() + ":"),
                portField,
                connectBtn);
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
                CoordinatorConnection connection = connectionService.currentConnection();
                if (connection == null || !connection.isOpen()) {
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
                        createSubmitJobTask(connection, plugin, inputPaths, targetFormat);
                backgroundTaskRunner.run(
                        "Submit job",
                        submitTask,
                        new GuiBackgroundTaskControls(busyControls, busyIndicator, statusLabel, cancelBtn),
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
                    .map(GuiInputStagingService::fileChooserPattern)
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
                    backgroundTaskRunner.run(
                            "Stage inputs",
                            stageTask,
                            new GuiBackgroundTaskControls(busyControls, busyIndicator, statusLabel, cancelBtn),
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
                return inputStagingService.stageFiles(sources, this::isCancelled);
            }
        };
    }

    private Task<GuiJobSubmitter.SubmittedJob> createSubmitJobTask(
            CoordinatorConnection connection,
            ClientJobPlugin plugin,
            List<Path> inputPaths,
            String targetFormat) {
        return new Task<>() {
            @Override
            protected GuiJobSubmitter.SubmittedJob call() throws Exception {
                updateMessage("Preparing " + inputPaths.size() + " input(s)...");
                GuiJobSubmitter.SubmittedJob submittedJob = jobSubmissionService.submit(
                        plugin,
                        inputPaths,
                        targetFormat,
                        connection,
                        () -> connectionService.isCurrent(connection),
                        () -> connectionService.clear(connection, true),
                        this::isCancelled,
                        () -> updateMessage("Submitting job..."));
                if (submittedJob == null) {
                    return null;
                }
                if (submittedJob.jobId() != null) {
                    LOGGER.info("event=gui_job_submitted job_id={} task_type={}",
                            submittedJob.jobId(), plugin.taskType());
                }
                return submittedJob;
            }
        };
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
        return inputStagingService.stagedInputFiles();
    }

    private void clearStagedInputs() {
        try {
            inputStagingService.clearStagedInputs();
        } catch (IOException deleteError) {
            LOGGER.warn("event=temp_inputs_clear_failed error={}", deleteError.getMessage(), deleteError);
        }
    }

    private Node buildHistoryPane() {
        try {
            if (historyView == null) {
                historyView = GuiHistoryView.openDefault();
            }
            return historyView.build();
        } catch (Exception e) {
            Label err = new Label("Job history unavailable: " + e.getMessage());
            err.setPadding(new Insets(20));
            return err;
        }
    }

    private void startCoordinatorConnection(String host, int port, Runnable onConnected, java.util.function.Consumer<String> onFailed) {
        connectionService.start(host, port, new GuiCoordinatorConnectionService.Listener() {
            @Override
            public void onConnected() {
                onConnected.run();
            }

            @Override
            public void onConnectionFailed(String error) {
                onFailed.accept(error);
            }

            @Override
            public void onDisconnected(String message) {
                LOGGER.warn("event=gui_connection_lost message={}", message);
                Platform.runLater(() -> new Alert(Alert.AlertType.WARNING, message).show());
            }

            @Override
            public void onJobResult(JobResultMessage result) {
                handleJobResult(result);
            }
        });
    }

    private void handleJobResult(JobResultMessage result) {
        GuiJobResultRouter.RoutedJobResult routed = GuiJobResultRouter.route(result, myActiveJobIds);
        if (routed.action() == GuiJobResultRouter.Action.IGNORE) {
            return;
        }

        Platform.runLater(() -> {
            if (routed.action() == GuiJobResultRouter.Action.SHOW_DOWNLOAD) {
                downloadWindow.show(routed.result());
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

    @Override
    public void stop() throws Exception {
        backgroundTaskRunner.cancelActiveTask();
        if (connectionService != null) {
            connectionService.stop();
        }
        if (workerRuntime != null) {
            workerRuntime.shutdown();
            if (!workerRuntime.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                LOGGER.warn("event=gui_engine_shutdown_timeout peer_id={}", peerNodeId);
            }
        }
        if (historyView != null) {
            historyView.close();
        }
        super.stop();
    }

    public static void main(String[] args) {
        if (GuiUsage.isHelpRequested(args)) {
            System.out.println(GuiUsage.usage());
            return;
        }
        launch(args);
    }

}
