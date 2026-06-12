package gui;

import com.google.gson.Gson;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import messaging.MessageDispatcher;
import messaging.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peer.PeerNode;
import peer.engine.PeerExecutionEngine;
import protocol.*;
import protocol.SafeFileNames;
import server.db.DatabaseManager;

import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class PeerApp extends Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeerApp.class);

    private Stage window;
    private PeerExecutionEngine engine;
    private final Gson gson = new Gson();
    private final java.util.Set<String> myActiveJobIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
    // Shared output stream for thread-safe communication
    private PeerNode backendNode;
    private PrintWriter socketOut;

    private String currentInPath;
    private String currentOutPath;
    private String sessionId;
    private TilePane gallery;
    private DatabaseManager historyDb;

    @Override
    public void init() {
        try {
            // Generate unique folders for this GUI instance
            this.sessionId = "PEER_" + (System.currentTimeMillis() % 100000);
            this.currentInPath = "java/in_" + sessionId;
            this.currentOutPath = "java/out_" + sessionId;

            FileUtils.prepareDirectories(currentInPath, currentOutPath);

            // Initialize Engine with the same unique ID
            engine = new PeerExecutionEngine(sessionId);
            LOGGER.info("event=gui_processors_registered peer_id={} task_types={}",
                    sessionId, engine.getRegisteredTaskTypes());
        } catch (IOException e) {
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

        HBox topBar = new HBox(10);

        ComboBox<String> jobTypeBox = new ComboBox<>();
        jobTypeBox.getItems().addAll("Image Conversion", "Video Transcoding");
        jobTypeBox.setValue("Image Conversion");

        ComboBox<String> formatBox = new ComboBox<>();
        formatBox.getItems().addAll("PNG", "JPG", "BMP", "GIF");
        formatBox.setValue("PNG");

        Button uploadBtn = new Button("Upload Files");
        Button startBtn = new Button("Start Conversion");
        topBar.getChildren().addAll(new Label("Job Type:"), jobTypeBox, new Label("Target:"), formatBox, uploadBtn, startBtn);

        jobTypeBox.setOnAction(e -> {
            String jobType = jobTypeBox.getValue();
            formatBox.getItems().clear();
            if ("Image Conversion".equals(jobType)) {
                formatBox.getItems().addAll("PNG", "JPG", "BMP", "GIF");
                formatBox.setValue("PNG");
            } else {
                formatBox.getItems().addAll("MP4", "AVI", "MKV", "MOV", "WEBM");
                formatBox.setValue("MP4");
            }
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
                if (socketOut == null) {
                    new Alert(Alert.AlertType.ERROR, "Not connected to the coordinator yet.").show();
                    return;
                }

                File folder = new File(currentInPath);
                File[] files = folder.listFiles();
                if (files == null || files.length == 0) {
                    new Alert(Alert.AlertType.WARNING, "No files to process. Upload files first.").show();
                    return;
                }

                List<FilePayload> payloads = new ArrayList<>();
                for (File f : files) {
                    byte[] bytes = Files.readAllBytes(f.toPath());
                    payloads.add(new FilePayload(f.getName(), Base64.getEncoder().encodeToString(bytes)));
                }

                String jobType = jobTypeBox.getValue();
                String targetFormat = formatBox.getValue().toLowerCase();
                String taskType = "Image Conversion".equals(jobType)
                        ? "IMAGE_CONVERSION"
                        : "VIDEO_TRANSCODING";
                String jobId = backendNode.submitJob(taskType, payloads, targetFormat, socketOut);

                if (jobId != null) {
                    myActiveJobIds.add(jobId);
                    LOGGER.info("event=gui_job_submitted job_id={} job_type={}", jobId, jobType);

                    gallery.getChildren().clear();

                    for (File f : files) {
                        try {
                            Files.deleteIfExists(f.toPath());
                        } catch (IOException deleteError) {
                            LOGGER.warn("event=temp_file_delete_failed file={} error={}",
                                    f.getName(), deleteError.getMessage(), deleteError);
                        }
                    }

                    new Alert(Alert.AlertType.CONFIRMATION, jobType + " Started! The gallery will be cleared.").show();
                }
            } catch (Exception ex) {
                LOGGER.error("event=gui_job_submit_failed error={}", ex.getMessage(), ex);
                new Alert(Alert.AlertType.ERROR, "Error: " + ex.getMessage()).show();
            }
        });

        uploadBtn.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            String jobType = jobTypeBox.getValue();

            if ("Image Conversion".equals(jobType)) {
                fileChooser.setTitle("Select Images or PDFs");
                fileChooser.getExtensionFilters().add(
                    new FileChooser.ExtensionFilter("Images", "*.png", "*.jpg", "*.jpeg", "*.bmp", "*.gif", "*.pdf")
                );
            } else {
                fileChooser.setTitle("Select Videos");
                fileChooser.getExtensionFilters().add(
                    new FileChooser.ExtensionFilter("Videos", "*.mp4", "*.avi", "*.mkv", "*.mov", "*.webm", "*.flv", "*.wmv")
                );
            }

            List<File> selectedFiles = fileChooser.showOpenMultipleDialog(window);
            if (selectedFiles != null) {
                for (File file : selectedFiles) {
                    try {
                        Files.copy(file.toPath(), Paths.get(currentInPath, file.getName()), StandardCopyOption.REPLACE_EXISTING);
                        VBox fileCard = new VBox(5);
                        fileCard.setStyle("-fx-border-color: #ccc; -fx-padding: 5; -fx-background-color: #eee;");
                        fileCard.getChildren().add(new Label(file.getName()));
                        gallery.getChildren().add(fileCard);
                    } catch (IOException ex) {
                        LOGGER.warn("event=gui_upload_copy_failed source={} error={}",
                                file.getAbsolutePath(), ex.getMessage(), ex);
                    }
                }
            }
        });

        return root;
    }

    private Node buildHistoryPane() {
        // Open (or reuse) a read connection to the shared DB file
        try {
            if (historyDb == null) historyDb = new DatabaseManager();
        } catch (SQLException e) {
            Label err = new Label("Database unavailable: " + e.getMessage());
            err.setPadding(new Insets(20));
            return err;
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

        // ---- Jobs table ----
        TableView<DatabaseManager.JobRecord> jobTable = new TableView<>();
        jobTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        jobTable.setPlaceholder(new Label("No jobs recorded yet. Run the coordinator and submit a job."));

        TableColumn<DatabaseManager.JobRecord, String> colType = new TableColumn<>("Type");
        colType.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().taskType()));

        TableColumn<DatabaseManager.JobRecord, String> colStatus = new TableColumn<>("Status");
        colStatus.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().status()));

        TableColumn<DatabaseManager.JobRecord, Number> colFiles = new TableColumn<>("Files");
        colFiles.setCellValueFactory(d -> new SimpleIntegerProperty(d.getValue().fileCount()));
        colFiles.setMaxWidth(60);

        TableColumn<DatabaseManager.JobRecord, String> colSubmitted = new TableColumn<>("Submitted");
        colSubmitted.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().submittedAt() == 0 ? "-" : fmt.format(Instant.ofEpochMilli(d.getValue().submittedAt()))
        ));

        TableColumn<DatabaseManager.JobRecord, String> colDuration = new TableColumn<>("Duration");
        colDuration.setCellValueFactory(d -> {
            long s = d.getValue().submittedAt();
            long c = d.getValue().completedAt();
            String val = (s > 0 && c > 0) ? ((c - s) / 1000.0) + " s" : "-";
            return new SimpleStringProperty(val);
        });

        TableColumn<DatabaseManager.JobRecord, String> colJobId = new TableColumn<>("Job ID");
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
        TableView<DatabaseManager.TaskRecord> taskTable = new TableView<>();
        taskTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        taskTable.setPlaceholder(new Label("Select a job above to see its tasks."));

        TableColumn<DatabaseManager.TaskRecord, String> tColPeer = new TableColumn<>("Peer");
        tColPeer.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().assignedPeerId() != null ? d.getValue().assignedPeerId() : "-"
        ));

        TableColumn<DatabaseManager.TaskRecord, String> tColStatus = new TableColumn<>("Status");
        tColStatus.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().status()));

        TableColumn<DatabaseManager.TaskRecord, String> tColDuration = new TableColumn<>("Duration");
        tColDuration.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().durationMs() > 0 ? d.getValue().durationMs() + " ms" : "-"
        ));

        TableColumn<DatabaseManager.TaskRecord, Number> tColRetries = new TableColumn<>("Retries");
        tColRetries.setCellValueFactory(d -> new SimpleIntegerProperty(d.getValue().retryCount()));
        tColRetries.setMaxWidth(70);

        TableColumn<DatabaseManager.TaskRecord, String> tColTaskId = new TableColumn<>("Task ID");
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
                taskTable.getItems().setAll(historyDb.getTasksForJob(sel.jobId()));
            }
        });

        // ---- Layout ----
        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> {
            jobTable.getItems().setAll(historyDb.getJobHistory());
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
        jobTable.getItems().setAll(historyDb.getJobHistory());

        return root;
    }

    /**
     * Synchronized method to ensure JSON messages don't interleave on the socket.
     */
    private void sendSafe(Object message) {
        if (socketOut != null) {
            synchronized (socketOut) {
                socketOut.println(gson.toJson(message));
            }
        }
    }

    private void startNetworkThread(String host, int port, Runnable onConnected, java.util.function.Consumer<String> onFailed) {
        backendNode = new PeerNode();
        Thread netThread = new Thread(() -> {
            try (Socket socket = new Socket(host, port)) {
                socketOut = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                MessageFactory factory = createFactory();
                MessageDispatcher dispatcher = createDispatcher(engine, socketOut);
                onConnected.run();

                String line;
                while ((line = in.readLine()) != null) {
                    try {
                        if (line.trim().isEmpty()) continue;
                        Message msg = factory.fromJson(line);
                        dispatcher.dispatch(msg, socketOut);
                    } catch (Exception messageError) {
                        LOGGER.warn("event=gui_message_processing_failed error={}",
                                messageError.getMessage(), messageError);
                    }
                }
            } catch (IOException e) {
                socketOut = null;
                onFailed.accept(e.getMessage());
                Platform.runLater(() -> LOGGER.warn("event=gui_connection_lost error={}", e.getMessage(), e));
            }
        });
        netThread.setDaemon(true);
        netThread.start();
    }

    private MessageFactory createFactory() {
        MessageFactory factory = new MessageFactory();
        factory.register(MessageType.PING, json -> gson.fromJson(json, PingMessage.class));
        factory.register(MessageType.TASK_ASSIGN, json -> gson.fromJson(json, TaskAssignMessage.class));
        factory.register(MessageType.JOB_RESULT, json -> gson.fromJson(json, JobResultMessage.class));
        return factory;
    }

    private MessageDispatcher createDispatcher(PeerExecutionEngine engine, PrintWriter out) {
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(MessageType.PING, new messaging.handlers.PingHandler(() -> engine.getRegisteredTaskTypes()));
        dispatcher.register(MessageType.TASK_ASSIGN, (message, writer) -> {
            // Offload execution so the networking thread stays responsive.
            engine.submitTask((TaskAssignMessage) message, out);
        });
        dispatcher.register(MessageType.JOB_RESULT, (message, writer) -> {
            JobResultMessage result = (JobResultMessage) message;

            // 1. Check if this GUI instance actually started this job
            if (myActiveJobIds.contains(result.getJobId())) {
                myActiveJobIds.remove(result.getJobId()); // Cleanup

                Platform.runLater(() -> {
                    if (result.isSuccessful()) {
                        showDownloadWindow(result);
                    } else {
                        showFailureWindow(result);
                    }
                });
            }
        });
        return dispatcher;
    }

    private void showFailureWindow(JobResultMessage result) {
        String message = result.getErrorMessage() == null || result.getErrorMessage().isBlank()
                ? "The job failed before producing downloadable results."
                : result.getErrorMessage();
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
                // 2. Perform the actual file writing
                saveFilesToDisk(result, selectedDirectory.getAbsolutePath());
                downloadStage.close();

                // Optional: Show a success alert
                new Alert(Alert.AlertType.INFORMATION, "Files saved successfully!").show();
            }
        });

        layout.getChildren().addAll(saveBtn);
        downloadStage.setScene(new Scene(layout, 300, 200));
        downloadStage.setTitle("Download Converted Files");
        downloadStage.show();
    }

    private void saveFilesToDisk(JobResultMessage result, String folderPath) {
        Path outputDir = Paths.get(folderPath).toAbsolutePath().normalize();
        try {
            Files.createDirectories(outputDir);
        } catch (IOException ex) {
            new Alert(Alert.AlertType.ERROR, "Could not prepare output folder: " + ex.getMessage()).show();
            return;
        }

        List<Object> payloads = result.getResultsByTaskId() == null
                ? List.of()
                : result.getResultsByTaskId();
        for (Object raw : payloads) {
            FilePayload fp = gson.fromJson(gson.toJson(raw), FilePayload.class);

            try {
                byte[] data = java.util.Base64.getDecoder().decode(fp.base64Data());
                Path path = SafeFileNames.safeOutputPath(outputDir, fp.fileName());
                Files.write(path, data);

                LOGGER.info("event=gui_file_saved path={}", path.toAbsolutePath());
            } catch (Exception ex) {
                LOGGER.warn("event=gui_file_save_failed file={} error={}",
                        fp.fileName(), ex.getMessage(), ex);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if (historyDb != null) historyDb.close();
        super.stop();
    }

    public static void main(String[] args) {
        launch(args);
    }

}
