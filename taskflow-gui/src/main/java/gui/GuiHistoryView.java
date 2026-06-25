package gui;

import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.Button;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.layout.HBox;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

final class GuiHistoryView implements AutoCloseable {
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final GuiHistoryStore historyStore;

    private GuiHistoryView(GuiHistoryStore historyStore) {
        this.historyStore = historyStore;
    }

    static GuiHistoryView openDefault() throws Exception {
        return new GuiHistoryView(new DatabaseGuiHistoryStore());
    }

    Node build() {
        TableView<GuiHistoryStore.JobRecord> jobTable = createJobTable();
        TableView<GuiHistoryStore.TaskRecord> taskTable = createTaskTable();

        jobTable.getSelectionModel().selectedItemProperty().addListener((obs, old, selected) -> {
            if (selected != null) {
                taskTable.getItems().setAll(historyStore.getTasksForJob(selected.jobId()));
            }
        });

        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> {
            jobTable.getItems().setAll(historyStore.getJobHistory());
            taskTable.getItems().clear();
        });

        Label title = new Label("Job History");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");
        HBox topBar = new HBox(10, title, refreshBtn);
        topBar.setPadding(new Insets(0, 0, 8, 0));

        VBox jobsSection = new VBox(4, new Label("Jobs"), jobTable);
        VBox tasksSection = new VBox(4, new Label("Tasks for Selected Job"), taskTable);
        VBox.setVgrow(jobTable, Priority.ALWAYS);
        VBox.setVgrow(taskTable, Priority.ALWAYS);

        SplitPane split = new SplitPane(jobsSection, tasksSection);
        split.setOrientation(Orientation.VERTICAL);
        split.setDividerPositions(0.55);
        VBox.setVgrow(split, Priority.ALWAYS);

        VBox root = new VBox(10, topBar, split);
        root.setPadding(new Insets(12));

        jobTable.getItems().setAll(historyStore.getJobHistory());
        return root;
    }

    private TableView<GuiHistoryStore.JobRecord> createJobTable() {
        TableView<GuiHistoryStore.JobRecord> jobTable = new TableView<>();
        jobTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        jobTable.setPlaceholder(new Label("No jobs recorded yet. Run the coordinator and submit a job."));

        TableColumn<GuiHistoryStore.JobRecord, String> typeColumn = new TableColumn<>("Type");
        typeColumn.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().taskType()));

        TableColumn<GuiHistoryStore.JobRecord, String> statusColumn = new TableColumn<>("Status");
        statusColumn.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().status()));

        TableColumn<GuiHistoryStore.JobRecord, Number> filesColumn = new TableColumn<>("Files");
        filesColumn.setCellValueFactory(d -> new SimpleIntegerProperty(d.getValue().fileCount()));
        filesColumn.setMaxWidth(60);

        TableColumn<GuiHistoryStore.JobRecord, String> submittedColumn = new TableColumn<>("Submitted");
        submittedColumn.setCellValueFactory(d -> new SimpleStringProperty(formatTimestamp(d.getValue().submittedAt())));

        TableColumn<GuiHistoryStore.JobRecord, String> durationColumn = new TableColumn<>("Duration");
        durationColumn.setCellValueFactory(d -> new SimpleStringProperty(formatDurationSeconds(
                d.getValue().submittedAt(),
                d.getValue().completedAt())));

        TableColumn<GuiHistoryStore.JobRecord, String> jobIdColumn = new TableColumn<>("Job ID");
        jobIdColumn.setCellValueFactory(d -> new SimpleStringProperty(abbreviateId(d.getValue().jobId())));

        jobTable.getColumns().add(typeColumn);
        jobTable.getColumns().add(statusColumn);
        jobTable.getColumns().add(filesColumn);
        jobTable.getColumns().add(submittedColumn);
        jobTable.getColumns().add(durationColumn);
        jobTable.getColumns().add(jobIdColumn);
        return jobTable;
    }

    private TableView<GuiHistoryStore.TaskRecord> createTaskTable() {
        TableView<GuiHistoryStore.TaskRecord> taskTable = new TableView<>();
        taskTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        taskTable.setPlaceholder(new Label("Select a job above to see its tasks."));

        TableColumn<GuiHistoryStore.TaskRecord, String> peerColumn = new TableColumn<>("Peer");
        peerColumn.setCellValueFactory(d -> new SimpleStringProperty(
                d.getValue().assignedPeerId() != null ? d.getValue().assignedPeerId() : "-"
        ));

        TableColumn<GuiHistoryStore.TaskRecord, String> statusColumn = new TableColumn<>("Status");
        statusColumn.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().status()));

        TableColumn<GuiHistoryStore.TaskRecord, String> durationColumn = new TableColumn<>("Duration");
        durationColumn.setCellValueFactory(d -> new SimpleStringProperty(
                d.getValue().durationMs() > 0 ? d.getValue().durationMs() + " ms" : "-"
        ));

        TableColumn<GuiHistoryStore.TaskRecord, Number> retriesColumn = new TableColumn<>("Retries");
        retriesColumn.setCellValueFactory(d -> new SimpleIntegerProperty(d.getValue().retryCount()));
        retriesColumn.setMaxWidth(70);

        TableColumn<GuiHistoryStore.TaskRecord, String> taskIdColumn = new TableColumn<>("Task ID");
        taskIdColumn.setCellValueFactory(d -> new SimpleStringProperty(abbreviateId(d.getValue().taskId())));

        taskTable.getColumns().add(peerColumn);
        taskTable.getColumns().add(statusColumn);
        taskTable.getColumns().add(durationColumn);
        taskTable.getColumns().add(retriesColumn);
        taskTable.getColumns().add(taskIdColumn);
        return taskTable;
    }

    static String abbreviateId(String id) {
        if (id == null) {
            return "";
        }
        return id.length() > 12 ? id.substring(0, 12) + "..." : id;
    }

    static String formatDurationSeconds(long submittedAt, long completedAt) {
        return submittedAt > 0 && completedAt > 0 ? ((completedAt - submittedAt) / 1000.0) + " s" : "-";
    }

    static String formatTimestamp(long epochMillis) {
        return epochMillis == 0 ? "-" : TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(epochMillis));
    }

    @Override
    public void close() {
        historyStore.close();
    }
}
