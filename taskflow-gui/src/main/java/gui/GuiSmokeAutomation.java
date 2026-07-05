package gui;

import client.ClientJobPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

final class GuiSmokeAutomation {
    static final String AUTORUN_ENV = "TASKFLOW_GUI_SMOKE_AUTORUN";
    static final String INPUT_FILE_ENV = "TASKFLOW_GUI_SMOKE_INPUT_FILE";
    static final String OUTPUT_DIR_ENV = "TASKFLOW_GUI_SMOKE_OUTPUT_DIR";

    private static final Logger LOGGER = LoggerFactory.getLogger(GuiSmokeAutomation.class);

    private final Map<String, String> environment;
    private final List<ClientJobPlugin> clientJobPlugins;
    private final GuiInputStagingService inputStagingService;
    private final GuiJobSubmissionService jobSubmissionService;
    private final GuiCoordinatorConnectionService connectionService;
    private final GuiResultSaveService resultSaveService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean handledResult = new AtomicBoolean(false);

    private volatile String submittedJobId;

    GuiSmokeAutomation(Map<String, String> environment,
                       List<ClientJobPlugin> clientJobPlugins,
                       GuiInputStagingService inputStagingService,
                       GuiJobSubmissionService jobSubmissionService,
                       GuiCoordinatorConnectionService connectionService,
                       GuiResultSaveService resultSaveService) {
        this.environment = Map.copyOf(Objects.requireNonNull(environment, "environment"));
        this.clientJobPlugins = List.copyOf(Objects.requireNonNull(clientJobPlugins, "clientJobPlugins"));
        this.inputStagingService = Objects.requireNonNull(inputStagingService, "inputStagingService");
        this.jobSubmissionService = Objects.requireNonNull(jobSubmissionService, "jobSubmissionService");
        this.connectionService = Objects.requireNonNull(connectionService, "connectionService");
        this.resultSaveService = Objects.requireNonNull(resultSaveService, "resultSaveService");
    }

    static boolean enabled(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        return "true".equalsIgnoreCase(environment.getOrDefault(AUTORUN_ENV, "false"));
    }

    void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        Thread thread = new Thread(this::runSmoke, "gui-smoke-automation");
        thread.setDaemon(true);
        thread.start();
    }

    boolean handleRoutedResult(GuiJobResultRouter.RoutedJobResult routed) {
        if (routed == null || routed.action() == GuiJobResultRouter.Action.IGNORE || routed.result() == null) {
            return false;
        }
        String jobId = routed.result().getJobId();
        String expectedJobId = submittedJobId;
        if (expectedJobId != null && !expectedJobId.equals(jobId)) {
            return false;
        }
        if (!handledResult.compareAndSet(false, true)) {
            return true;
        }
        if (routed.action() == GuiJobResultRouter.Action.SHOW_FAILURE) {
            LOGGER.error("event=gui_smoke_failed stage=result job_id={} error={}",
                    jobId, GuiJobResultRouter.failureMessage(routed.result()));
            return true;
        }

        Path outputDir = outputDir();
        GuiResultSaver.SaveResult saveResult = resultSaveService.save(routed.result(), outputDir.toString());
        if (!saveResult.successful()) {
            LOGGER.error("event=gui_smoke_failed stage=save job_id={} error={}",
                    jobId, saveResult.errorMessage(), saveResult.cause());
            return true;
        }
        LOGGER.info("event=gui_smoke_result_saved job_id={} output_dir={}",
                jobId, saveResult.outputDir());
        return true;
    }

    private void runSmoke() {
        try {
            ClientJobPlugin plugin = selectedPlugin();
            Path inputFile = inputFile();
            Path outputDir = outputDir();
            List<Path> stagedInputs = inputStagingService.stageFiles(List.of(inputFile), () -> false)
                    .stream()
                    .map(InputStaging.StagedInput::stagedPath)
                    .toList();

            CoordinatorConnection connection = connectionService.currentConnection();
            if (connection == null || !connection.isOpen()) {
                throw new IllegalStateException("RabbitMQ GUI connection is not open for smoke automation.");
            }

            String targetFormat = defaultParameter(plugin);
            GuiJobSubmitter.SubmittedJob submittedJob = jobSubmissionService.submit(
                    plugin,
                    stagedInputs,
                    targetFormat,
                    connection,
                    () -> connectionService.isCurrent(connection),
                    () -> connectionService.clear(connection, true),
                    () -> false,
                    () -> LOGGER.info("event=gui_smoke_submitting task_type={} input={} output_dir={}",
                            plugin.taskType(), inputFile, outputDir));
            if (submittedJob == null || submittedJob.jobId() == null) {
                throw new IllegalStateException("Smoke automation did not submit a job.");
            }
            submittedJobId = submittedJob.jobId();
            LOGGER.info("event=gui_job_submitted job_id={} task_type={} source=smoke_automation",
                    submittedJob.jobId(), plugin.taskType());
            LOGGER.info("event=gui_smoke_job_submitted job_id={} task_type={} parameter={} input={} output_dir={}",
                    submittedJob.jobId(), plugin.taskType(), targetFormat, inputFile, outputDir);
        } catch (Exception e) {
            LOGGER.error("event=gui_smoke_failed stage=submit error={}", e.getMessage(), e);
        }
    }

    private ClientJobPlugin selectedPlugin() {
        ClientJobPlugin selected = GuiDefaultTaskSelection.choose(clientJobPlugins, environment);
        if (selected == null) {
            throw new IllegalStateException("No client job plugin is available for smoke automation.");
        }
        return selected;
    }

    private Path inputFile() {
        String configured = environment.get(INPUT_FILE_ENV);
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException(INPUT_FILE_ENV + " is required for smoke automation.");
        }
        return Path.of(configured).toAbsolutePath().normalize();
    }

    private Path outputDir() {
        String configured = environment.get(OUTPUT_DIR_ENV);
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException(OUTPUT_DIR_ENV + " is required for smoke automation.");
        }
        return Path.of(configured).toAbsolutePath().normalize();
    }

    private static String defaultParameter(ClientJobPlugin plugin) {
        String defaultParameter = plugin.defaultParameter();
        if (defaultParameter != null && !defaultParameter.isBlank()) {
            return defaultParameter;
        }
        List<String> options = plugin.parameterOptions();
        if (options == null || options.isEmpty() || options.getFirst() == null || options.getFirst().isBlank()) {
            throw new IllegalStateException("Smoke automation plugin has no target parameter: " + plugin.taskType());
        }
        return options.getFirst();
    }
}
