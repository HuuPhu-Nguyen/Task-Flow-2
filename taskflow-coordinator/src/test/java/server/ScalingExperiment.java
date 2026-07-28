package server;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.model.MessageEnvelope;
import server.rabbitmq.RabbitMqOutboxReplayer;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.registry.PeerRegistryRecord;
import server.scaling.ScalingExperimentConfig;
import server.scaling.ScalingMetrics;
import server.scaling.ScalingWorkerProcessMain;
import server.scheduler.BrokerOutboxPublisher;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqMessageCodec;
import transport.rabbitmq.RabbitMqRecoveryPolicy;
import transport.rabbitmq.RabbitMqTopology;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Opt-in TF-0707 experiment. Its name deliberately avoids Surefire's default
 * test patterns; invoke it through {@code verify-scaling.ps1}.
 */
public class ScalingExperiment {
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_ID = "scaling-requester";
    private static final String WARMUP_JOB_PREFIX = "scaling-warmup-";
    private static final String MEASURED_JOB_PREFIX = "scaling-measured-";
    private static final int WORKER_CAPACITY = 1;
    private static final long CONDITION_POLL_NANOS =
            TimeUnit.MILLISECONDS.toNanos(25L);
    private static final long PROGRESS_POLL_NANOS =
            TimeUnit.SECONDS.toNanos(1L);
    private static final DockerImageName RABBITMQ_IMAGE =
            DockerImageName.parse("rabbitmq:3.13-management");
    private static final RabbitMqRecoveryPolicy RECOVERY_POLICY =
            new RabbitMqRecoveryPolicy(1_000, 100L, 1_000L, 2.0D);

    @Test
    void runConfiguredScalingPoint() throws Exception {
        ScalingExperimentConfig config =
                ScalingExperimentConfig.fromSystemProperties();
        boolean reportGrade = Boolean.getBoolean(
                "taskflow.scaling.reportGrade"
        );
        if (reportGrade) {
            config.requireReportGrade();
        }
        try (ScalingRun run = new ScalingRun(config, reportGrade)) {
            run.execute();
        }
    }

    private static final class ScalingRun implements AutoCloseable {
        private final ScalingExperimentConfig config;
        private final boolean reportGrade;
        private final Path outputDirectory;
        private final Path databasePath;
        private final RabbitMQContainer broker =
                new RabbitMQContainer(RABBITMQ_IMAGE);
        private final List<WorkerProcess> workers = new ArrayList<>();
        private final Map<String, Long> measuredSubmissionEpochMillis =
                new ConcurrentHashMap<>();
        private final Map<String, JobResultMessage> requesterResults =
                new ConcurrentHashMap<>();
        private final AtomicReference<Throwable> asynchronousFailure =
                new AtomicReference<>();

        private RabbitMqTransportConfig transportConfig;
        private ManagementQueueProbe managementQueueProbe;
        private RabbitMqTransport requesterTransport;
        private CoordinatorRuntime coordinator;
        private java.sql.Connection auditConnection;
        private ResourceSampler resourceSampler;
        private long measuredStartedNanos;
        private long taskCompletionDurationNanos;
        private long measuredDrainDurationNanos;
        private boolean workersStopped;
        private boolean completed;

        private ScalingRun(
                ScalingExperimentConfig config,
                boolean reportGrade
        ) throws IOException {
            this.config = config;
            this.reportGrade = reportGrade;
            this.outputDirectory = config.outputDirectory().toAbsolutePath();
            this.databasePath = outputDirectory.resolve("scaling.db");
            if (Files.exists(outputDirectory)) {
                throw new IllegalStateException(
                        "Scaling output directory already exists: "
                                + outputDirectory
                );
            }
            Files.createDirectories(outputDirectory);
        }

        private void execute() throws Exception {
            writeConfiguration();
            startInfrastructure();
            runWorkload(
                    WARMUP_JOB_PREFIX,
                    config.warmupTaskCount(),
                    config.warmupJobCount(),
                    false
            );
            awaitDurableCompletion(
                    WARMUP_JOB_PREFIX,
                    config.warmupTaskCount(),
                    config.warmupJobCount()
            );
            awaitRequesterResults(
                    WARMUP_JOB_PREFIX,
                    config.warmupJobCount()
            );
            awaitOutboxDrain();
            awaitAllQueuesDrained();

            coordinator.database().beginMeasurement();
            resourceSampler = new ResourceSampler();
            measuredStartedNanos = System.nanoTime();
            resourceSampler.start(measuredStartedNanos);
            runWorkload(
                    MEASURED_JOB_PREFIX,
                    config.taskCount(),
                    config.measuredJobCount(),
                    true
            );
            awaitDurableCompletion(
                    MEASURED_JOB_PREFIX,
                    config.taskCount(),
                    config.measuredJobCount()
            );
            taskCompletionDurationNanos =
                    System.nanoTime() - measuredStartedNanos;
            awaitRequesterResults(
                    MEASURED_JOB_PREFIX,
                    config.measuredJobCount()
            );
            awaitOutboxDrain();
            awaitAllQueuesDrained();
            measuredDrainDurationNanos =
                    System.nanoTime() - measuredStartedNanos;
            resourceSampler.close();
            coordinator.database().endMeasurement();

            stopWorkers();
            Audit audit = audit();
            writeRawEvidence(audit);
            writeMetrics(audit);
            completed = true;
        }

        private void startInfrastructure() throws Exception {
            broker.start();
            String token = Integer.toUnsignedString(
                    (int) System.nanoTime(),
                    36
            ) + "-" + config.workerCount();
            transportConfig = managedTransportConfig(
                    broker.getHost(),
                    broker.getAmqpPort(),
                    broker.getAdminUsername(),
                    broker.getAdminPassword(),
                    token
            );
            managementQueueProbe = new ManagementQueueProbe(
                    broker.getHttpUrl(),
                    broker.getAdminUsername(),
                    broker.getAdminPassword(),
                    transportConfig.queuePrefix()
            );

            coordinator = new CoordinatorRuntime();
            coordinator.start();
            requesterTransport = new RabbitMqTransport(
                    transportConfig,
                    RECOVERY_POLICY
            );
            requesterTransport.subscribePeer(
                    TransportRoute.JOB_RESULT,
                    REQUESTER_ID,
                    this::receiveJobResult
            );
            startWorkers(token);
            auditConnection = DriverManager.getConnection(
                    "jdbc:sqlite:" + databasePath
            );
            try (Statement statement = auditConnection.createStatement()) {
                statement.execute("PRAGMA busy_timeout = 10000");
            }
        }

        private void receiveJobResult(InboundTransportMessage delivery) {
            if (!(delivery.message() instanceof JobResultMessage result)) {
                throw new IllegalArgumentException(
                        "Scaling requester received a non-job-result message."
                );
            }
            requesterResults.put(result.getJobId(), result);
        }

        private void startWorkers(String token) throws Exception {
            for (int index = 0; index < config.workerCount(); index++) {
                String workerId = workerId(index);
                Path workerDirectory = outputDirectory.resolve(workerId);
                Files.createDirectories(workerDirectory);
                WorkerProcess worker = new WorkerProcess(
                        workerId,
                        workerDirectory,
                        token
                );
                workers.add(worker);
                worker.start();
            }
            for (WorkerProcess worker : workers) {
                worker.awaitReady();
            }
        }

        private void runWorkload(
                String prefix,
                int taskCount,
                int jobCount,
                boolean measured
        ) throws Exception {
            int nextOrdinal = 0;
            for (int jobIndex = 0; jobIndex < jobCount; jobIndex++) {
                int tasksInJob = Math.min(
                        config.tasksPerJob(),
                        taskCount - nextOrdinal
                );
                List<Object> payloads = new ArrayList<>(tasksInJob);
                for (int offset = 0; offset < tasksInJob; offset++) {
                    payloads.add(payload(nextOrdinal + offset));
                }
                String jobId = prefix + jobIndex;
                JobSubmitMessage submission = new JobSubmitMessage(
                        REQUESTER_ID,
                        Instant.now().toString(),
                        jobId,
                        TASK_TYPE,
                        payloads,
                        "",
                        "token-" + jobId
                );
                long submittedAt = System.currentTimeMillis();
                if (measured) {
                    measuredSubmissionEpochMillis.put(jobId, submittedAt);
                }
                assertTrue(
                        requesterTransport.publish(
                                new OutboundTransportMessage(
                                        TransportRoute.JOB_SUBMIT,
                                        REQUESTER_ID,
                                        submission
                                )
                        ),
                        "Submission publish was not confirmed for " + jobId
                );
                nextOrdinal += tasksInJob;
            }
            assertEquals(taskCount, nextOrdinal);
        }

        private String payload(int ordinal) {
            String prefix = "payload-" + ordinal + "-";
            StringBuilder payload = new StringBuilder(config.payloadBytes());
            payload.append(prefix);
            while (payload.length() < config.payloadBytes()) {
                payload.append((char) ('a' + Math.floorMod(payload.length(), 26)));
            }
            return payload.substring(0, config.payloadBytes());
        }

        private void awaitDurableCompletion(
                String prefix,
                int expectedTasks,
                int expectedJobs
        ) throws Exception {
            long deadline = deadlineNanos();
            while (System.nanoTime() < deadline) {
                throwIfAsynchronousFailure();
                if (queryLong(
                        "SELECT COUNT(*) FROM tasks "
                                + "WHERE job_id LIKE '"
                                + prefix
                                + "%' AND status='COMPLETED'"
                ) == expectedTasks
                        && queryLong(
                        "SELECT COUNT(*) FROM jobs "
                                + "WHERE job_id LIKE '"
                                + prefix
                                + "%' AND status='COMPLETED'"
                ) == expectedJobs) {
                    return;
                }
                LockSupport.parkNanos(PROGRESS_POLL_NANOS);
            }
            fail(
                    "Timed out waiting for durable completion for "
                            + prefix
                            + "; tasks="
                            + queryLong(
                            "SELECT COUNT(*) FROM tasks WHERE job_id LIKE '"
                                    + prefix
                                    + "%' AND status='COMPLETED'"
                    )
                            + "/"
                            + expectedTasks
                            + ", jobs="
                            + queryLong(
                            "SELECT COUNT(*) FROM jobs WHERE job_id LIKE '"
                                    + prefix
                                    + "%' AND status='COMPLETED'"
                    )
                            + "/"
                            + expectedJobs
            );
        }

        private void awaitRequesterResults(String prefix, int expected)
                throws Exception {
            awaitCondition(
                    () -> requesterResults.keySet().stream()
                            .filter(jobId -> jobId.startsWith(prefix))
                            .count() == expected,
                    "requester results for " + prefix
            );
            requesterResults.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(prefix))
                    .forEach(entry -> assertTrue(
                            entry.getValue().isSuccessful(),
                            "Job failed: " + entry.getKey() + " "
                                    + entry.getValue().getErrorMessage()
                    ));
        }

        private void awaitOutboxDrain() throws Exception {
            awaitCondition(
                    () -> coordinator.database()
                            .countPendingBrokerOutbox()
                            .count() == 0L,
                    "coordinator outbox drain"
            );
        }

        private void awaitAllQueuesDrained() throws Exception {
            awaitCondition(
                    this::allQueuesDrained,
                    "RabbitMQ work queues drain"
            );
        }

        private boolean allQueuesDrained() throws Exception {
            return managementQueueProbe.depth().totalMessages() == 0L;
        }

        private void stopWorkers() throws Exception {
            Throwable failure = null;
            for (WorkerProcess worker : workers) {
                try {
                    worker.stop();
                } catch (Throwable throwable) {
                    failure = merge(failure, throwable);
                }
            }
            workersStopped = true;
            if (failure != null) {
                if (failure instanceof Exception exception) {
                    throw exception;
                }
                throw new IllegalStateException(
                        "Could not stop scaling workers.",
                        failure
                );
            }
        }

        private Audit audit() throws Exception {
            throwIfAsynchronousFailure();
            assertEquals(
                    config.taskCount(),
                    queryLong(
                            "SELECT COUNT(*) FROM tasks "
                                    + "WHERE job_id LIKE '"
                                    + MEASURED_JOB_PREFIX
                                    + "%'"
                    )
            );
            assertEquals(
                    config.taskCount(),
                    queryLong(
                            "SELECT COUNT(*) FROM tasks "
                                    + "WHERE job_id LIKE '"
                                    + MEASURED_JOB_PREFIX
                                    + "%' AND status='COMPLETED' "
                                    + "AND result_payload_json IS NOT NULL"
                    )
            );
            assertEquals(
                    config.measuredJobCount(),
                    queryLong(
                            "SELECT COUNT(*) FROM jobs "
                                    + "WHERE job_id LIKE '"
                                    + MEASURED_JOB_PREFIX
                                    + "%' AND status='COMPLETED'"
                    )
            );
            assertEquals(
                    0L,
                    queryLong(
                            "SELECT COUNT(*) FROM tasks t "
                                    + "WHERE t.job_id LIKE '"
                                    + MEASURED_JOB_PREFIX
                                    + "%' AND ("
                                    + "SELECT COUNT(*) FROM task_attempts a "
                                    + "WHERE a.task_id=t.task_id "
                                    + "AND a.outcome='SUCCEEDED' "
                                    + "AND a.attempt_number=t.attempt_number "
                                    + "AND a.assignment_id=t.assignment_id"
                                    + ") <> 1"
                    )
            );
            assertEquals(
                    0L,
                    coordinator.database().countPendingBrokerOutbox().count()
            );
            assertTrue(coordinator.capacityProjectionValid());
            assertTrue(allQueuesDrained());

            List<TaskLatency> taskLatencies = loadTaskLatencies();
            assertEquals(config.taskCount(), taskLatencies.size());
            Map<String, Long> assignmentsByWorker =
                    assignmentDistribution(taskLatencies);
            assertEquals(config.workerCount(), assignmentsByWorker.size());
            assertTrue(
                    assignmentsByWorker.values().stream()
                            .allMatch(count -> count > 0L)
            );

            long workerTasks = 0L;
            long workerBusyNanos = 0L;
            List<WorkerMetrics> workerMetrics = new ArrayList<>();
            for (WorkerProcess worker : workers) {
                WorkerMetrics metrics = worker.metrics();
                workerMetrics.add(metrics);
                workerTasks += metrics.measuredTasks();
                workerBusyNanos += metrics.measuredBusyNanos();
                assertTrue(
                        metrics.measuredTasks()
                                >= assignmentsByWorker.get(worker.workerId()),
                        "Worker executed fewer tasks than SQLite attributes "
                                + "to it: "
                                + worker.workerId()
                );
            }
            assertTrue(
                    workerTasks >= config.taskCount(),
                    "Worker execution count is below authoritative task count."
            );

            List<WriteSample> writeSamples =
                    coordinator.database().writeSamples();
            assertFalse(writeSamples.isEmpty());
            List<ResourceSample> resourceSamples =
                    resourceSampler.samples();
            assertFalse(resourceSamples.isEmpty());

            return new Audit(
                    taskLatencies,
                    writeSamples,
                    resourceSamples,
                    workerMetrics,
                    assignmentsByWorker,
                    workerTasks,
                    workerBusyNanos
            );
        }

        private List<TaskLatency> loadTaskLatencies() throws Exception {
            String sql = """
                    SELECT
                        t.task_id,
                        t.job_id,
                        t.assigned_peer_id,
                        t.completed_at,
                        a.started_at
                    FROM tasks t
                    JOIN task_attempts a
                      ON a.task_id=t.task_id
                     AND a.attempt_number=t.attempt_number
                     AND a.assignment_id=t.assignment_id
                     AND a.outcome='SUCCEEDED'
                    WHERE t.job_id LIKE 'scaling-measured-%'
                    ORDER BY t.task_id
                    """;
            List<TaskLatency> latencies =
                    new ArrayList<>(config.taskCount());
            try (Statement statement = auditConnection.createStatement();
                 ResultSet results = statement.executeQuery(sql)) {
                while (results.next()) {
                    String jobId = results.getString("job_id");
                    Long submittedAt =
                            measuredSubmissionEpochMillis.get(jobId);
                    assertNotNull(
                            submittedAt,
                            "Missing requester timestamp for " + jobId
                    );
                    long assignedAt = results.getLong("started_at");
                    long completedAt = results.getLong("completed_at");
                    assertTrue(assignedAt >= submittedAt);
                    assertTrue(completedAt >= assignedAt);
                    latencies.add(new TaskLatency(
                            results.getString("task_id"),
                            jobId,
                            results.getString("assigned_peer_id"),
                            TimeUnit.MILLISECONDS.toNanos(
                                    assignedAt - submittedAt
                            ),
                            TimeUnit.MILLISECONDS.toNanos(
                                    completedAt - submittedAt
                            )
                    ));
                }
            }
            return List.copyOf(latencies);
        }

        private Map<String, Long> assignmentDistribution(
                List<TaskLatency> latencies
        ) {
            Map<String, Long> distribution = new LinkedHashMap<>();
            workers.stream()
                    .map(WorkerProcess::workerId)
                    .sorted()
                    .forEach(workerId -> distribution.put(workerId, 0L));
            for (TaskLatency latency : latencies) {
                distribution.compute(
                        latency.workerId(),
                        (ignored, count) -> count == null ? 1L : count + 1L
                );
            }
            return Map.copyOf(distribution);
        }

        private void writeRawEvidence(Audit audit) throws IOException {
            List<String> taskLines =
                    new ArrayList<>(audit.taskLatencies().size() + 1);
            taskLines.add(
                    "task_id,job_id,worker_id,assignment_latency_nanos,"
                            + "end_to_end_latency_nanos"
            );
            for (TaskLatency latency : audit.taskLatencies()) {
                taskLines.add(String.join(
                        ",",
                        latency.taskId(),
                        latency.jobId(),
                        latency.workerId(),
                        Long.toString(latency.assignmentLatencyNanos()),
                        Long.toString(latency.endToEndLatencyNanos())
                ));
            }
            writeLines(outputDirectory.resolve("task-latencies.csv"), taskLines);

            List<String> writeLines =
                    new ArrayList<>(audit.writeSamples().size() + 1);
            writeLines.add("operation,latency_nanos");
            for (WriteSample sample : audit.writeSamples()) {
                writeLines.add(sample.operation() + "," + sample.nanos());
            }
            writeLines(
                    outputDirectory.resolve("sqlite-writes.csv"),
                    writeLines
            );

            List<String> resourceLines =
                    new ArrayList<>(audit.resourceSamples().size() + 1);
            resourceLines.add(
                    "elapsed_nanos,heap_used_bytes,process_cpu_nanos,"
                            + "rabbitmq_total_messages,"
                            + "rabbitmq_max_single_queue_messages"
            );
            for (ResourceSample sample : audit.resourceSamples()) {
                resourceLines.add(String.join(
                        ",",
                        Long.toString(sample.elapsedNanos()),
                        Long.toString(sample.heapUsedBytes()),
                        Long.toString(sample.processCpuNanos()),
                        Long.toString(sample.totalQueueDepth()),
                        Long.toString(sample.maxQueueDepth())
                ));
            }
            writeLines(
                    outputDirectory.resolve("resource-samples.csv"),
                    resourceLines
            );

            List<String> workerLines =
                    new ArrayList<>(audit.workerMetrics().size() + 1);
            workerLines.add(
                    "worker_id,authoritative_tasks,executions,"
                            + "duplicate_executions,busy_nanos,"
                            + "utilization_percent"
            );
            for (WorkerMetrics metrics : audit.workerMetrics()) {
                long authoritative =
                        audit.assignmentsByWorker().get(metrics.workerId());
                workerLines.add(String.join(
                        ",",
                        metrics.workerId(),
                        Long.toString(authoritative),
                        Long.toString(metrics.measuredTasks()),
                        Long.toString(
                                metrics.measuredTasks() - authoritative
                        ),
                        Long.toString(metrics.measuredBusyNanos()),
                        decimal(
                                metrics.measuredBusyNanos()
                                        * 100.0D
                                        / measuredDrainDurationNanos
                        )
                ));
            }
            writeLines(
                    outputDirectory.resolve("worker-metrics.csv"),
                    workerLines
            );
        }

        private void writeMetrics(Audit audit) throws IOException {
            List<Long> assignmentLatencies = audit.taskLatencies().stream()
                    .map(TaskLatency::assignmentLatencyNanos)
                    .toList();
            List<Long> endToEndLatencies = audit.taskLatencies().stream()
                    .map(TaskLatency::endToEndLatencyNanos)
                    .toList();
            List<Long> writeLatencies = audit.writeSamples().stream()
                    .map(WriteSample::nanos)
                    .toList();
            List<Long> queueDepths = audit.resourceSamples().stream()
                    .map(ResourceSample::totalQueueDepth)
                    .toList();
            long peakHeap = audit.resourceSamples().stream()
                    .mapToLong(ResourceSample::heapUsedBytes)
                    .max()
                    .orElseThrow();
            long heapBefore = audit.resourceSamples().getFirst().heapUsedBytes();
            long processCpuStart =
                    audit.resourceSamples().getFirst().processCpuNanos();
            long processCpuEnd =
                    audit.resourceSamples().getLast().processCpuNanos();
            long processCpuNanos = Math.max(
                    0L,
                    processCpuEnd - processCpuStart
            );
            double throughput = config.taskCount()
                    * 1_000_000_000.0D
                    / taskCompletionDurationNanos;
            double cpuCorePercent = processCpuNanos
                    * 100.0D
                    / measuredDrainDurationNanos;
            double cpuHostPercent = cpuCorePercent
                    / Runtime.getRuntime().availableProcessors();
            double workerUtilization = audit.workerBusyNanos()
                    * 100.0D
                    / (measuredDrainDurationNanos * (double) config.workerCount());

            List<String> properties = new ArrayList<>();
            properties.add("formatVersion=1");
            properties.add("result=PASS");
            properties.add("reportGrade=" + reportGrade);
            properties.add("workerCount=" + config.workerCount());
            properties.add("workerCapacity=" + WORKER_CAPACITY);
            properties.add("taskCount=" + config.taskCount());
            properties.add("warmupTaskCount=" + config.warmupTaskCount());
            properties.add("tasksPerJob=" + config.tasksPerJob());
            properties.add("measuredJobCount=" + config.measuredJobCount());
            properties.add("workUnitsPerTask=" + config.workUnitsPerTask());
            properties.add("payloadBytes=" + config.payloadBytes());
            properties.add(
                    "taskCompletionDurationNanos="
                            + taskCompletionDurationNanos
            );
            properties.add(
                    "measuredDrainDurationNanos="
                            + measuredDrainDurationNanos
            );
            properties.add(
                    "throughputTasksPerSecond="
                            + decimal(throughput)
            );
            addPercentiles(
                    properties,
                    "assignmentLatency",
                    assignmentLatencies,
                    true
            );
            addPercentiles(
                    properties,
                    "endToEndTaskLatency",
                    endToEndLatencies,
                    true
            );
            properties.add("coordinatorProcessCpuNanos=" + processCpuNanos);
            properties.add(
                    "coordinatorProcessCpuCorePercent="
                            + decimal(cpuCorePercent)
            );
            properties.add(
                    "coordinatorProcessCpuHostPercent="
                            + decimal(cpuHostPercent)
            );
            properties.add("coordinatorHeapBeforeBytes=" + heapBefore);
            properties.add("coordinatorPeakHeapBytes=" + peakHeap);
            properties.add("workerMeasuredBusyNanos=" + audit.workerBusyNanos());
            properties.add(
                    "workerExecutionCount=" + audit.workerExecutionCount()
            );
            properties.add(
                    "workerDuplicateExecutions="
                            + (audit.workerExecutionCount()
                            - config.taskCount())
            );
            properties.add(
                    "workerUtilizationPercent="
                            + decimal(workerUtilization)
            );
            properties.add(
                    "workerAssignmentDistribution="
                            + formatDistribution(audit.assignmentsByWorker())
            );
            properties.add(
                    "workerExecutionDistribution="
                            + formatWorkerExecutions(audit.workerMetrics())
            );
            properties.add(
                    "workerUtilizationDistribution="
                            + formatWorkerUtilization(
                            audit.workerMetrics(),
                            measuredDrainDurationNanos
                    )
            );
            addPercentiles(
                    properties,
                    "rabbitMqQueueDepth",
                    queueDepths,
                    false
            );
            properties.add("sqliteWriteCount=" + writeLatencies.size());
            addPercentiles(
                    properties,
                    "sqliteWriteLatency",
                    writeLatencies,
                    true
            );
            properties.add(
                    "resourceSampleCount="
                            + audit.resourceSamples().size()
            );
            properties.add("completedTasks=" + config.taskCount());
            properties.add("terminalJobs=" + config.measuredJobCount());
            properties.add("authoritativeAttemptMismatches=0");
            properties.add("pendingOutboxAtCompletion=0");
            properties.add("rabbitMqQueueDepthAtCompletion=0");
            writeLines(outputDirectory.resolve("metrics.properties"), properties);
        }

        private static void addPercentiles(
                List<String> properties,
                String prefix,
                List<Long> samples,
                boolean nanosAsMillis
        ) {
            long p50 = ScalingMetrics.nearestRank(samples, 0.50D);
            long p95 = ScalingMetrics.nearestRank(samples, 0.95D);
            long p99 = ScalingMetrics.nearestRank(samples, 0.99D);
            long maximum = samples.stream().mapToLong(Long::longValue).max()
                    .orElseThrow();
            if (nanosAsMillis) {
                properties.add(
                        prefix + "P50Millis="
                                + decimal(ScalingMetrics.nanosToMillis(p50))
                );
                properties.add(
                        prefix + "P95Millis="
                                + decimal(ScalingMetrics.nanosToMillis(p95))
                );
                properties.add(
                        prefix + "P99Millis="
                                + decimal(ScalingMetrics.nanosToMillis(p99))
                );
                properties.add(
                        prefix + "MaxMillis="
                                + decimal(ScalingMetrics.nanosToMillis(maximum))
                );
                return;
            }
            properties.add(prefix + "P50=" + p50);
            properties.add(prefix + "P95=" + p95);
            properties.add(prefix + "P99=" + p99);
            properties.add(prefix + "Max=" + maximum);
        }

        private void writeConfiguration() throws IOException {
            writeLines(
                    outputDirectory.resolve("configuration.properties"),
                    List.of(
                            "formatVersion=1",
                            "reportGrade=" + reportGrade,
                            "workerCount=" + config.workerCount(),
                            "workerCapacity=" + WORKER_CAPACITY,
                            "taskCount=" + config.taskCount(),
                            "warmupTaskCount=" + config.warmupTaskCount(),
                            "tasksPerJob=" + config.tasksPerJob(),
                            "workUnitsPerTask=" + config.workUnitsPerTask(),
                            "payloadBytes=" + config.payloadBytes(),
                            "completionTimeoutSeconds="
                                    + config.completionTimeoutSeconds(),
                            "sampleIntervalMillis="
                                    + config.sampleIntervalMillis(),
                            "coordinatorJvmRole=coordinator-requester-samplers",
                            "workerJvmXms=32m",
                            "workerJvmXmx=128m",
                            "assignmentPublisher="
                                    + "predeclared-route-persistent-confirmed"
                    )
            );
        }

        private long queryLong(String sql) {
            try (Statement statement = auditConnection.createStatement();
                 ResultSet result = statement.executeQuery(sql)) {
                return result.next() ? result.getLong(1) : 0L;
            } catch (Exception failure) {
                asynchronousFailure.compareAndSet(null, failure);
                return Long.MIN_VALUE;
            }
        }

        private long deadlineNanos() {
            return System.nanoTime() + TimeUnit.SECONDS.toNanos(
                    config.completionTimeoutSeconds()
            );
        }

        private void awaitCondition(
                CheckedBoolean condition,
                String description
        ) throws Exception {
            long deadline = deadlineNanos();
            while (System.nanoTime() < deadline) {
                throwIfAsynchronousFailure();
                if (condition.getAsBoolean()) {
                    return;
                }
                LockSupport.parkNanos(CONDITION_POLL_NANOS);
            }
            fail("Timed out waiting for " + description + ".");
        }

        private void throwIfAsynchronousFailure() {
            Throwable failure = asynchronousFailure.get();
            if (failure == null) {
                for (WorkerProcess worker : workers) {
                    failure = worker.observedFailure();
                    if (failure != null) {
                        break;
                    }
                }
            }
            if (failure != null) {
                throw new IllegalStateException(
                        "Scaling experiment asynchronous failure.",
                        failure
                );
            }
        }

        @Override
        public void close() throws Exception {
            Throwable failure = null;
            if (resourceSampler != null) {
                try {
                    resourceSampler.close();
                } catch (Throwable throwable) {
                    failure = merge(failure, throwable);
                }
            }
            if (!workersStopped) {
                for (WorkerProcess worker : workers) {
                    try {
                        worker.close();
                    } catch (Throwable throwable) {
                        failure = merge(failure, throwable);
                    }
                }
            }
            if (requesterTransport != null) {
                try {
                    requesterTransport.close();
                } catch (Throwable throwable) {
                    failure = merge(failure, throwable);
                }
            }
            if (coordinator != null) {
                try {
                    coordinator.close();
                } catch (Throwable throwable) {
                    failure = merge(failure, throwable);
                }
            }
            if (auditConnection != null) {
                try {
                    auditConnection.close();
                } catch (Throwable throwable) {
                    failure = merge(failure, throwable);
                }
            }
            try {
                broker.stop();
            } catch (Throwable throwable) {
                failure = merge(failure, throwable);
            }
            if (completed) {
                assertTrue(workers.stream().noneMatch(WorkerProcess::isAlive));
            }
            if (failure != null) {
                if (failure instanceof Exception exception) {
                    throw exception;
                }
                throw new IllegalStateException(
                        "Scaling experiment cleanup failed.",
                        failure
                );
            }
        }

        private final class CoordinatorRuntime implements AutoCloseable {
            private RabbitMqTransport transport;
            private TimedDatabaseManager database;
            private InMemoryPeerRegistry registry;
            private TaskScheduler scheduler;
            private Thread schedulerThread;
            private RabbitMqOutboxReplayer outboxReplayer;
            private SchedulerMailbox.BrokerIngress ingress;
            private FastAssignmentPublisher assignmentPublisher;
            private boolean closed;

            private void start() throws Exception {
                transport = new RabbitMqTransport(
                        transportConfig,
                        RECOVERY_POLICY
                );
                transport.declareTopology();
                database = new TimedDatabaseManager(
                        databasePath.toString(),
                        config.maximumWriteSampleCount()
                );
                SchedulerConfig schedulerConfig = schedulerConfig(config);
                registry = new InMemoryPeerRegistry(database);
                registerWorkers(schedulerConfig);
                BlockingQueue<MessageEnvelope> mailbox =
                        SchedulerMailbox.create(schedulerConfig);
                ingress = SchedulerMailbox.brokerIngress(mailbox);
                assignmentPublisher = new FastAssignmentPublisher();
                ScalingSchedulerOutput output = new ScalingSchedulerOutput(
                        new RabbitMqSchedulerOutput(transport)
                );
                scheduler = new TaskScheduler(
                        mailbox,
                        registry,
                        database,
                        output,
                        schedulerConfig
                );
                outboxReplayer = new RabbitMqOutboxReplayer(
                        database,
                        output,
                        schedulerConfig.schedulerOutboxBatchSize()
                );
                transport.subscribe(
                        TransportRoute.JOB_SUBMIT,
                        delivery -> ingress.offer(delivery)
                );
                transport.subscribe(
                        TransportRoute.TASK_RESULT,
                        delivery -> ingress.offer(delivery)
                );
                schedulerThread = new Thread(
                        scheduler,
                        "scaling-coordinator-scheduler"
                );
                schedulerThread.start();
                outboxReplayer.start();
            }

            private void registerWorkers(SchedulerConfig schedulerConfig) {
                for (int index = 0; index < config.workerCount(); index++) {
                    String workerId = workerId(index);
                    PeerInfo peer = new PeerInfo(
                            workerId,
                            schedulerConfig,
                            List.of()
                    );
                    registry.register(workerId, peer);
                    registry.updateHeartbeat(
                            workerId,
                            new PongMessage(
                                    workerId,
                                    Instant.now().toString(),
                                    List.of(TASK_TYPE),
                                    UUID.nameUUIDFromBytes(
                                            workerId.getBytes(
                                                    StandardCharsets.UTF_8
                                            )
                                    ).toString(),
                                    1L,
                                    WORKER_CAPACITY,
                                    WORKER_CAPACITY,
                                    Map.of(TASK_TYPE, WORKER_CAPACITY)
                            )
                    );
                }
            }

            private TimedDatabaseManager database() {
                return database;
            }

            private boolean capacityProjectionValid() {
                return registry.capacityProjectionValid();
            }

            @Override
            public void close() throws Exception {
                if (closed) {
                    return;
                }
                closed = true;
                if (ingress != null) {
                    ingress.stopIntake();
                }
                if (outboxReplayer != null) {
                    outboxReplayer.close();
                }
                if (scheduler != null) {
                    scheduler.requestShutdownAfterDrain();
                }
                if (schedulerThread != null) {
                    schedulerThread.join(10_000L);
                    assertFalse(
                            schedulerThread.isAlive(),
                            "Scaling coordinator scheduler did not stop."
                    );
                }
                if (transport != null) {
                    transport.close();
                }
                if (assignmentPublisher != null) {
                    assignmentPublisher.close();
                }
                if (database != null) {
                    database.close();
                }
            }

            private final class ScalingSchedulerOutput
                    implements SchedulerOutput, BrokerOutboxPublisher {
                private final RabbitMqSchedulerOutput delegate;

                private ScalingSchedulerOutput(
                        RabbitMqSchedulerOutput delegate
                ) {
                    this.delegate = delegate;
                }

                @Override
                public void sendTask(
                        PeerInfo peer,
                        TaskAssignMessage message
                ) throws Exception {
                    assertTrue(assignmentPublisher.publish(
                            delegate.taskAssignmentOutboxMessage(peer, message)
                    ));
                }

                @Override
                public boolean sendJobResult(
                        String requesterNodeId,
                        JobResultMessage message
                ) throws Exception {
                    return delegate.sendJobResult(requesterNodeId, message);
                }

                @Override
                public BrokerOutboxStore.OutboxMessage
                taskAssignmentOutboxMessage(
                        PeerInfo peer,
                        TaskAssignMessage message
                ) {
                    return delegate.taskAssignmentOutboxMessage(peer, message);
                }

                @Override
                public BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(
                        String requesterNodeId,
                        JobResultMessage message
                ) {
                    return delegate.jobResultOutboxMessage(
                            requesterNodeId,
                            message
                    );
                }

                @Override
                public boolean publishOutbox(
                        BrokerOutboxStore.OutboxRecord record
                ) throws Exception {
                    if (record.message().route()
                            == TransportRoute.TASK_ASSIGN) {
                        return assignmentPublisher.publish(record.message());
                    }
                    return delegate.publishOutbox(record);
                }
            }
        }

        private final class FastAssignmentPublisher implements AutoCloseable {
            private final Connection connection;
            private final Channel channel;
            private final RabbitMqMessageCodec codec =
                    new RabbitMqMessageCodec();
            private final RabbitMqTopology topology =
                    new RabbitMqTopology(transportConfig);

            private FastAssignmentPublisher() throws Exception {
                connection = connectionFactory(transportConfig).newConnection();
                channel = connection.createChannel();
                channel.confirmSelect();
            }

            private synchronized boolean publish(
                    BrokerOutboxStore.OutboxMessage message
            ) throws Exception {
                if (message.route() != TransportRoute.TASK_ASSIGN) {
                    throw new IllegalArgumentException(
                            "Fast assignment publisher only accepts TASK_ASSIGN."
                    );
                }
                String routingKey = topology.peerRoutingKey(
                        message.route(),
                        message.peerNodeId()
                );
                String messageId = UUID.randomUUID().toString();
                AMQP.BasicProperties properties =
                        new AMQP.BasicProperties.Builder()
                                .messageId(messageId)
                                .contentType("application/json")
                                .contentEncoding(StandardCharsets.UTF_8.name())
                                .deliveryMode(2)
                                .timestamp(new Date())
                                .headers(Map.of(
                                        "x-taskflow-delivery-attempt",
                                        1,
                                        "x-taskflow-original-routing-key",
                                        routingKey,
                                        "x-taskflow-original-exchange",
                                        topology.exchangeName(),
                                        "x-taskflow-original-message-id",
                                        messageId
                                ))
                                .build();
                channel.basicPublish(
                        topology.exchangeName(),
                        routingKey,
                        false,
                        properties,
                        codec.encode(new OutboundTransportMessage(
                                message.route(),
                                message.fromNodeId(),
                                message.message()
                        ))
                );
                return channel.waitForConfirms(
                        transportConfig.publisherConfirmTimeoutMillis()
                );
            }

            @Override
            public void close() throws Exception {
                Throwable failure = null;
                try {
                    channel.close();
                } catch (Throwable throwable) {
                    failure = throwable;
                }
                try {
                    connection.close();
                } catch (Throwable throwable) {
                    failure = merge(failure, throwable);
                }
                if (failure != null) {
                    if (failure instanceof Exception exception) {
                        throw exception;
                    }
                    throw new IllegalStateException(
                            "Fast assignment publisher close failed.",
                            failure
                    );
                }
            }
        }

        private final class ResourceSampler implements AutoCloseable {
            private final AtomicBoolean running = new AtomicBoolean();
            private final List<ResourceSample> samples = new ArrayList<>();
            private final MemoryMXBean memory =
                    ManagementFactory.getMemoryMXBean();
            private final com.sun.management.OperatingSystemMXBean operatingSystem =
                    ManagementFactory.getPlatformMXBean(
                            com.sun.management.OperatingSystemMXBean.class
                    );
            private Thread thread;
            private long startNanos;
            private boolean closed;

            private ResourceSampler() {
            }

            private void start(long startNanos) {
                this.startNanos = startNanos;
                running.set(true);
                thread = Thread.ofPlatform()
                        .name("scaling-resource-sampler")
                        .daemon(true)
                        .start(this::sampleUntilStopped);
            }

            private void sampleUntilStopped() {
                try {
                    while (running.get()) {
                        sample();
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(
                                config.sampleIntervalMillis()
                        ));
                    }
                    sample();
                } catch (Throwable failure) {
                    asynchronousFailure.compareAndSet(null, failure);
                }
            }

            private void sample() throws Exception {
                QueueDepth depth = managementQueueProbe.depth();
                synchronized (samples) {
                    if (samples.size() >= config.maximumSampleCount()) {
                        throw new IllegalStateException(
                                "Resource sampler exceeded its configured bound."
                        );
                    }
                    samples.add(new ResourceSample(
                            Math.max(0L, System.nanoTime() - startNanos),
                            memory.getHeapMemoryUsage().getUsed(),
                            Math.max(0L, operatingSystem.getProcessCpuTime()),
                            depth.totalMessages(),
                            depth.maximumQueueMessages()
                    ));
                }
            }

            private List<ResourceSample> samples() {
                synchronized (samples) {
                    return List.copyOf(samples);
                }
            }

            @Override
            public void close() throws Exception {
                if (closed) {
                    return;
                }
                closed = true;
                running.set(false);
                if (thread != null) {
                    thread.join(5_000L);
                    assertFalse(
                            thread.isAlive(),
                            "Scaling resource sampler did not stop."
                    );
                }
                throwIfAsynchronousFailure();
            }
        }

        private final class WorkerProcess implements AutoCloseable {
            private final String workerId;
            private final Path directory;
            private final String token;
            private final Path readyPath;
            private final Path stopPath;
            private final Path failurePath;
            private final Path metricsPath;
            private final Path logPath;
            private Process process;
            private boolean stopped;

            private WorkerProcess(
                    String workerId,
                    Path directory,
                    String token
            ) {
                this.workerId = workerId;
                this.directory = directory;
                this.token = token;
                readyPath = directory.resolve("ready.signal");
                stopPath = directory.resolve("stop.signal");
                failurePath = directory.resolve("failure.signal");
                metricsPath = directory.resolve("metrics.properties");
                logPath = directory.resolve("worker.log");
            }

            private void start() throws IOException {
                List<String> command = new ArrayList<>();
                command.add(javaExecutable());
                command.add("-Xms32m");
                command.add("-Xmx128m");
                command.add("-cp");
                command.add(testClasspath());
                command.add(ScalingWorkerProcessMain.class.getName());
                command.add(workerId);
                command.add(transportConfig.host());
                command.add(Integer.toString(transportConfig.port()));
                command.add(transportConfig.username());
                command.add(transportConfig.password());
                command.add(token);
                command.add(Integer.toString(config.workUnitsPerTask()));
                command.add(readyPath.toString());
                command.add(stopPath.toString());
                command.add(failurePath.toString());
                command.add(metricsPath.toString());
                process = new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(logPath.toFile())
                        .start();
            }

            private void awaitReady() throws Exception {
                long deadline = deadlineNanos();
                while (System.nanoTime() < deadline) {
                    Throwable failure = observedFailure();
                    if (failure != null) {
                        throw new IllegalStateException(
                                "Scaling worker did not become ready: "
                                        + workerId,
                                failure
                        );
                    }
                    if (Files.exists(readyPath)) {
                        String readyWorkerId = Files.readString(
                                readyPath,
                                StandardCharsets.UTF_8
                        );
                        if (workerId.equals(readyWorkerId)) {
                            return;
                        }
                    }
                    LockSupport.parkNanos(CONDITION_POLL_NANOS);
                }
                fail(
                        "Timed out waiting for scaling worker "
                                + workerId
                                + "; ready signal="
                                + safeRead(readyPath)
                );
            }

            private void stop() throws Exception {
                if (stopped) {
                    return;
                }
                stopped = true;
                Files.writeString(
                        stopPath,
                        "stop",
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE
                );
                assertTrue(
                        process.waitFor(30L, TimeUnit.SECONDS),
                        "Scaling worker did not exit after stop signal: "
                                + workerId
                );
                assertEquals(
                        0,
                        process.exitValue(),
                        () -> "Scaling worker exited unsuccessfully: "
                                + workerId
                                + System.lineSeparator()
                                + childLog()
                );
                assertTrue(
                        Files.exists(metricsPath),
                        "Scaling worker did not write metrics: " + workerId
                );
            }

            private WorkerMetrics metrics() throws IOException {
                Properties properties = new Properties();
                try (var reader = Files.newBufferedReader(
                        metricsPath,
                        StandardCharsets.UTF_8
                )) {
                    properties.load(reader);
                }
                assertEquals(workerId, properties.getProperty("workerId"));
                return new WorkerMetrics(
                        workerId,
                        Long.parseLong(
                                properties.getProperty("measuredTasks")
                        ),
                        Long.parseLong(
                                properties.getProperty("measuredBusyNanos")
                        )
                );
            }

            private Throwable observedFailure() {
                if (Files.exists(failurePath)) {
                    return new IllegalStateException(
                            "Worker failure signal for "
                                    + workerId
                                    + ": "
                                    + safeRead(failurePath)
                    );
                }
                if (process != null && !process.isAlive() && !stopped) {
                    return new IllegalStateException(
                            "Worker exited early with code "
                                    + process.exitValue()
                                    + ": "
                                    + workerId
                                    + System.lineSeparator()
                                    + childLog()
                    );
                }
                return null;
            }

            private boolean isAlive() {
                return process != null && process.isAlive();
            }

            private String workerId() {
                return workerId;
            }

            private String childLog() {
                return safeRead(logPath);
            }

            @Override
            public void close() throws Exception {
                if (process == null || !process.isAlive()) {
                    return;
                }
                if (!Files.exists(stopPath)) {
                    Files.writeString(
                            stopPath,
                            "stop",
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE
                    );
                }
                if (!process.waitFor(10L, TimeUnit.SECONDS)) {
                    process.destroy();
                    if (!process.waitFor(5L, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        assertTrue(process.waitFor(5L, TimeUnit.SECONDS));
                    }
                }
            }
        }
    }

    private static final class TimedDatabaseManager extends DatabaseManager {
        private final int maximumSamples;
        private final List<WriteSample> samples = new ArrayList<>();
        private boolean measuring;

        private TimedDatabaseManager(String path, int maximumSamples)
                throws Exception {
            super(path);
            this.maximumSamples = maximumSamples;
        }

        private synchronized void beginMeasurement() {
            samples.clear();
            measuring = true;
        }

        private synchronized void endMeasurement() {
            measuring = false;
        }

        private synchronized List<WriteSample> writeSamples() {
            return List.copyOf(samples);
        }

        private <T> T timed(String operation, Supplier<T> action) {
            long startedAt = System.nanoTime();
            try {
                return action.get();
            } finally {
                record(operation, System.nanoTime() - startedAt);
            }
        }

        private synchronized void record(String operation, long nanos) {
            if (!measuring) {
                return;
            }
            if (samples.size() >= maximumSamples) {
                throw new IllegalStateException(
                        "SQLite write samples exceeded the configured bound."
                );
            }
            samples.add(new WriteSample(operation, Math.max(0L, nanos)));
        }

        @Override
        public JobSubmissionDecision commitJobSubmission(
                String jobId,
                String taskType,
                String requesterId,
                String requesterTokenHash,
                String requesterIdentityKey,
                String requestHash,
                String parameter,
                Collection<TaskStartupState> tasks
        ) {
            return timed(
                    "commitJobSubmission",
                    () -> super.commitJobSubmission(
                            jobId,
                            taskType,
                            requesterId,
                            requesterTokenHash,
                            requesterIdentityKey,
                            requestHash,
                            parameter,
                            tasks
                    )
            );
        }

        @Override
        public BrokerOutboxStore.TaskAssignmentCommit
        commitTaskAssignmentAndEnqueueBrokerOutbox(
                String taskId,
                String peerId,
                long startedAt,
                String leaseOwnerId,
                long leaseExpiresAt,
                String assignmentId,
                BrokerOutboxStore.OutboxMessage messageTemplate
        ) {
            return timed(
                    "commitTaskAssignmentAndEnqueueBrokerOutbox",
                    () -> super.commitTaskAssignmentAndEnqueueBrokerOutbox(
                            taskId,
                            peerId,
                            startedAt,
                            leaseOwnerId,
                            leaseExpiresAt,
                            assignmentId,
                            messageTemplate
                    )
            );
        }

        @Override
        public ResultCommitOutcome commitTaskResult(
                String taskId,
                int attemptNumber,
                String assignmentId,
                String assignedPeerId,
                long completedAt,
                long durationMs,
                Object resultPayload
        ) {
            return timed(
                    "commitTaskResult",
                    () -> super.commitTaskResult(
                            taskId,
                            attemptNumber,
                            assignmentId,
                            assignedPeerId,
                            completedAt,
                            durationMs,
                            resultPayload
                    )
            );
        }

        @Override
        public BrokerOutboxStore.OutboxCommit
        commitJobCompletedAndEnqueueBrokerOutbox(
                String jobId,
                Object resultPayload,
                BrokerOutboxStore.OutboxMessage message
        ) {
            return timed(
                    "commitJobCompletedAndEnqueueBrokerOutbox",
                    () -> super.commitJobCompletedAndEnqueueBrokerOutbox(
                            jobId,
                            resultPayload,
                            message
                    )
            );
        }

        @Override
        public boolean markBrokerOutboxPublished(
                long outboxId,
                long publishedAt
        ) {
            return timed(
                    "markBrokerOutboxPublished",
                    () -> super.markBrokerOutboxPublished(
                            outboxId,
                            publishedAt
                    )
            );
        }

        @Override
        public boolean markBrokerOutboxPublishFailed(
                long outboxId,
                String error,
                long attemptedAt
        ) {
            return timed(
                    "markBrokerOutboxPublishFailed",
                    () -> super.markBrokerOutboxPublishFailed(
                            outboxId,
                            error,
                            attemptedAt
                    )
            );
        }

        @Override
        public boolean upsertPeerRecord(PeerRegistryRecord record) {
            return timed(
                    "upsertPeerRecord",
                    () -> super.upsertPeerRecord(record)
            );
        }
    }

    private static SchedulerConfig schedulerConfig(
            ScalingExperimentConfig config
    ) {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        return new SchedulerConfig(
                60_000L,
                120_000L,
                20,
                20_000,
                Math.max(1_000L, config.measuredJobCount() + 100L),
                Math.max(100_000L, config.taskCount() + 10_000L),
                Math.max(200_000L, config.taskCount() * 3L),
                300,
                1_000,
                1_000,
                64,
                8,
                1_000,
                10_000L,
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );
    }

    private static RabbitMqTransportConfig managedTransportConfig(
            String host,
            int port,
            String username,
            String password,
            String token
    ) {
        String name = "taskflow.scaling." + token;
        return new RabbitMqTransportConfig(
                host,
                port,
                username,
                password,
                "/",
                name + ".exchange",
                name,
                true,
                1,
                2_000L,
                true,
                name + ".dlx",
                name + ".dlq",
                "dead-letter",
                List.of(100L, 250L)
        );
    }

    private static ConnectionFactory connectionFactory(
            RabbitMqTransportConfig config
    ) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(false);
        factory.setTopologyRecoveryEnabled(false);
        return factory;
    }

    private record QueueDepth(
            long totalMessages,
            long maximumQueueMessages
    ) {
    }

    private static final class ManagementQueueProbe {
        private static final Duration REQUEST_TIMEOUT =
                Duration.ofSeconds(3L);

        private final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
        private final URI queuesUri;
        private final String authorization;
        private final String queuePrefix;

        private ManagementQueueProbe(
                String baseUrl,
                String username,
                String password,
                String queuePrefix
        ) {
            queuesUri = URI.create(
                    baseUrl.replaceFirst("/+$", "")
                            + "/api/queues/%2F?disable_stats=true"
            );
            authorization = "Basic " + Base64.getEncoder()
                    .encodeToString(
                            (username + ":" + password)
                                    .getBytes(StandardCharsets.UTF_8)
                    );
            this.queuePrefix = queuePrefix;
        }

        private QueueDepth depth() throws Exception {
            HttpRequest request = HttpRequest.newBuilder(queuesUri)
                    .timeout(REQUEST_TIMEOUT)
                    .header("Authorization", authorization)
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(
                    request,
                    HttpResponse.BodyHandlers.ofString()
            );
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "RabbitMQ management queue request failed with HTTP "
                                + response.statusCode()
                );
            }

            long total = 0L;
            long maximum = 0L;
            JsonArray queues = JsonParser.parseString(response.body())
                    .getAsJsonArray();
            for (JsonElement element : queues) {
                JsonObject queue = element.getAsJsonObject();
                JsonElement name = queue.get("name");
                if (name == null
                        || !name.getAsString().startsWith(queuePrefix)) {
                    continue;
                }
                JsonElement messages = queue.get("messages");
                long count = messages == null || messages.isJsonNull()
                        ? 0L
                        : messages.getAsLong();
                total += count;
                maximum = Math.max(maximum, count);
            }
            return new QueueDepth(total, maximum);
        }
    }

    private static String workerId(int index) {
        return "scaling-worker-" + index;
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name", "")
                .toLowerCase(Locale.ROOT)
                .contains("win") ? "java.exe" : "java";
        return Path.of(
                System.getProperty("java.home"),
                "bin",
                executable
        ).toString();
    }

    private static String testClasspath() {
        String classpath = System.getProperty("surefire.test.class.path");
        return classpath == null || classpath.isBlank()
                ? System.getProperty("java.class.path")
                : classpath;
    }

    private static String formatDistribution(Map<String, Long> distribution) {
        return distribution.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private static String formatWorkerExecutions(
            List<WorkerMetrics> workerMetrics
    ) {
        return workerMetrics.stream()
                .sorted(Comparator.comparing(WorkerMetrics::workerId))
                .map(metrics -> metrics.workerId()
                        + ":"
                        + metrics.measuredTasks())
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private static String formatWorkerUtilization(
            List<WorkerMetrics> workerMetrics,
            long measuredDurationNanos
    ) {
        return workerMetrics.stream()
                .sorted(Comparator.comparing(WorkerMetrics::workerId))
                .map(metrics -> metrics.workerId()
                        + ":"
                        + decimal(
                        metrics.measuredBusyNanos()
                                * 100.0D
                                / measuredDurationNanos
                ))
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private static String decimal(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static void writeLines(Path path, List<String> lines)
            throws IOException {
        Files.write(
                path,
                lines,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
        );
    }

    private static String safeRead(Path path) {
        try {
            return Files.exists(path)
                    ? Files.readString(path, StandardCharsets.UTF_8)
                    : "<missing>";
        } catch (Exception failure) {
            return "<unreadable: " + failure.getMessage() + ">";
        }
    }

    private static Throwable merge(
            Throwable existing,
            Throwable additional
    ) {
        if (existing == null) {
            return additional;
        }
        existing.addSuppressed(additional);
        return existing;
    }

    private interface CheckedBoolean {
        boolean getAsBoolean() throws Exception;
    }

    private record TaskLatency(
            String taskId,
            String jobId,
            String workerId,
            long assignmentLatencyNanos,
            long endToEndLatencyNanos
    ) {
    }

    private record WriteSample(String operation, long nanos) {
    }

    private record ResourceSample(
            long elapsedNanos,
            long heapUsedBytes,
            long processCpuNanos,
            long totalQueueDepth,
            long maxQueueDepth
    ) {
    }

    private record WorkerMetrics(
            String workerId,
            long measuredTasks,
            long measuredBusyNanos
    ) {
    }

    private record Audit(
            List<TaskLatency> taskLatencies,
            List<WriteSample> writeSamples,
            List<ResourceSample> resourceSamples,
            List<WorkerMetrics> workerMetrics,
            Map<String, Long> assignmentsByWorker,
            long workerExecutionCount,
            long workerBusyNanos
    ) {
    }
}
