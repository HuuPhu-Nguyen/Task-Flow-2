package server;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PeerDisconnectedMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.chaos.CorrectnessChaosConfig;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.rabbitmq.RabbitMqOutboxReplayer;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.scheduler.BrokerOutboxPublisher;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
import server.scheduler.SchedulerMetrics;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRecoveryPolicy;
import transport.rabbitmq.RabbitMqMessageCodec;
import transport.rabbitmq.RabbitMqTopology;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Opt-in TF-0706 experiment. Its name deliberately does not match Surefire's
 * normal test include patterns; invoke it through verify-correctness-chaos.ps1.
 */
public class CorrectnessChaosExperiment {
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_ID = "correctness-chaos-requester";
    private static final DockerImageName RABBITMQ_IMAGE =
            DockerImageName.parse("rabbitmq:3.13-management");
    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");
    private static final int TOXIPROXY_AMQP_PORT = 8_666;
    private static final int WORKER_CAPACITY = 8;
    private static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(25L);
    private static final long PROGRESS_POLL_NANOS =
            TimeUnit.SECONDS.toNanos(1L);
    private static final String DELIVERY_ATTEMPT_HEADER =
            "x-taskflow-delivery-attempt";
    private static final String ORIGINAL_ROUTING_KEY_HEADER =
            "x-taskflow-original-routing-key";
    private static final String ORIGINAL_EXCHANGE_HEADER =
            "x-taskflow-original-exchange";
    private static final String ORIGINAL_MESSAGE_ID_HEADER =
            "x-taskflow-original-message-id";

    @Test
    void allJobsTerminateAfterFailuresStop() throws Exception {
        CorrectnessChaosConfig config =
                CorrectnessChaosConfig.fromSystemProperties();
        boolean reportGrade = Boolean.getBoolean(
                "taskflow.chaos.reportGrade"
        );
        if (reportGrade) {
            config.requireReportGrade();
        }

        try (ChaosRun run = new ChaosRun(config, reportGrade)) {
            run.execute();
        }
    }

    private static final class ChaosRun implements AutoCloseable {
        private final CorrectnessChaosConfig config;
        private final boolean reportGrade;
        private final Path outputDirectory;
        private final Path databasePath;
        private final EventLog events;
        private final Network network = Network.newNetwork();
        private final RabbitMQContainer broker =
                new RabbitMQContainer(RABBITMQ_IMAGE)
                        .withNetwork(network)
                        .withNetworkAliases("rabbitmq");
        private final ToxiproxyContainer toxiproxy =
                new ToxiproxyContainer(TOXIPROXY_IMAGE)
                        .withNetwork(network);
        private final RabbitMqRecoveryPolicy recoveryPolicy =
                new RabbitMqRecoveryPolicy(1_000, 100L, 1_000L, 2.0D);
        private final ScheduledExecutorService delayedPublisher =
                Executors.newScheduledThreadPool(4, runnable -> {
                    Thread thread = new Thread(
                            runnable,
                            "correctness-chaos-delayed-result"
                    );
                    thread.setDaemon(true);
                    return thread;
                });
        private final ScheduledExecutorService workerController =
                Executors.newScheduledThreadPool(2, runnable -> {
                    Thread thread = new Thread(
                            runnable,
                            "correctness-chaos-worker-controller"
                    );
                    thread.setDaemon(true);
                    return thread;
                });
        private final AtomicReference<Throwable> asynchronousFailure =
                new AtomicReference<>();
        private final Set<Long> delayedOrdinals =
                ConcurrentHashMap.newKeySet();
        private final Set<Long> terminatedOrdinals =
                ConcurrentHashMap.newKeySet();
        private final Set<Long> duplicatedResultOrdinals =
                ConcurrentHashMap.newKeySet();
        private final Set<Long> duplicatedAssignmentOrdinals =
                ConcurrentHashMap.newKeySet();
        private final AtomicLong duplicateAssignmentsPublished =
                new AtomicLong();
        private final AtomicLong duplicateResultsPublished = new AtomicLong();
        private final AtomicLong delayedResultsPublished = new AtomicLong();
        private final AtomicLong workerTerminations = new AtomicLong();
        private final AtomicLong poisonDeliveries = new AtomicLong();
        private final AtomicReference<String> poisonAssignmentId =
                new AtomicReference<>();
        private final AtomicLong transportRedeliveries = new AtomicLong();
        private final AtomicLong transportQuarantines = new AtomicLong();
        private final AtomicLong schedulerDuplicateResults = new AtomicLong();
        private final AtomicLong schedulerStaleResults = new AtomicLong();
        private final AtomicLong minimumWorkerActiveTasks =
                new AtomicLong(Long.MAX_VALUE);
        private final Set<String> requesterJobResults =
                ConcurrentHashMap.newKeySet();
        private final List<WorkerRuntime> workers = new ArrayList<>();
        private final long poisonOrdinal;
        private volatile Map<String, Long> lastNonEmptyQueueDepths =
                Map.of();

        private RabbitMqTransportConfig transportConfig;
        private RabbitMqTransport requesterTransport;
        private CoordinatorRuntime coordinator;
        private java.sql.Connection auditConnection;
        private boolean brokerRestarted;
        private boolean coordinatorRestarted;
        private long pendingOutboxDuringBrokerOutage;
        private long startedAtMillis;
        private long finishedAtMillis;
        private boolean completed;

        private ChaosRun(
                CorrectnessChaosConfig config,
                boolean reportGrade
        ) throws IOException {
            this.config = config;
            this.reportGrade = reportGrade;
            this.outputDirectory = config.outputDirectory().toAbsolutePath();
            this.databasePath = outputDirectory.resolve(
                    "correctness-chaos.db"
            );
            Files.createDirectories(outputDirectory);
            Files.deleteIfExists(databasePath);
            Files.deleteIfExists(Path.of(databasePath + "-shm"));
            Files.deleteIfExists(Path.of(databasePath + "-wal"));
            this.events = new EventLog(
                    outputDirectory.resolve("events.jsonl")
            );
            this.poisonOrdinal = findPoisonOrdinal(config);
        }

        private void execute() throws Exception {
            startedAtMillis = System.currentTimeMillis();
            writeConfiguration();
            events.record("experiment_started", Map.of(
                    "seed", config.seed(),
                    "tasks", config.taskCount(),
                    "jobs", config.jobCount(),
                    "reportGrade", reportGrade
            ));

            startInfrastructure();
            submitJobs();
            driveFaultsAndAwaitCompletion();
            awaitCondition(
                    () -> requesterJobResults.size() == config.jobCount(),
                    60_000L,
                    "all terminal job results delivered to requester"
            );
            awaitCondition(
                    () -> coordinator.pendingOutboxRows() == 0L,
                    60_000L,
                    "durable broker outbox drain"
            );
            awaitCondition(
                    this::allWorkQueuesDrained,
                    60_000L,
                    "RabbitMQ work and retry queues drain"
            );
            sampleCapacity();
            audit();
            finishedAtMillis = System.currentTimeMillis();
            completed = true;
            writeAudit();
            events.record("experiment_completed", Map.of(
                    "elapsedMillis", finishedAtMillis - startedAtMillis,
                    "completedTasks", durableCompletedTaskCount(),
                    "terminalJobs", durableTerminalJobCount()
            ));
        }

        private void startInfrastructure() throws Exception {
            broker.start();
            toxiproxy.start();
            new ToxiproxyClient(
                    toxiproxy.getHost(),
                    toxiproxy.getControlPort()
            ).createProxy(
                    "rabbitmq-amqp",
                    "0.0.0.0:" + TOXIPROXY_AMQP_PORT,
                    "rabbitmq:5672"
            );

            String token = Long.toUnsignedString(config.seed(), 36)
                    + "-"
                    + UUID.randomUUID().toString().substring(0, 8);
            transportConfig = managedTransportConfig(
                    toxiproxy.getHost(),
                    toxiproxy.getMappedPort(TOXIPROXY_AMQP_PORT),
                    broker.getAdminUsername(),
                    broker.getAdminPassword(),
                    token
            );
            coordinator = new CoordinatorRuntime();
            coordinator.start();

            requesterTransport = new RabbitMqTransport(
                    transportConfig,
                    recoveryPolicy
            );
            requesterTransport.subscribePeer(
                    TransportRoute.JOB_RESULT,
                    REQUESTER_ID,
                    delivery -> {
                        if (!(delivery.message() instanceof JobResultMessage result)) {
                            throw new IllegalStateException(
                                    "Requester received a non-job-result message."
                            );
                        }
                        requesterJobResults.add(result.getJobId());
                    }
            );

            for (int index = 0; index < config.workerCount(); index++) {
                WorkerRuntime worker = new WorkerRuntime(
                        "correctness-chaos-worker-" + index
                );
                workers.add(worker);
                worker.start();
            }
            auditConnection = DriverManager.getConnection(
                    "jdbc:sqlite:" + databasePath
            );
            try (Statement statement = auditConnection.createStatement()) {
                statement.execute("PRAGMA busy_timeout = 10000");
            }
            events.record("infrastructure_started", Map.of(
                    "brokerContainerId", broker.getContainerId(),
                    "workers", workers.size(),
                    "database", databasePath.toString()
            ));
        }

        private void submitJobs() throws Exception {
            for (int jobIndex = 0; jobIndex < config.jobCount(); jobIndex++) {
                List<Object> payloads = new ArrayList<>(
                        config.tasksInJob(jobIndex)
                );
                int firstOrdinal = jobIndex * config.tasksPerJob();
                for (int offset = 0;
                     offset < config.tasksInJob(jobIndex);
                     offset++) {
                    payloads.add(Long.toString(firstOrdinal + offset));
                }
                String jobId = jobId(jobIndex);
                JobSubmitMessage submission = new JobSubmitMessage(
                        REQUESTER_ID,
                        Instant.now().toString(),
                        jobId,
                        TASK_TYPE,
                        payloads,
                        "",
                        "token-" + jobId
                );
                assertTrue(requesterTransport.publish(
                        new OutboundTransportMessage(
                                TransportRoute.JOB_SUBMIT,
                                REQUESTER_ID,
                                submission
                        )
                ), "Job submission was not confirmed for " + jobId);
            }
            awaitCondition(
                    () -> durableAcceptedTaskCount() == config.taskCount(),
                    120_000L,
                    "all configured tasks durably accepted"
            );
            events.record("workload_accepted", Map.of(
                    "jobs", config.jobCount(),
                    "tasks", durableAcceptedTaskCount()
            ));
        }

        private void driveFaultsAndAwaitCompletion() throws Exception {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(
                    config.completionTimeoutSeconds()
            );
            while (System.nanoTime() < deadline) {
                throwIfAsynchronousFailure();
                long completedTasks = durableCompletedTaskCount();
                sampleCapacity();

                if (!brokerRestarted
                        && completedTasks
                        >= config.brokerRestartAfterCompletions()
                        && currentTransportQuarantines() >= 1L
                        && workerTerminations.get()
                        == config.workerTerminationCount()) {
                    restartBroker(completedTasks);
                }
                if (!coordinatorRestarted
                        && completedTasks
                        >= config.coordinatorRestartAfterCompletions()
                        && brokerRestarted) {
                    restartCoordinator(completedTasks);
                }
                if (completedTasks == config.taskCount()
                        && durableTerminalJobCount() == config.jobCount()
                        && brokerRestarted
                        && coordinatorRestarted
                        && workerTerminations.get()
                        == config.workerTerminationCount()
                        && delayedResultsPublished.get()
                        == config.delayedResultCount()) {
                    return;
                }
                LockSupport.parkNanos(PROGRESS_POLL_NANOS);
            }
            fail("Timed out after "
                    + config.completionTimeoutSeconds()
                    + " seconds: completedTasks="
                    + durableCompletedTaskCount()
                    + ", terminalJobs="
                    + durableTerminalJobCount()
                    + ", workerTerminations="
                    + workerTerminations.get()
                    + ", delayedResultsPublished="
                    + delayedResultsPublished.get()
                    + asynchronousFailureDescription());
        }

        private void restartBroker(long completedTasks) throws Exception {
            brokerRestarted = true;
            events.record("broker_restart_started", Map.of(
                    "completedTasks", completedTasks
            ));
            stopBroker(broker);
            awaitCondition(
                    () -> !coordinator.connectionUsable(),
                    15_000L,
                    "coordinator observes broker outage"
            );
            awaitCondition(
                    () -> coordinator.pendingOutboxRows() > 0L,
                    Math.max(15_000L, config.leaseMillis() * 5L),
                    "committed unsent message retained in outbox"
            );
            pendingOutboxDuringBrokerOutage =
                    coordinator.pendingOutboxRows();
            startBroker(broker, transportConfig);
            awaitCondition(
                    () -> coordinator.connectionUsable()
                            && workers.stream()
                            .allMatch(WorkerRuntime::connectionUsable)
                            && requesterTransport.connectionUsable(),
                    60_000L,
                    "all RabbitMQ clients recover after broker restart"
            );
            events.record("broker_restart_completed", Map.of(
                    "pendingOutboxObserved",
                    pendingOutboxDuringBrokerOutage
            ));
        }

        private void restartCoordinator(long completedTasks) throws Exception {
            coordinatorRestarted = true;
            events.record("coordinator_restart_started", Map.of(
                    "completedTasks", completedTasks
            ));
            accumulateSchedulerMetrics(coordinator.metrics());
            coordinator.close();
            coordinator = new CoordinatorRuntime();
            coordinator.start();
            events.record("coordinator_restart_completed", Map.of(
                    "resumedJobs", durableRunningJobCount()
            ));
        }

        private void restartWorker(WorkerRuntime worker, long ordinal) {
            workerController.execute(() -> {
                try {
                    awaitCondition(
                            () -> currentTransportQuarantines() >= 1L,
                            15_000L,
                            "poison quarantine before worker termination"
                    );
                    events.record("worker_termination_started", Map.of(
                            "workerId", worker.workerId,
                            "taskOrdinal", ordinal
                    ));
                    worker.closeTransport();
                    CoordinatorRuntime current = coordinator;
                    if (current != null) {
                        current.workerUnavailable(worker.workerId);
                    }
                    worker.startTransport();
                    current = coordinator;
                    if (current != null) {
                        current.registerWorker(worker.workerId);
                    }
                    workerTerminations.incrementAndGet();
                    events.record("worker_termination_completed", Map.of(
                            "workerId", worker.workerId,
                            "taskOrdinal", ordinal
                    ));
                } catch (Throwable failure) {
                    asynchronousFailure.compareAndSet(null, failure);
                } finally {
                    worker.restartInProgress.set(false);
                }
            });
        }

        private void publishDelayedResult(
                WorkerRuntime worker,
                TaskAssignMessage assignment,
                long ordinal
        ) {
            delayedPublisher.schedule(() -> {
                long deadline = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(60L);
                Throwable lastFailure = null;
                while (System.nanoTime() < deadline) {
                    try {
                        worker.publishResult(assignment, ordinal);
                        delayedResultsPublished.incrementAndGet();
                        events.record("delayed_result_published", Map.of(
                                "taskOrdinal", ordinal,
                                "assignmentId",
                                assignment.getAssignmentId()
                        ));
                        return;
                    } catch (Throwable failure) {
                        lastFailure = failure;
                        LockSupport.parkNanos(POLL_NANOS);
                    }
                }
                asynchronousFailure.compareAndSet(
                        null,
                        new IllegalStateException(
                                "Delayed result could not be published for "
                                        + assignment.getAssignmentId(),
                                lastFailure
                        )
                );
            }, config.delayedResultMillis(), TimeUnit.MILLISECONDS);
        }

        private void audit() throws Exception {
            throwIfAsynchronousFailure();
            accumulateSchedulerMetrics(coordinator.metrics());

            assertEquals(config.taskCount(), durableAcceptedTaskCount());
            assertEquals(config.taskCount(), durableCompletedTaskCount());
            assertEquals(config.jobCount(), durableTerminalJobCount());
            assertEquals(0L, queryLong(
                    "SELECT COUNT(*) FROM jobs WHERE status <> 'COMPLETED'"
            ));
            assertEquals(config.delayedResultCount(), delayedOrdinals.size());
            assertEquals(
                    config.workerTerminationCount(),
                    terminatedOrdinals.size()
            );
            assertEquals(
                    config.workerTerminationCount(),
                    workerTerminations.get()
            );
            assertEquals(
                    expectedDuplicateCount(),
                    duplicateAssignmentsPublished.get()
            );
            assertEquals(
                    expectedDuplicateCount(),
                    duplicateResultsPublished.get()
            );
            assertTrue(
                    schedulerDuplicateResults.get() > 0L,
                    "Coordinator did not classify any duplicate result."
            );
            assertTrue(
                    schedulerStaleResults.get() > 0L,
                    "Coordinator did not reject any beyond-lease result."
            );
            assertTrue(pendingOutboxDuringBrokerOutage > 0L);
            assertEquals(0L, coordinator.pendingOutboxRows());
            assertTrue(coordinator.capacityProjectionValid());
            assertTrue(minimumWorkerActiveTasks.get() >= 0L);

            long succeededAttempts = 0L;
            long totalAttempts = 0L;
            DatabaseManager database = coordinator.database();
            for (DatabaseManager.JobRecord job : database.getJobHistory()) {
                List<JobStateStore.TaskAttemptRecord> attempts =
                        database.loadTaskAttempts(job.jobId());
                totalAttempts += attempts.size();
                Map<String, List<JobStateStore.TaskAttemptRecord>> byTask =
                        new LinkedHashMap<>();
                for (JobStateStore.TaskAttemptRecord attempt : attempts) {
                    byTask.computeIfAbsent(
                            attempt.taskId(),
                            ignored -> new ArrayList<>()
                    ).add(attempt);
                }
                for (DatabaseManager.TaskRecord task
                        : database.getTasksForJob(job.jobId())) {
                    List<JobStateStore.TaskAttemptRecord> taskAttempts =
                            byTask.getOrDefault(task.taskId(), List.of());
                    List<JobStateStore.TaskAttemptRecord> successes =
                            taskAttempts.stream()
                                    .filter(attempt ->
                                            attempt.outcome()
                                                    == JobStateStore
                                                    .TaskAttemptOutcome
                                                    .SUCCEEDED)
                                    .toList();
                    assertEquals(
                            1,
                            successes.size(),
                            "Task must have exactly one authoritative success: "
                                    + task.taskId()
                    );
                    JobStateStore.TaskAttemptRecord success =
                            successes.getFirst();
                    assertEquals(task.attemptNumber(), success.attemptNumber());
                    assertEquals(task.assignmentId(), success.assignmentId());
                    succeededAttempts++;
                }
            }
            assertEquals(config.taskCount(), succeededAttempts);
            assertEquals(
                    totalAttempts,
                    queryLong("SELECT COUNT(*) FROM task_attempts")
            );
            assertTrue(queryLong(
                    "SELECT COUNT(*) FROM broker_outbox "
                            + "WHERE route='TASK_ASSIGN'"
            ) >= totalAttempts);
            assertTrue(queryLong(
                    "SELECT COUNT(*) FROM broker_outbox "
                            + "WHERE route='JOB_RESULT'"
            ) >= config.jobCount());
            assertEquals(
                    coordinator.pendingOutboxRows(),
                    queryLong(
                            "SELECT COUNT(*) FROM broker_outbox "
                                    + "WHERE published_at IS NULL"
                    )
            );

            for (WorkerRuntime worker : workers) {
                worker.accumulateTransportMetrics();
            }
            assertTrue(
                    transportQuarantines.get() >= 1L,
                    "Injected poison delivery was not quarantined."
            );
            assertTrue(
                    poisonDeliveries.get()
                            <= transportConfig.maxDeliveryAttempts() + 1L,
                    "Poison redelivery exceeded its configured bound."
            );
            assertEquals(
                    1L,
                    queueDepth(
                            new RabbitMqTopology(transportConfig)
                                    .deadLetterQuarantineQueueName()
                    ),
                    "The single injected poison message must be quarantined."
            );
            assertTrue(allWorkQueuesDrained());
        }

        private long expectedDuplicateCount() {
            long count = 0L;
            for (long ordinal = 0L;
                 ordinal < config.taskCount();
                 ordinal++) {
                if (config.duplicateAssignment(ordinal)) {
                    count++;
                }
            }
            return count;
        }

        private void writeConfiguration() throws IOException {
            Properties properties = new Properties();
            properties.setProperty("seed", Long.toString(config.seed()));
            properties.setProperty(
                    "reportGrade",
                    Boolean.toString(reportGrade)
            );
            properties.setProperty(
                    "taskCount",
                    Integer.toString(config.taskCount())
            );
            properties.setProperty(
                    "tasksPerJob",
                    Integer.toString(config.tasksPerJob())
            );
            properties.setProperty(
                    "jobCount",
                    Integer.toString(config.jobCount())
            );
            properties.setProperty(
                    "workerCount",
                    Integer.toString(config.workerCount())
            );
            properties.setProperty(
                    "workerCapacity",
                    Integer.toString(WORKER_CAPACITY)
            );
            properties.setProperty(
                    "duplicateBasisPoints",
                    Integer.toString(config.duplicateBasisPoints())
            );
            properties.setProperty(
                    "delayedResultCount",
                    Integer.toString(config.delayedResultCount())
            );
            properties.setProperty(
                    "workerTerminationCount",
                    Integer.toString(config.workerTerminationCount())
            );
            properties.setProperty(
                    "brokerRestartAfterCompletions",
                    Integer.toString(
                            config.brokerRestartAfterCompletions()
                    )
            );
            properties.setProperty(
                    "coordinatorRestartAfterCompletions",
                    Integer.toString(
                            config.coordinatorRestartAfterCompletions()
                    )
            );
            properties.setProperty(
                    "leaseMillis",
                    Long.toString(config.leaseMillis())
            );
            properties.setProperty(
                    "delayedResultMillis",
                    Long.toString(config.delayedResultMillis())
            );
            try (var writer = Files.newBufferedWriter(
                    outputDirectory.resolve("configuration.properties"),
                    StandardCharsets.UTF_8
            )) {
                properties.store(writer, "TF-0706 correctness chaos");
            }
        }

        private void writeAudit() throws IOException {
            Properties properties = new Properties();
            properties.setProperty("result", "PASS");
            properties.setProperty(
                    "elapsedMillis",
                    Long.toString(finishedAtMillis - startedAtMillis)
            );
            properties.setProperty(
                    "acceptedTasks",
                    Long.toString(durableAcceptedTaskCount())
            );
            properties.setProperty(
                    "completedTasks",
                    Long.toString(durableCompletedTaskCount())
            );
            properties.setProperty(
                    "terminalJobs",
                    Long.toString(durableTerminalJobCount())
            );
            properties.setProperty(
                    "duplicateAssignmentsPublished",
                    Long.toString(duplicateAssignmentsPublished.get())
            );
            properties.setProperty(
                    "duplicateResultsPublished",
                    Long.toString(duplicateResultsPublished.get())
            );
            properties.setProperty(
                    "schedulerDuplicateResults",
                    Long.toString(schedulerDuplicateResults.get())
            );
            properties.setProperty(
                    "schedulerStaleResults",
                    Long.toString(schedulerStaleResults.get())
            );
            properties.setProperty(
                    "delayedResultsPublished",
                    Long.toString(delayedResultsPublished.get())
            );
            properties.setProperty(
                    "workerTerminations",
                    Long.toString(workerTerminations.get())
            );
            properties.setProperty("brokerRestarts", "1");
            properties.setProperty("coordinatorRestarts", "1");
            properties.setProperty(
                    "pendingOutboxDuringBrokerOutage",
                    Long.toString(pendingOutboxDuringBrokerOutage)
            );
            properties.setProperty(
                    "pendingOutboxAtCompletion",
                    Long.toString(coordinator.pendingOutboxRows())
            );
            properties.setProperty(
                    "minimumWorkerActiveTasks",
                    Long.toString(minimumWorkerActiveTasks.get())
            );
            properties.setProperty(
                    "poisonDeliveries",
                    Long.toString(poisonDeliveries.get())
            );
            properties.setProperty(
                    "transportRedeliveries",
                    Long.toString(transportRedeliveries.get())
            );
            properties.setProperty(
                    "transportQuarantines",
                    Long.toString(transportQuarantines.get())
            );
            try (var writer = Files.newBufferedWriter(
                    outputDirectory.resolve("audit.properties"),
                    StandardCharsets.UTF_8
            )) {
                properties.store(writer, "TF-0706 correctness chaos audit");
            }
        }

        private long durableAcceptedTaskCount() {
            return queryLong("SELECT COUNT(*) FROM tasks");
        }

        private long durableCompletedTaskCount() {
            return queryLong(
                    "SELECT COUNT(*) FROM tasks WHERE status='COMPLETED'"
            );
        }

        private long durableTerminalJobCount() {
            return queryLong(
                    "SELECT COUNT(*) FROM jobs "
                            + "WHERE status IN ('COMPLETED','FAILED')"
            );
        }

        private long durableRunningJobCount() {
            return queryLong(
                    "SELECT COUNT(*) FROM jobs WHERE status='RUNNING'"
            );
        }

        private synchronized long queryLong(String sql) {
            if (auditConnection == null) {
                return 0L;
            }
            try (Statement statement = auditConnection.createStatement();
                 ResultSet result = statement.executeQuery(sql)) {
                return result.next() ? result.getLong(1) : 0L;
            } catch (Exception failure) {
                throw new IllegalStateException(
                        "Chaos audit query failed: " + sql,
                        failure
                );
            }
        }

        private void sampleCapacity() {
            CoordinatorRuntime current = coordinator;
            if (current == null) {
                return;
            }
            for (PeerInfo peer : current.peers()) {
                minimumWorkerActiveTasks.accumulateAndGet(
                        peer.getActiveTasks(),
                        Math::min
                );
            }
            assertTrue(current.capacityProjectionValid());
        }

        private long currentTransportQuarantines() {
            long current = transportQuarantines.get();
            for (WorkerRuntime worker : workers) {
                current += worker.currentTransportQuarantines();
            }
            return current;
        }

        private boolean allWorkQueuesDrained() {
            if (transportConfig == null || !broker.isRunning()) {
                return false;
            }
            RabbitMqTopology topology = new RabbitMqTopology(transportConfig);
            try (Connection connection =
                         connectionFactory(transportConfig).newConnection();
                 Channel channel = connection.createChannel()) {
                Map<String, Long> nonEmpty = new LinkedHashMap<>();
                for (String queue : topology.queueNames().values()) {
                    long depth = queueDepth(channel, queue);
                    if (depth != 0L) {
                        nonEmpty.put(queue, depth);
                    }
                }
                // Peer endpoints are exclusive auto-delete queues. A broker
                // restart may remove them before automatic topology recovery;
                // durable completion plus requester receipt below proves that
                // no authoritative work remains in those ephemeral queues.
                for (int stage = 1;
                     stage <= topology.retryStageCount();
                     stage++) {
                    String queue = topology.retryQueueName(stage);
                    long depth = queueDepth(channel, queue);
                    if (depth != 0L) {
                        nonEmpty.put(queue, depth);
                    }
                }
                lastNonEmptyQueueDepths = Map.copyOf(nonEmpty);
                return nonEmpty.isEmpty();
            } catch (Exception unavailable) {
                return false;
            }
        }

        private long queueDepth(String queueName) throws Exception {
            ConnectionFactory factory = connectionFactory(transportConfig);
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                return queueDepth(channel, queueName);
            }
        }

        private static long queueDepth(
                Channel channel,
                String queueName
        ) throws Exception {
            return channel.queueDeclarePassive(queueName)
                    .getMessageCount();
        }

        private void accumulateSchedulerMetrics(
                SchedulerMetrics.Snapshot snapshot
        ) {
            if (snapshot == null) {
                return;
            }
            schedulerDuplicateResults.addAndGet(
                    snapshot.duplicateResultCount()
            );
            schedulerStaleResults.addAndGet(snapshot.staleResultCount());
        }

        private void awaitCondition(
                CheckedBoolean condition,
                long timeoutMillis,
                String description
        ) throws Exception {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            Throwable lastFailure = null;
            while (System.nanoTime() < deadline) {
                throwIfAsynchronousFailure();
                try {
                    if (condition.getAsBoolean()) {
                        return;
                    }
                } catch (Throwable failure) {
                    lastFailure = failure;
                }
                LockSupport.parkNanos(POLL_NANOS);
            }
            throw new IllegalStateException(
                    "Timed out waiting for "
                            + description
                            + ("RabbitMQ work and retry queues drain"
                            .equals(description)
                            ? "; nonEmptyQueues="
                            + lastNonEmptyQueueDepths
                            : ""),
                    lastFailure
            );
        }

        private void throwIfAsynchronousFailure() {
            Throwable failure = asynchronousFailure.get();
            if (failure != null) {
                throw new IllegalStateException(
                        "Asynchronous chaos component failed.",
                        failure
                );
            }
        }

        private String asynchronousFailureDescription() {
            Throwable failure = asynchronousFailure.get();
            return failure == null
                    ? ""
                    : ", asynchronousFailure=" + failure;
        }

        @Override
        public void close() throws Exception {
            Throwable closeFailure = null;
            delayedPublisher.shutdownNow();
            workerController.shutdownNow();
            for (WorkerRuntime worker : workers) {
                try {
                    worker.close();
                } catch (Throwable failure) {
                    closeFailure = merge(closeFailure, failure);
                }
            }
            if (requesterTransport != null) {
                try {
                    requesterTransport.close();
                } catch (Throwable failure) {
                    closeFailure = merge(closeFailure, failure);
                }
            }
            if (coordinator != null) {
                try {
                    coordinator.close();
                } catch (Throwable failure) {
                    closeFailure = merge(closeFailure, failure);
                }
            }
            if (auditConnection != null) {
                try {
                    auditConnection.close();
                } catch (Throwable failure) {
                    closeFailure = merge(closeFailure, failure);
                }
            }
            try {
                events.close();
            } catch (Throwable failure) {
                closeFailure = merge(closeFailure, failure);
            }
            try {
                toxiproxy.close();
            } catch (Throwable failure) {
                closeFailure = merge(closeFailure, failure);
            }
            try {
                broker.close();
            } catch (Throwable failure) {
                closeFailure = merge(closeFailure, failure);
            }
            try {
                network.close();
            } catch (Throwable failure) {
                closeFailure = merge(closeFailure, failure);
            }
            if (!completed && closeFailure != null) {
                throw new IllegalStateException(
                        "Chaos cleanup failed after an incomplete run.",
                        closeFailure
                );
            }
        }

        private final class CoordinatorRuntime implements AutoCloseable {
            private RabbitMqTransport transport;
            private DatabaseManager database;
            private InMemoryPeerRegistry registry;
            private TaskScheduler scheduler;
            private Thread schedulerThread;
            private RabbitMqOutboxReplayer outboxReplayer;
            private SchedulerMailbox.BrokerIngress ingress;
            private BlockingQueue<MessageEnvelope> mailbox;
            private FastConfirmedPublisher assignmentPublisher;
            private boolean closed;

            private void start() throws Exception {
                transport = new RabbitMqTransport(
                        transportConfig,
                        recoveryPolicy
                );
                transport.declareTopology();
                database = new DatabaseManager(databasePath.toString());
                CoordinatorStartupRecovery.RecoveryResult recovery =
                        CoordinatorStartupRecovery.recoverPersistedJobs(
                                database
                        );
                assertTrue(
                        recovery.successful(),
                        "Coordinator startup recovery failed."
                );

                SchedulerConfig schedulerConfig = schedulerConfig(config);
                registry = new InMemoryPeerRegistry(database);
                registerAllWorkers();
                mailbox = SchedulerMailbox.create(schedulerConfig);
                ingress = SchedulerMailbox.brokerIngress(mailbox);
                DuplicatingSchedulerOutput output =
                        new DuplicatingSchedulerOutput(
                                new RabbitMqSchedulerOutput(transport)
                        );
                assignmentPublisher = new FastConfirmedPublisher();
                scheduler = new TaskScheduler(
                        mailbox,
                        registry,
                        database,
                        output,
                        schedulerConfig
                );
                scheduler.restoreJobs(
                        recovery.resumedJobs(),
                        recovery.requesterTokenHashes(),
                        recovery.requesterIdentityKeys()
                );
                outboxReplayer = new RabbitMqOutboxReplayer(
                        database,
                        output,
                        schedulerConfig.schedulerOutboxBatchSize()
                );
                transport.subscribe(
                        TransportRoute.JOB_SUBMIT,
                        this::enqueueForScheduler
                );
                transport.subscribe(
                        TransportRoute.TASK_RESULT,
                        this::enqueueForScheduler
                );
                schedulerThread = new Thread(
                        scheduler,
                        "correctness-chaos-coordinator"
                );
                schedulerThread.start();
                outboxReplayer.start();
            }

            private void enqueueForScheduler(
                    InboundTransportMessage delivery
            ) throws Exception {
                TransportAcknowledgement acknowledgement =
                        delivery.acknowledgement();
                InboundTransportMessage observed = acknowledgement == null
                        ? delivery
                        : new InboundTransportMessage(
                        delivery.route(),
                        delivery.fromNodeId(),
                        delivery.message(),
                        new ObservedAcknowledgement(
                                acknowledgement,
                                delivery
                        )
                );
                ingress.offer(observed);
            }

            private void registerAllWorkers() {
                for (int index = 0;
                     index < config.workerCount();
                     index++) {
                    registerWorker("correctness-chaos-worker-" + index);
                }
            }

            private synchronized void registerWorker(String workerId) {
                PeerInfo peer = new PeerInfo(
                        workerId,
                        schedulerConfig(config),
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
                if (scheduler != null) {
                    scheduler.requestSchedulingRecheck();
                }
            }

            private synchronized void workerUnavailable(String workerId) {
                SchedulerMailbox.offer(
                        mailbox,
                        new MessageEnvelope(
                                new PeerDisconnectedMessage(
                                        workerId,
                                        Instant.now().toString(),
                                        "injected_worker_termination"
                                ),
                                workerId
                        )
                );
                registry.remove(workerId);
                scheduler.requestSchedulingRecheck();
            }

            private long pendingOutboxRows() {
                return database.countPendingBrokerOutbox().count();
            }

            private boolean connectionUsable() {
                return transport != null && transport.connectionUsable();
            }

            private boolean capacityProjectionValid() {
                return registry.capacityProjectionValid();
            }

            private List<PeerInfo> peers() {
                return List.copyOf(registry.getAllPeers());
            }

            private DatabaseManager database() {
                return database;
            }

            private SchedulerMetrics.Snapshot metrics() {
                return scheduler == null
                        ? null
                        : scheduler.getMetricsSnapshot();
            }

            private final class ObservedAcknowledgement
                    implements TransportAcknowledgement {
                private final TransportAcknowledgement delegate;
                private final InboundTransportMessage delivery;

                private ObservedAcknowledgement(
                        TransportAcknowledgement delegate,
                        InboundTransportMessage delivery
                ) {
                    this.delegate = delegate;
                    this.delivery = delivery;
                }

                @Override
                public void settle(
                        DeliveryDisposition disposition,
                        String reasonCode
                ) throws Exception {
                    if (disposition != DeliveryDisposition.ACK_SUCCESS) {
                        Map<String, Object> fields = new LinkedHashMap<>();
                        fields.put("route", delivery.route().name());
                        fields.put("fromNodeId", delivery.fromNodeId());
                        fields.put("disposition", disposition.name());
                        fields.put("reasonCode", reasonCode);
                        if (delivery.message()
                                instanceof TaskResultMessage result) {
                            fields.put("taskId", result.getTaskId());
                            fields.put(
                                    "assignmentId",
                                    result.getAssignmentId()
                            );
                        }
                        events.record(
                                "coordinator_delivery_disposition",
                                fields
                        );
                    }
                    delegate.settle(disposition, reasonCode);
                }

                @Override
                public void ack() throws Exception {
                    delegate.ack();
                }

                @Override
                public void requeue() throws Exception {
                    delegate.requeue();
                }

                @Override
                public void reject() throws Exception {
                    delegate.reject();
                }

                @Override
                public void defer() {
                    delegate.defer();
                }
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
                if (transport != null) {
                    transport.close();
                }
                if (assignmentPublisher != null) {
                    assignmentPublisher.close();
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
                            "Coordinator scheduler did not stop."
                    );
                }
                if (database != null) {
                    database.close();
                }
            }

            private final class DuplicatingSchedulerOutput
                    implements SchedulerOutput, BrokerOutboxPublisher {
                private final RabbitMqSchedulerOutput delegate;

                private DuplicatingSchedulerOutput(
                        RabbitMqSchedulerOutput delegate
                ) {
                    this.delegate = delegate;
                }

                @Override
                public void sendTask(
                        PeerInfo peer,
                        TaskAssignMessage message
                ) throws Exception {
                    BrokerOutboxStore.OutboxMessage routed =
                            delegate.taskAssignmentOutboxMessage(
                                    peer,
                                    message
                            );
                    if (!publishAssignment(routed)) {
                        throw new IllegalStateException(
                                "Task assignment was not confirmed."
                        );
                    }
                }

                @Override
                public boolean sendJobResult(
                        String requesterNodeId,
                        JobResultMessage message
                ) throws Exception {
                    return delegate.sendJobResult(
                            requesterNodeId,
                            message
                    );
                }

                @Override
                public BrokerOutboxStore.OutboxMessage
                taskAssignmentOutboxMessage(
                        PeerInfo peer,
                        TaskAssignMessage message
                ) {
                    return delegate.taskAssignmentOutboxMessage(
                            peer,
                            message
                    );
                }

                @Override
                public BrokerOutboxStore.OutboxMessage
                jobResultOutboxMessage(
                        String requesterNodeId,
                        JobResultMessage message
                ) {
                    return delegate.jobResultOutboxMessage(
                            requesterNodeId,
                            message
                    );
                }

                @Override
                public synchronized boolean publishOutbox(
                        BrokerOutboxStore.OutboxRecord record
                ) throws Exception {
                    if (record.message().route()
                            != TransportRoute.TASK_ASSIGN) {
                        return delegate.publishOutbox(record);
                    }
                    boolean published = publishAssignment(record.message());
                    if (!published
                            || !(record.message().message()
                            instanceof TaskAssignMessage assignment)) {
                        return published;
                    }
                    long ordinal = assignmentOrdinal(assignment);
                    if (!config.duplicateAssignment(ordinal)
                            || duplicatedAssignmentOrdinals
                            .contains(ordinal)) {
                        return true;
                    }
                    if (!publishAssignment(record.message())) {
                        return false;
                    }
                    duplicatedAssignmentOrdinals.add(ordinal);
                    duplicateAssignmentsPublished.incrementAndGet();
                    return true;
                }

                private boolean publishAssignment(
                        BrokerOutboxStore.OutboxMessage message
                ) throws Exception {
                    if (message.route() != TransportRoute.TASK_ASSIGN) {
                        throw new IllegalArgumentException(
                                "Fast publisher only accepts assignments."
                        );
                    }
                    return assignmentPublisher.publish(
                            new OutboundTransportMessage(
                                    message.route(),
                                    message.fromNodeId(),
                                    message.message()
                            ),
                            new RabbitMqTopology(transportConfig)
                                    .peerRoutingKey(
                                    message.route(),
                                    message.peerNodeId()
                            )
                    );
                }
            }
        }

        private final class WorkerRuntime implements AutoCloseable {
            private final String workerId;
            private final Set<String> completedAssignments =
                    ConcurrentHashMap.newKeySet();
            private final AtomicBoolean restartInProgress =
                    new AtomicBoolean();
            private volatile RabbitMqTransport transport;
            private volatile FastConfirmedPublisher resultPublisher;
            private boolean closed;

            private WorkerRuntime(String workerId) {
                this.workerId = workerId;
            }

            private void start() throws Exception {
                startTransport();
            }

            private synchronized void startTransport() throws Exception {
                if (closed) {
                    throw new IllegalStateException(
                            "Cannot restart a closed worker."
                    );
                }
                RabbitMqTransport replacement = new RabbitMqTransport(
                        transportConfig,
                        recoveryPolicy
                );
                FastConfirmedPublisher publisher = null;
                try {
                    publisher = new FastConfirmedPublisher();
                    replacement.subscribePeer(
                            TransportRoute.TASK_ASSIGN,
                            workerId,
                            this::handleAssignment
                    );
                    resultPublisher = publisher;
                    transport = replacement;
                } catch (Exception failure) {
                    if (publisher != null) {
                        publisher.close();
                    }
                    replacement.close();
                    throw failure;
                }
            }

            private void handleAssignment(
                    transport.InboundTransportMessage delivery
            ) throws Exception {
                if (!(delivery.message()
                        instanceof TaskAssignMessage assignment)) {
                    throw new IllegalStateException(
                            "Worker received a non-assignment message."
                    );
                }
                long ordinal = assignmentOrdinal(assignment);
                String assignmentId = assignment.getAssignmentId();
                assertNotNull(assignmentId);

                if (ordinal == poisonOrdinal) {
                    poisonAssignmentId.compareAndSet(null, assignmentId);
                }
                if (assignmentId.equals(poisonAssignmentId.get())) {
                    poisonDeliveries.incrementAndGet();
                    throw new IllegalStateException(
                            "injected_deterministic_poison"
                    );
                }
                if (completedAssignments.contains(assignmentId)) {
                    return;
                }

                if (config.terminateWorker(ordinal)
                        && !terminatedOrdinals.contains(ordinal)
                        && restartInProgress.compareAndSet(false, true)) {
                    delivery.acknowledgement().defer();
                    terminatedOrdinals.add(ordinal);
                    completedAssignments.add(assignmentId);
                    restartWorker(this, ordinal);
                    return;
                }

                if (config.delayResult(ordinal)
                        && delayedOrdinals.add(ordinal)) {
                    completedAssignments.add(assignmentId);
                    publishDelayedResult(this, assignment, ordinal);
                    return;
                }

                publishResult(assignment, ordinal);
                completedAssignments.add(assignmentId);
            }

            private void publishResult(
                    TaskAssignMessage assignment,
                    long ordinal
            ) throws Exception {
                TaskResultMessage result = new TaskResultMessage(
                        workerId,
                        Instant.now().toString(),
                        assignment.getTaskId(),
                        assignment.getJobId(),
                        assignment.getAttemptNumber(),
                        assignment.getAssignmentId(),
                        assignment.getPayload(),
                        true,
                        ""
                );
                FastConfirmedPublisher current = resultPublisher;
                if (current == null || !current.publish(
                        new OutboundTransportMessage(
                                TransportRoute.TASK_RESULT,
                                workerId,
                                result
                        ),
                        TransportRoute.TASK_RESULT.routingKey()
                )) {
                    throw new IllegalStateException(
                            "Task-result publication was not confirmed."
                    );
                }
                if (config.duplicateResult(ordinal)
                        && duplicatedResultOrdinals.add(ordinal)) {
                    try {
                        if (!current.publish(new OutboundTransportMessage(
                                TransportRoute.TASK_RESULT,
                                workerId,
                                result
                        ), TransportRoute.TASK_RESULT.routingKey())) {
                            throw new IllegalStateException(
                                    "Duplicate task-result publication "
                                            + "was not confirmed."
                            );
                        }
                        duplicateResultsPublished.incrementAndGet();
                    } catch (Exception failure) {
                        duplicatedResultOrdinals.remove(ordinal);
                        throw failure;
                    }
                }
            }

            private synchronized void closeTransport() throws Exception {
                RabbitMqTransport current = transport;
                FastConfirmedPublisher currentPublisher = resultPublisher;
                transport = null;
                resultPublisher = null;
                if (currentPublisher != null) {
                    currentPublisher.close();
                }
                if (current != null) {
                    accumulateTransportMetrics(current);
                    try {
                        current.close();
                    } catch (com.rabbitmq.client.AlreadyClosedException ignored) {
                        // Broker termination already closed this worker.
                    }
                }
            }

            private boolean connectionUsable() {
                RabbitMqTransport current = transport;
                return current != null && current.connectionUsable();
            }

            private synchronized void accumulateTransportMetrics() {
                if (transport != null) {
                    accumulateTransportMetrics(transport);
                }
            }

            private long currentTransportQuarantines() {
                RabbitMqTransport current = transport;
                return current == null
                        ? 0L
                        : current.metricsSnapshot().quarantinedTotal();
            }

            private void accumulateTransportMetrics(
                    RabbitMqTransport source
            ) {
                var snapshot = source.metricsSnapshot();
                transportRedeliveries.addAndGet(
                        snapshot.redeliveriesTotal()
                );
                transportQuarantines.addAndGet(
                        snapshot.quarantinedTotal()
                );
            }

            @Override
            public synchronized void close() throws Exception {
                if (closed) {
                    return;
                }
                closed = true;
                RabbitMqTransport current = transport;
                FastConfirmedPublisher currentPublisher = resultPublisher;
                transport = null;
                resultPublisher = null;
                if (currentPublisher != null) {
                    currentPublisher.close();
                }
                if (current != null) {
                    accumulateTransportMetrics(current);
                    try {
                        current.close();
                    } catch (com.rabbitmq.client.AlreadyClosedException ignored) {
                        // Broker termination already closed this worker.
                    }
                }
            }
        }

        /**
         * The experiment predeclares every destination. This bounded publisher
         * preserves protocol encoding, persistent delivery, retry-origin
         * headers, and broker confirms without imposing the production
         * adapter's mandatory peer-route observation delay on each lightweight
         * assignment or result.
         */
        private final class FastConfirmedPublisher implements AutoCloseable {
            private final RabbitMqMessageCodec codec =
                    new RabbitMqMessageCodec();
            private final Connection connection;
            private final Channel channel;
            private final RabbitMqTopology topology =
                    new RabbitMqTopology(transportConfig);

            private FastConfirmedPublisher() throws Exception {
                connection = connectionFactory(transportConfig).newConnection();
                channel = connection.createChannel();
                channel.confirmSelect();
            }

            private synchronized boolean publish(
                    OutboundTransportMessage outbound,
                    String routingKey
            ) throws Exception {
                String messageId = UUID.randomUUID().toString();
                AMQP.BasicProperties properties =
                        new AMQP.BasicProperties.Builder()
                                .messageId(messageId)
                                .contentType("application/json")
                                .contentEncoding(
                                        StandardCharsets.UTF_8.name()
                                )
                                .deliveryMode(2)
                                .timestamp(
                                        java.util.Date.from(Instant.now())
                                )
                                .headers(Map.of(
                                        DELIVERY_ATTEMPT_HEADER,
                                        1,
                                        ORIGINAL_ROUTING_KEY_HEADER,
                                        routingKey,
                                        ORIGINAL_EXCHANGE_HEADER,
                                        topology.exchangeName(),
                                        ORIGINAL_MESSAGE_ID_HEADER,
                                        messageId
                                ))
                                .build();
                channel.basicPublish(
                        topology.exchangeName(),
                        routingKey,
                        false,
                        properties,
                        codec.encode(outbound)
                );
                return channel.waitForConfirms(
                        transportConfig.publisherConfirmTimeoutMillis()
                );
            }

            @Override
            public void close() {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (Exception ignored) {
                    // A broker restart may already own this close edge.
                } finally {
                    try {
                        if (connection.isOpen()) {
                            connection.close();
                        }
                    } catch (Exception ignored) {
                        // A broker restart may already own this close edge.
                    }
                }
            }
        }
    }

    private static SchedulerConfig schedulerConfig(
            CorrectnessChaosConfig config
    ) {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        return new SchedulerConfig(
                60_000L,
                config.leaseMillis(),
                20,
                20_000,
                Math.max(1_000L, config.jobCount() + 100L),
                Math.max(200_000L, config.taskCount() + 10_000L),
                Math.max(250_000L, config.taskCount() * 3L),
                300,
                1_000,
                1_000,
                32,
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
        String name = "taskflow.correctness.chaos." + token;
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

    private static long findPoisonOrdinal(
            CorrectnessChaosConfig config
    ) {
        for (long ordinal = 0L;
             ordinal < config.taskCount();
             ordinal++) {
            if (!config.delayResult(ordinal)
                    && !config.terminateWorker(ordinal)
                    && !config.duplicateAssignment(ordinal)
                    && !config.duplicateResult(ordinal)) {
                return ordinal;
            }
        }
        throw new IllegalArgumentException(
                "Workload does not contain a task available for poison."
        );
    }

    private static long assignmentOrdinal(TaskAssignMessage assignment) {
        try {
            return Long.parseLong(String.valueOf(assignment.getPayload()));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "Chaos assignment payload is not an ordinal: "
                            + assignment.getPayload(),
                    failure
            );
        }
    }

    private static String jobId(int jobIndex) {
        return "correctness-chaos-job-%06d".formatted(jobIndex);
    }

    private static void stopBroker(RabbitMQContainer broker) {
        broker.getDockerClient()
                .stopContainerCmd(broker.getContainerId())
                .withTimeout(20)
                .exec();
    }

    private static void startBroker(
            RabbitMQContainer broker,
            RabbitMqTransportConfig config
    ) throws Exception {
        broker.getDockerClient()
                .startContainerCmd(broker.getContainerId())
                .exec();
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(45L);
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            var state = broker.getCurrentContainerInfo().getState();
            if (!Boolean.TRUE.equals(state.getRunning())) {
                throw new IllegalStateException(
                        "Managed RabbitMQ exited during restart: "
                                + state.getStatus()
                                + ", error="
                                + state.getError()
                );
            }
            try (Connection connection =
                         connectionFactory(config).newConnection()) {
                return;
            } catch (Throwable unavailable) {
                lastFailure = unavailable;
                LockSupport.parkNanos(POLL_NANOS);
            }
        }
        throw new IllegalStateException(
                "Timed out waiting for RabbitMQ restart.",
                lastFailure
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
        factory.setConnectionTimeout(1_000);
        factory.setHandshakeTimeout(1_000);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        return factory;
    }

    private static Throwable merge(
            Throwable existing,
            Throwable addition
    ) {
        if (existing == null) {
            return addition;
        }
        existing.addSuppressed(addition);
        return existing;
    }

    @FunctionalInterface
    private interface CheckedBoolean {
        boolean getAsBoolean() throws Exception;
    }

    private static final class EventLog implements AutoCloseable {
        private static final Gson GSON = new Gson();
        private final BufferedWriter writer;

        private EventLog(Path path) throws IOException {
            writer = Files.newBufferedWriter(
                    path,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        }

        private synchronized void record(
                String event,
                Map<String, ?> fields
        ) {
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("time", Instant.now().toString());
            line.put("event", event);
            line.putAll(fields);
            try {
                writer.write(GSON.toJson(line));
                writer.newLine();
                writer.flush();
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "Could not write chaos event log.",
                        failure
                );
            }
        }

        @Override
        public synchronized void close() throws IOException {
            writer.close();
        }
    }
}
