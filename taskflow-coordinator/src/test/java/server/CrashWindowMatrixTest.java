package server;

import com.github.dockerjava.api.model.ExposedPort;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import objectstore.ObjectListing;
import objectstore.ObjectMetadata;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.ObjectStores;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.lifecycle.Startables;
import protocol.JobResultMessage;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.objectstore.OrphanOutputGc;
import server.objectstore.OrphanOutputGcConfig;
import server.rabbitmq.RabbitMqOutboxReplayer;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.runtime.TaskFlowClock;
import server.scheduler.SchedulerConfig;
import server.scheduler.TaskScheduler;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CrashWindowMatrixTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";
    private static final DockerImageName RABBITMQ_IMAGE =
            DockerImageName.parse("rabbitmq:3.13-management");
    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");
    private static final String MINIO_IMAGE =
            "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    private static final int TOXIPROXY_AMQP_PORT = 8_666;
    private static final long PROCESS_SIGNAL_TIMEOUT_SECONDS = 30L;
    private static final long DELIVERY_TIMEOUT_SECONDS = 15L;
    private static final String[] MINIO_PROPERTIES = {
            "taskflow.minioEndpoint",
            "taskflow.minioAccessKey",
            "taskflow.minioSecretKey",
            "taskflow.minioBucket"
    };

    @TempDir
    Path tempDir;

    private final Map<String, String> originalMinioProperties =
            new LinkedHashMap<>();
    private Network network;
    private RabbitMQContainer rabbit;
    private ToxiproxyContainer toxiproxy;
    private MinIOContainer minio;
    private String minioEndpoint;
    private String minioBucket;

    @BeforeAll
    void startManagedInfrastructure() throws Exception {
        if (!liveTestEnabled()) {
            return;
        }
        network = Network.newNetwork();
        rabbit = new RabbitMQContainer(RABBITMQ_IMAGE)
                .withAdminUser("taskflow")
                .withAdminPassword("taskflow-crash")
                .withNetwork(network)
                .withNetworkAliases("rabbitmq");
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network);
        minio = new MinIOContainer(MINIO_IMAGE);
        Startables.deepStart(Stream.of(rabbit, toxiproxy, minio)).join();

        new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort())
                .createProxy(
                        "crash-window-rabbitmq",
                        "0.0.0.0:" + TOXIPROXY_AMQP_PORT,
                        "rabbitmq:5672"
                );
        minioEndpoint = mappedMinioEndpoint();
        minioBucket = "taskflow-crash-"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        try (MinioClient client = minioClient()) {
            client.makeBucket(
                    MakeBucketArgs.builder().bucket(minioBucket).build()
            );
        }
        configureParentObjectStore();
    }

    @BeforeEach
    void requireManagedInfrastructure() {
        Assumptions.assumeTrue(
                liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV
                        + "=true to run the process crash-window matrix."
        );
    }

    @AfterAll
    void stopManagedInfrastructure() {
        restoreParentObjectStoreProperties();
        closeQuietly(minio);
        closeQuietly(toxiproxy);
        closeQuietly(rabbit);
        closeQuietly(network);
    }

    @Test
    void acceptedJobSurvivesLostAcceptanceResponse() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint.AFTER_JOB_TRANSACTION_COMMIT
        );
        killAtFailpoint(context);

        try (DatabaseManager database =
                     new DatabaseManager(context.database().toString())) {
            assertEquals(1, database.getJobHistory().size());
            assertEquals("RUNNING", database.getJobHistory().getFirst().status());
            assertEquals(1, database.getTasksForJob(context.jobId()).size());

            JobStateStore.JobSubmissionDecision replay =
                    database.commitJobSubmission(
                            context.jobId(),
                            CrashWindowProcessMain.TASK_TYPE,
                            context.requesterId(),
                            RequesterTokens.hashToken(context.requesterToken()),
                            "",
                            context.requestHash(),
                            "",
                            List.of(new JobStateStore.TaskStartupState(
                                    context.taskId(),
                                    "alpha"
                            ))
                    );
            assertEquals(
                    JobStateStore.JobSubmissionOutcome.REPLAY,
                    replay.outcome()
            );
            assertEquals(1, database.getJobHistory().size());
            assertEquals(1, database.getTasksForJob(context.jobId()).size());
        }
    }

    @Test
    void assignmentCommitBeforePublishReplaysExactIdentity() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint.AFTER_ASSIGNMENT_OUTBOX_COMMIT
        );
        killAtFailpoint(context);

        BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> deliveryFailure = new AtomicReference<>();
        try (RabbitMqTransport peer = new RabbitMqTransport(context.rabbitConfig());
             RabbitMqTransport coordinator =
                     new RabbitMqTransport(context.rabbitConfig());
             DatabaseManager database =
                     new DatabaseManager(context.database().toString())) {
            peer.declareTopology();
            subscribePeer(
                    peer,
                    TransportRoute.TASK_ASSIGN,
                    context.peerId(),
                    TaskAssignMessage.class,
                    assignments,
                    deliveryFailure
            );

            BrokerOutboxStore.OutboxRecord pending =
                    onlyPending(database);
            TaskAssignMessage durable = assertInstanceOf(
                    TaskAssignMessage.class,
                    pending.message().message()
            );
            assertEquals(context.assignmentId(), durable.getAssignmentId());
            assertEquals(1, durable.getAttemptNumber());

            coordinator.declareTopology();
            try (RabbitMqOutboxReplayer replayer =
                         new RabbitMqOutboxReplayer(
                                 database,
                                 new RabbitMqSchedulerOutput(coordinator),
                                 10
                         )) {
                replayer.start();
            }
            TaskAssignMessage replayed = awaitMessage(
                    assignments,
                    deliveryFailure,
                    "replayed committed assignment"
            );
            assertAssignmentEquals(durable, replayed);
            assertTrue(database.loadPendingBrokerOutbox(10).isEmpty());
            assertEquals(1, database.loadTaskAttempts(context.jobId()).size());
        }
    }

    @Test
    void publishedAssignmentRemainsReplayableUntilMarked() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint
                        .AFTER_ASSIGNMENT_BROKER_CONFIRM_BEFORE_MARK
        );
        BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> deliveryFailure = new AtomicReference<>();
        try (RabbitMqTransport peer = new RabbitMqTransport(context.rabbitConfig())) {
            peer.declareTopology();
            subscribePeer(
                    peer,
                    TransportRoute.TASK_ASSIGN,
                    context.peerId(),
                    TaskAssignMessage.class,
                    assignments,
                    deliveryFailure
            );
            killAtFailpoint(context);

            TaskAssignMessage confirmed = awaitMessage(
                    assignments,
                    deliveryFailure,
                    "confirmed assignment before child kill"
            );
            try (DatabaseManager database =
                         new DatabaseManager(context.database().toString());
                 RabbitMqTransport coordinator =
                         new RabbitMqTransport(context.rabbitConfig())) {
                BrokerOutboxStore.OutboxRecord pending = onlyPending(database);
                assertEquals(0, pending.attemptCount());
                assertEquals(
                        context.assignmentId(),
                        assertInstanceOf(
                                TaskAssignMessage.class,
                                pending.message().message()
                        ).getAssignmentId()
                );

                coordinator.declareTopology();
                try (RabbitMqOutboxReplayer replayer =
                             new RabbitMqOutboxReplayer(
                                     database,
                                     new RabbitMqSchedulerOutput(coordinator),
                                     10
                             )) {
                    replayer.start();
                }
                TaskAssignMessage replayed = awaitMessage(
                        assignments,
                        deliveryFailure,
                        "duplicate assignment replay"
                );
                assertAssignmentEquals(confirmed, replayed);
                assertTrue(database.loadPendingBrokerOutbox(10).isEmpty());
                assertEquals(
                        1,
                        database.loadTaskAttempts(context.jobId()).size()
                );
            }
        }
    }

    @Test
    void lostResultPublishConfirmCannotDoubleCommit() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint.AFTER_RESULT_PUBLISH_CONFIRM
        );
        BlockingQueue<TaskResultMessage> results = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> deliveryFailure = new AtomicReference<>();
        try (RabbitMqTransport coordinator =
                     new RabbitMqTransport(context.rabbitConfig())) {
            coordinator.declareTopology();
            subscribe(
                    coordinator,
                    TransportRoute.TASK_RESULT,
                    TaskResultMessage.class,
                    results,
                    deliveryFailure
            );
            killAtFailpoint(context);

            TaskResultMessage first = awaitMessage(
                    results,
                    deliveryFailure,
                    "task result confirmed before child kill"
            );
            try (DatabaseManager database =
                         new DatabaseManager(context.database().toString())) {
                assertEquals(
                        JobStateStore.ResultCommitOutcome.COMMITTED,
                        commit(database, first)
                );
                assertTrue(coordinator.publish(new OutboundTransportMessage(
                        TransportRoute.TASK_RESULT,
                        context.peerId(),
                        first
                )));
                TaskResultMessage replayed = awaitMessage(
                        results,
                        deliveryFailure,
                        "replayed uncertain task result"
                );
                assertEquals(first.getAssignmentId(), replayed.getAssignmentId());
                assertEquals(
                        JobStateStore.ResultCommitOutcome
                                .DUPLICATE_ALREADY_COMPLETED,
                        commit(database, replayed)
                );
                assertEquals(
                        1L,
                        database.loadTaskAttempts(context.jobId()).stream()
                                .filter(attempt -> attempt.outcome()
                                        == JobStateStore.TaskAttemptOutcome.SUCCEEDED)
                                .count()
                );
            }
        }
    }

    @Test
    void resultCommitBeforeFinalizationRecoversTerminalResult()
            throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint.AFTER_RESULT_COMMIT
        );
        killAtFailpoint(context);

        BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> deliveryFailure = new AtomicReference<>();
        try (DatabaseManager database =
                     new DatabaseManager(context.database().toString());
             RabbitMqTransport requester =
                     new RabbitMqTransport(context.rabbitConfig());
             RabbitMqTransport coordinator =
                     new RabbitMqTransport(context.rabbitConfig())) {
            assertEquals("FINALIZING", jobStatus(database, context.jobId()));
            assertEquals(
                    "COMPLETED",
                    database.getTasksForJob(context.jobId()).getFirst().status()
            );
            assertTrue(database.loadPendingBrokerOutbox(10).isEmpty());

            requester.declareTopology();
            subscribePeer(
                    requester,
                    TransportRoute.JOB_RESULT,
                    context.requesterId(),
                    JobResultMessage.class,
                    results,
                    deliveryFailure
            );
            coordinator.declareTopology();
            CoordinatorStartupRecovery.RecoveryResult recovery =
                    CoordinatorStartupRecovery.recoverPersistedJobs(
                            database,
                            new FixedClock(
                                    CrashWindowProcessMain.STARTED_AT + 200L
                            ),
                            () -> UUID.nameUUIDFromBytes(
                                    "recovery-assignment"
                                            .getBytes(StandardCharsets.UTF_8)
                            ).toString()
                    );
            assertEquals(1, recovery.resumedJobs().size());
            TaskScheduler scheduler = new TaskScheduler(
                    new LinkedBlockingQueue<MessageEnvelope>(),
                    new InMemoryPeerRegistry(),
                    database,
                    new RabbitMqSchedulerOutput(coordinator),
                    SchedulerConfig.defaults(),
                    new FixedClock(
                            CrashWindowProcessMain.STARTED_AT + 200L
                    ),
                    () -> UUID.nameUUIDFromBytes(
                            "recovery-assignment"
                                    .getBytes(StandardCharsets.UTF_8)
                    ).toString()
            );
            scheduler.restoreJobs(
                    recovery.resumedJobs(),
                    recovery.requesterTokenHashes(),
                    recovery.requesterIdentityKeys()
            );

            JobResultMessage terminal = awaitMessage(
                    results,
                    deliveryFailure,
                    "recovered terminal job result"
            );
            assertEquals(context.jobId(), terminal.getJobId());
            assertEquals(
                    List.of(CrashWindowProcessMain.RESULT_PAYLOAD),
                    terminal.getResultsByTaskId()
            );
            assertEquals("COMPLETED", jobStatus(database, context.jobId()));
            assertTrue(database.loadPendingBrokerOutbox(10).isEmpty());
            assertTrue(
                    CoordinatorStartupRecovery.recoverPersistedJobs(
                            database,
                            new FixedClock(
                                    CrashWindowProcessMain.STARTED_AT + 300L
                            ),
                            () -> context.assignmentId()
                    ).resumedJobs().isEmpty()
            );
        }
    }

    @Test
    void terminalResultCommitBeforePublishReplays() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint.AFTER_TERMINAL_OUTBOX_COMMIT
        );
        killAtFailpoint(context);

        BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> deliveryFailure = new AtomicReference<>();
        try (DatabaseManager database =
                     new DatabaseManager(context.database().toString());
             RabbitMqTransport requester =
                     new RabbitMqTransport(context.rabbitConfig());
             RabbitMqTransport coordinator =
                     new RabbitMqTransport(context.rabbitConfig())) {
            assertEquals("COMPLETED", jobStatus(database, context.jobId()));
            BrokerOutboxStore.OutboxRecord pending = onlyPending(database);
            JobResultMessage durable = assertInstanceOf(
                    JobResultMessage.class,
                    pending.message().message()
            );

            requester.declareTopology();
            subscribePeer(
                    requester,
                    TransportRoute.JOB_RESULT,
                    context.requesterId(),
                    JobResultMessage.class,
                    results,
                    deliveryFailure
            );
            coordinator.declareTopology();
            try (RabbitMqOutboxReplayer replayer =
                         new RabbitMqOutboxReplayer(
                                 database,
                                 new RabbitMqSchedulerOutput(coordinator),
                                 10
                         )) {
                replayer.start();
            }
            JobResultMessage replayed = awaitMessage(
                    results,
                    deliveryFailure,
                    "terminal result replay"
            );
            assertJobResultEquals(durable, replayed);
            assertTrue(database.loadPendingBrokerOutbox(10).isEmpty());
        }
    }

    @Test
    void publishedFinalResultRemainsReplayableUntilMarked() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint
                        .AFTER_FINAL_RESULT_CONFIRM_BEFORE_MARK
        );
        BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> deliveryFailure = new AtomicReference<>();
        try (RabbitMqTransport requester =
                     new RabbitMqTransport(context.rabbitConfig())) {
            requester.declareTopology();
            subscribePeer(
                    requester,
                    TransportRoute.JOB_RESULT,
                    context.requesterId(),
                    JobResultMessage.class,
                    results,
                    deliveryFailure
            );
            killAtFailpoint(context);

            JobResultMessage confirmed = awaitMessage(
                    results,
                    deliveryFailure,
                    "confirmed final result before child kill"
            );
            try (DatabaseManager database =
                         new DatabaseManager(context.database().toString());
                 RabbitMqTransport coordinator =
                         new RabbitMqTransport(context.rabbitConfig())) {
                assertEquals("COMPLETED", jobStatus(database, context.jobId()));
                BrokerOutboxStore.OutboxRecord pending = onlyPending(database);
                assertEquals(0, pending.attemptCount());

                coordinator.declareTopology();
                try (RabbitMqOutboxReplayer replayer =
                             new RabbitMqOutboxReplayer(
                                     database,
                                     new RabbitMqSchedulerOutput(coordinator),
                                     10
                             )) {
                    replayer.start();
                }
                JobResultMessage replayed = awaitMessage(
                        results,
                        deliveryFailure,
                        "duplicate final-result replay"
                );
                assertJobResultEquals(confirmed, replayed);
                assertTrue(database.loadPendingBrokerOutbox(10).isEmpty());
            }
        }
    }

    @Test
    void partialObjectUploadCannotBecomeAuthoritative() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint.DURING_OBJECT_UPLOAD
        );
        killAtFailpoint(context);

        try (DatabaseManager database =
                     new DatabaseManager(context.database().toString());
             ObjectStore objectStore = ObjectStores.open()) {
            DatabaseManager.TaskRecord task =
                    database.getTasksForJob(context.jobId()).getFirst();
            assertEquals("ASSIGNED", task.status());
            assertEquals(context.assignmentId(), task.assignmentId());
            assertFalse(taskResultPresent(database, context.taskId()));

            ObjectStoreException missing = assertThrows(
                    ObjectStoreException.class,
                    () -> objectStore.stat(context.outputKey())
            );
            assertEquals(ObjectStoreException.Reason.NOT_FOUND, missing.reason());
            assertTrue(
                    objectStore.list(
                            TaskFlowObjectKeys.prefix("jobs"),
                            null,
                            20
                    ).objects().stream()
                            .noneMatch(metadata ->
                                    context.outputKey().equals(metadata.key()))
            );
        }
    }

    @Test
    void uploadedOutputBeforeResultIsEventuallyCollected() throws Exception {
        Context context = context(
                CrashWindowProcessMain.Failpoint
                        .AFTER_OBJECT_UPLOAD_BEFORE_RESULT
        );
        killAtFailpoint(context);

        CountDownLatch deleted = new CountDownLatch(1);
        try (DatabaseManager database =
                     new DatabaseManager(context.database().toString())) {
            ObjectStore delegate = ObjectStores.open();
            assertEquals(context.outputKey(), delegate.stat(context.outputKey()).key());
            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    database.commitAssignedTaskFailure(
                            context.taskId(),
                            1,
                            context.assignmentId(),
                            context.peerId(),
                            1,
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            "executor_process_killed",
                            System.currentTimeMillis()
                    )
            );

            try (OrphanOutputGc gc = new OrphanOutputGc(
                    new ObservingObjectStore(delegate, context.outputKey(), deleted),
                    database,
                    new OrphanOutputGcConfig(true, 1L, 60_000L, 10),
                    new FixedClock(System.currentTimeMillis() + 60_000L)
            )) {
                gc.start();
                assertTrue(
                        deleted.await(DELIVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                        "Timed out waiting for bounded orphan-output deletion"
                );
            }

            try (ObjectStore verifier = ObjectStores.open()) {
                ObjectStoreException missing = assertThrows(
                        ObjectStoreException.class,
                        () -> verifier.stat(context.outputKey())
                );
                assertEquals(
                        ObjectStoreException.Reason.NOT_FOUND,
                        missing.reason()
                );
            }
            DatabaseManager.TaskRecord task =
                    database.getTasksForJob(context.jobId()).getFirst();
            assertEquals("PENDING", task.status());
            assertFalse(taskResultPresent(database, context.taskId()));
        }
    }

    private Context context(CrashWindowProcessMain.Failpoint failpoint)
            throws Exception {
        String token = UUID.randomUUID().toString()
                .replace("-", "")
                .substring(0, 16);
        String jobId = "job-crash-" + token;
        String assignmentId = UUID.nameUUIDFromBytes(
                ("assignment-" + token).getBytes(StandardCharsets.UTF_8)
        ).toString();
        Path directory = tempDir.resolve(failpoint.name().toLowerCase());
        Files.createDirectories(directory);
        return new Context(
                failpoint,
                directory.resolve("taskflow.db"),
                directory.resolve("failpoint.signal"),
                directory.resolve("child.log"),
                jobId,
                assignmentId,
                token
        );
    }

    private void killAtFailpoint(Context context) throws Exception {
        Files.deleteIfExists(context.signal());
        List<String> command = new ArrayList<>();
        command.add(javaExecutable());
        command.add("-cp");
        command.add(testClasspath());
        command.add(CrashWindowProcessMain.class.getName());
        command.add(context.failpoint().name());
        command.add(context.database().toAbsolutePath().toString());
        command.add(context.signal().toAbsolutePath().toString());
        command.add(context.jobId());
        command.add(context.assignmentId());
        command.add(toxiproxy.getHost());
        command.add(String.valueOf(
                toxiproxy.getMappedPort(TOXIPROXY_AMQP_PORT)
        ));
        command.add(rabbit.getAdminUsername());
        command.add(rabbit.getAdminPassword());
        command.add(context.token());
        command.add(minioEndpoint);
        command.add(minio.getUserName());
        command.add(minio.getPassword());
        command.add(minioBucket);

        ProcessBuilder builder = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(context.log().toFile());
        Process process;
        try (WatchService watchService =
                     FileSystems.getDefault().newWatchService()) {
            context.signal().getParent().register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY
            );
            process = builder.start();
            awaitFailpointSignal(context, process, watchService);
        }

        process.destroyForcibly();
        assertTrue(
                process.waitFor(10L, TimeUnit.SECONDS),
                "Child JVM did not terminate after destroyForcibly; pid="
                        + process.pid()
        );
        assertFalse(process.isAlive());
        assertTrue(
                process.exitValue() != 0,
                "Crash victim exited successfully instead of being killed"
        );
    }

    private static void awaitFailpointSignal(
            Context context,
            Process process,
            WatchService watchService
    ) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(PROCESS_SIGNAL_TIMEOUT_SECONDS);
        while (true) {
            if (Files.exists(context.signal())) {
                assertEquals(
                        context.failpoint().name(),
                        Files.readString(context.signal(), StandardCharsets.UTF_8)
                );
                return;
            }
            if (!process.isAlive()) {
                fail(
                        "Crash victim exited before failpoint "
                                + context.failpoint()
                                + "; exit=" + process.exitValue()
                                + System.lineSeparator()
                                + childLog(context)
                );
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                process.destroyForcibly();
                fail(
                        "Timed out waiting for failpoint "
                                + context.failpoint()
                                + "; pid=" + process.pid()
                                + System.lineSeparator()
                                + childLog(context)
                );
            }
            WatchKey key = watchService.poll(remaining, TimeUnit.NANOSECONDS);
            if (key == null) {
                continue;
            }
            key.pollEvents();
            if (!key.reset()) {
                fail("Failpoint signal directory became unavailable");
            }
        }
    }

    private static String childLog(Context context) {
        try {
            return Files.exists(context.log())
                    ? Files.readString(context.log(), StandardCharsets.UTF_8)
                    : "<no child log>";
        } catch (Exception e) {
            return "<child log unreadable: " + e.getMessage() + ">";
        }
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name", "")
                .toLowerCase()
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

    private static <T> void subscribePeer(
            RabbitMqTransport transport,
            TransportRoute route,
            String peerId,
            Class<T> type,
            BlockingQueue<T> messages,
            AtomicReference<Throwable> failure
    ) throws Exception {
        transport.subscribePeer(
                route,
                peerId,
                delivery -> capture(type, messages, failure, delivery)
        );
    }

    private static <T> void subscribe(
            RabbitMqTransport transport,
            TransportRoute route,
            Class<T> type,
            BlockingQueue<T> messages,
            AtomicReference<Throwable> failure
    ) throws Exception {
        transport.subscribe(
                route,
                delivery -> capture(type, messages, failure, delivery)
        );
    }

    private static <T> void capture(
            Class<T> type,
            BlockingQueue<T> messages,
            AtomicReference<Throwable> failure,
            InboundTransportMessage delivery
    ) {
        try {
            messages.add(type.cast(delivery.message()));
        } catch (Throwable throwable) {
            failure.compareAndSet(null, throwable);
        }
    }

    private static <T> T awaitMessage(
            BlockingQueue<T> messages,
            AtomicReference<Throwable> failure,
            String description
    ) throws Exception {
        T message = messages.poll(
                DELIVERY_TIMEOUT_SECONDS,
                TimeUnit.SECONDS
        );
        Throwable deliveryFailure = failure.get();
        if (deliveryFailure != null) {
            fail(deliveryFailure);
        }
        assertNotNull(message, "Timed out waiting for " + description);
        return message;
    }

    private static JobStateStore.ResultCommitOutcome commit(
            DatabaseManager database,
            TaskResultMessage result
    ) {
        return database.commitTaskResult(
                result.getTaskId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                result.getNodeId(),
                CrashWindowProcessMain.STARTED_AT + 100L,
                100L,
                result.getResultPayload()
        );
    }

    private static BrokerOutboxStore.OutboxRecord onlyPending(
            DatabaseManager database
    ) {
        List<BrokerOutboxStore.OutboxRecord> pending =
                database.loadPendingBrokerOutbox(10);
        assertEquals(1, pending.size());
        return pending.getFirst();
    }

    private static String jobStatus(
            DatabaseManager database,
            String jobId
    ) {
        return database.getJobHistory().stream()
                .filter(job -> jobId.equals(job.jobId()))
                .findFirst()
                .orElseThrow()
                .status();
    }

    private static boolean taskResultPresent(
            DatabaseManager database,
            String taskId
    ) {
        return database.loadRunningJobsForResume().stream()
                .flatMap(job -> job.tasks().stream())
                .filter(task -> taskId.equals(task.taskId()))
                .findFirst()
                .orElseThrow()
                .resultPayloadPresent();
    }

    private static void assertAssignmentEquals(
            TaskAssignMessage expected,
            TaskAssignMessage actual
    ) {
        assertEquals(expected.getJobId(), actual.getJobId());
        assertEquals(expected.getTaskId(), actual.getTaskId());
        assertEquals(expected.getAttemptNumber(), actual.getAttemptNumber());
        assertEquals(expected.getAssignmentId(), actual.getAssignmentId());
        assertEquals(
                expected.getLeaseExpiresAtEpochMillis(),
                actual.getLeaseExpiresAtEpochMillis()
        );
    }

    private static void assertJobResultEquals(
            JobResultMessage expected,
            JobResultMessage actual
    ) {
        assertEquals(expected.getJobId(), actual.getJobId());
        assertEquals(expected.isSuccessful(), actual.isSuccessful());
        assertEquals(expected.getResultPayload(), actual.getResultPayload());
        assertEquals(expected.getResultsByTaskId(), actual.getResultsByTaskId());
    }

    private RabbitMqTransportConfig rabbitConfig(String token) {
        String name = "taskflow.crash." + token;
        return new RabbitMqTransportConfig(
                toxiproxy.getHost(),
                toxiproxy.getMappedPort(TOXIPROXY_AMQP_PORT),
                rabbit.getAdminUsername(),
                rabbit.getAdminPassword(),
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

    private String mappedMinioEndpoint() {
        var bindings = minio.getDockerClient()
                .inspectContainerCmd(minio.getContainerId())
                .exec()
                .getNetworkSettings()
                .getPorts()
                .getBindings()
                .get(ExposedPort.tcp(9000));
        if (bindings == null || bindings.length == 0) {
            throw new IllegalStateException("MinIO S3 port is not mapped");
        }
        return "http://" + minio.getHost() + ":"
                + bindings[0].getHostPortSpec();
    }

    private MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minio.getUserName(), minio.getPassword())
                .build();
    }

    private void configureParentObjectStore() {
        for (String property : MINIO_PROPERTIES) {
            originalMinioProperties.put(property, System.getProperty(property));
        }
        System.setProperty("taskflow.minioEndpoint", minioEndpoint);
        System.setProperty("taskflow.minioAccessKey", minio.getUserName());
        System.setProperty("taskflow.minioSecretKey", minio.getPassword());
        System.setProperty("taskflow.minioBucket", minioBucket);
    }

    private void restoreParentObjectStoreProperties() {
        for (String property : MINIO_PROPERTIES) {
            String original = originalMinioProperties.get(property);
            if (original == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, original);
            }
        }
    }

    private static boolean liveTestEnabled() {
        String property = System.getProperty(LIVE_TEST_PROPERTY);
        if (property != null && !property.isBlank()) {
            return Boolean.parseBoolean(property);
        }
        return Boolean.parseBoolean(System.getenv(LIVE_TEST_ENV));
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Best-effort managed-infrastructure cleanup after assertions.
        }
    }

    private record FixedClock(long epochMillis) implements TaskFlowClock {
        @Override
        public Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public long nowEpochMillis() {
            return epochMillis;
        }
    }

    private final class Context {
        private final CrashWindowProcessMain.Failpoint failpoint;
        private final Path database;
        private final Path signal;
        private final Path log;
        private final String jobId;
        private final String assignmentId;
        private final String token;

        private Context(
                CrashWindowProcessMain.Failpoint failpoint,
                Path database,
                Path signal,
                Path log,
                String jobId,
                String assignmentId,
                String token
        ) {
            this.failpoint = failpoint;
            this.database = database;
            this.signal = signal;
            this.log = log;
            this.jobId = jobId;
            this.assignmentId = assignmentId;
            this.token = token;
        }

        private CrashWindowProcessMain.Failpoint failpoint() {
            return failpoint;
        }

        private Path database() {
            return database;
        }

        private Path signal() {
            return signal;
        }

        private Path log() {
            return log;
        }

        private String jobId() {
            return jobId;
        }

        private String assignmentId() {
            return assignmentId;
        }

        private String token() {
            return token;
        }

        private String taskId() {
            return "task-" + jobId + "-0";
        }

        private String requesterId() {
            return "requester-" + jobId;
        }

        private String peerId() {
            return "peer-" + jobId;
        }

        private String requesterToken() {
            return "token-" + jobId;
        }

        private String requestHash() {
            return "request-hash-" + jobId;
        }

        private String outputKey() {
            return TaskFlowObjectKeys.attemptOutputKey(
                    jobId,
                    taskId(),
                    1,
                    assignmentId
            );
        }

        private RabbitMqTransportConfig rabbitConfig() {
            return CrashWindowMatrixTest.this.rabbitConfig(token);
        }
    }

    private static final class ObservingObjectStore implements ObjectStore {
        private final ObjectStore delegate;
        private final String observedKey;
        private final CountDownLatch deleted;

        private ObservingObjectStore(
                ObjectStore delegate,
                String observedKey,
                CountDownLatch deleted
        ) {
            this.delegate = delegate;
            this.observedKey = observedKey;
            this.deleted = deleted;
        }

        @Override
        public ObjectReference put(
                ObjectReference reference,
                InputStream content
        ) throws ObjectStoreException {
            return delegate.put(reference, content);
        }

        @Override
        public ObjectReference putIfAbsent(
                ObjectReference reference,
                InputStream content
        ) throws ObjectStoreException {
            return delegate.putIfAbsent(reference, content);
        }

        @Override
        public InputStream get(String key) throws ObjectStoreException {
            return delegate.get(key);
        }

        @Override
        public ObjectReference stat(String key) throws ObjectStoreException {
            return delegate.stat(key);
        }

        @Override
        public void delete(String key) throws ObjectStoreException {
            delegate.delete(key);
            if (observedKey.equals(key)) {
                deleted.countDown();
            }
        }

        @Override
        public ObjectReference copy(
                String sourceKey,
                String destinationKey
        ) throws ObjectStoreException {
            return delegate.copy(sourceKey, destinationKey);
        }

        @Override
        public ObjectListing list(
                String prefix,
                String startAfter,
                int limit
        ) throws ObjectStoreException {
            return delegate.list(prefix, startAfter, limit);
        }

        @Override
        public void close() throws ObjectStoreException {
            delegate.close();
        }
    }
}
