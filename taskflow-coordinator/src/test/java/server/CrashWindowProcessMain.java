package server;

import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStores;
import objectstore.TaskFlowObjectKeys;
import protocol.JobResultMessage;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.rabbitmq.RabbitMqSchedulerOutput;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Child-JVM crash victim for {@link CrashWindowMatrixTest}.
 *
 * <p>Each action performs one real durable or broker/object-store boundary,
 * writes the named failpoint signal, and then waits until the parent kills the
 * operating-system process. The parent owns all post-crash assertions.</p>
 */
public final class CrashWindowProcessMain {
    static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    static final String RESULT_PAYLOAD = "crash-window-result";
    static final long STARTED_AT = 1_000L;
    static final long LEASE_EXPIRES_AT = 4_102_444_800_000L;
    private static final int PARTIAL_UPLOAD_BYTES = 6 * 1024 * 1024;
    private static final int FIRST_UPLOAD_CHUNK_BYTES = 64 * 1024;

    private CrashWindowProcessMain() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        switch (arguments.failpoint()) {
            case AFTER_JOB_TRANSACTION_COMMIT -> afterJobCommit(arguments);
            case AFTER_ASSIGNMENT_OUTBOX_COMMIT -> afterAssignmentCommit(arguments);
            case AFTER_ASSIGNMENT_BROKER_CONFIRM_BEFORE_MARK ->
                    afterAssignmentConfirm(arguments);
            case AFTER_RESULT_PUBLISH_CONFIRM -> afterResultPublishConfirm(arguments);
            case AFTER_RESULT_COMMIT -> afterResultCommit(arguments);
            case AFTER_TERMINAL_OUTBOX_COMMIT -> afterTerminalCommit(arguments);
            case AFTER_FINAL_RESULT_CONFIRM_BEFORE_MARK ->
                    afterFinalResultConfirm(arguments);
            case DURING_OBJECT_UPLOAD -> duringObjectUpload(arguments);
            case AFTER_OBJECT_UPLOAD_BEFORE_RESULT -> afterObjectUpload(arguments);
        }
        throw new AssertionError("Crash-window child returned past its failpoint");
    }

    private static void afterJobCommit(Arguments arguments) throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath())) {
            require(
                    database.commitJobSubmission(
                            arguments.jobId(),
                            TASK_TYPE,
                            arguments.requesterId(),
                            RequesterTokens.hashToken(arguments.requesterToken()),
                            "",
                            arguments.requestHash(),
                            "",
                            List.of(new JobStateStore.TaskStartupState(
                                    arguments.taskId(),
                                    "alpha"
                            ))
                    ).outcome() == JobStateStore.JobSubmissionOutcome.COMMITTED,
                    "job transaction did not commit"
            );
            signalAndBlock(arguments);
        }
    }

    private static void afterAssignmentCommit(Arguments arguments) throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath())) {
            BrokerOutboxStore.CommittedTaskAssignment assignment =
                    seedAssignment(database, arguments);
            require(
                    assignment.outboxRecord().message().route()
                            == TransportRoute.TASK_ASSIGN,
                    "assignment outbox route was not durable"
            );
            signalAndBlock(arguments);
        }
    }

    private static void afterAssignmentConfirm(Arguments arguments) throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath());
             RabbitMqTransport transport = new RabbitMqTransport(arguments.rabbitConfig())) {
            BrokerOutboxStore.CommittedTaskAssignment assignment =
                    seedAssignment(database, arguments);
            transport.declareTopology();
            require(
                    new RabbitMqSchedulerOutput(transport)
                            .publishOutbox(assignment.outboxRecord()),
                    "assignment publication was not confirmed and routed"
            );
            require(
                    database.loadPendingBrokerOutbox(10).size() == 1,
                    "child marked assignment outbox before failpoint"
            );
            signalAndBlock(arguments);
        }
    }

    private static void afterResultPublishConfirm(Arguments arguments)
            throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath());
             RabbitMqTransport transport = new RabbitMqTransport(arguments.rabbitConfig())) {
            BrokerOutboxStore.CommittedTaskAssignment assignment =
                    seedAssignment(database, arguments);
            require(
                    database.markBrokerOutboxPublished(
                            assignment.outboxRecord().outboxId(),
                            STARTED_AT + 1L
                    ),
                    "assignment outbox could not be marked sent"
            );
            transport.declareTopology();
            TaskResultMessage result = taskResult(arguments);
            require(
                    transport.publish(new OutboundTransportMessage(
                            TransportRoute.TASK_RESULT,
                            arguments.peerId(),
                            result
                    )),
                    "task-result publication was not confirmed"
            );
            signalAndBlock(arguments);
        }
    }

    private static void afterResultCommit(Arguments arguments) throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath())) {
            BrokerOutboxStore.CommittedTaskAssignment assignment =
                    seedAssignment(database, arguments);
            require(
                    database.markBrokerOutboxPublished(
                            assignment.outboxRecord().outboxId(),
                            STARTED_AT + 1L
                    ),
                    "assignment outbox could not be marked sent"
            );
            require(
                    database.commitTaskResult(
                            arguments.taskId(),
                            assignment.identity().attemptNumber(),
                            assignment.identity().assignmentId(),
                            arguments.peerId(),
                            STARTED_AT + 100L,
                            100L,
                            RESULT_PAYLOAD
                    ) == JobStateStore.ResultCommitOutcome.COMMITTED,
                    "authoritative result did not commit"
            );
            signalAndBlock(arguments);
        }
    }

    private static void afterTerminalCommit(Arguments arguments) throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath())) {
            seedFinalizingResult(database, arguments);
            BrokerOutboxStore.OutboxCommit terminal =
                    database.commitJobCompletedAndEnqueueBrokerOutbox(
                            arguments.jobId(),
                            List.of(RESULT_PAYLOAD),
                            finalResultOutbox(arguments)
                    );
            require(
                    terminal.outcome()
                            == JobStateStore.DurableTransitionOutcome.COMMITTED,
                    "terminal job/final-outbox transaction did not commit"
            );
            signalAndBlock(arguments);
        }
    }

    private static void afterFinalResultConfirm(Arguments arguments)
            throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath());
             RabbitMqTransport transport = new RabbitMqTransport(arguments.rabbitConfig())) {
            seedFinalizingResult(database, arguments);
            BrokerOutboxStore.OutboxCommit terminal =
                    database.commitJobCompletedAndEnqueueBrokerOutbox(
                            arguments.jobId(),
                            List.of(RESULT_PAYLOAD),
                            finalResultOutbox(arguments)
                    );
            require(
                    terminal.outcome()
                            == JobStateStore.DurableTransitionOutcome.COMMITTED,
                    "terminal job/final-outbox transaction did not commit"
            );
            transport.declareTopology();
            require(
                    new RabbitMqSchedulerOutput(transport)
                            .publishOutbox(terminal.outboxRecord()),
                    "final-result publication was not confirmed and routed"
            );
            require(
                    database.loadPendingBrokerOutbox(10).size() == 1,
                    "child marked final-result outbox before failpoint"
            );
            signalAndBlock(arguments);
        }
    }

    private static void duringObjectUpload(Arguments arguments) throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath());
             ObjectStore objectStore = openObjectStore(arguments)) {
            seedAssignment(database, arguments);
            byte[] content = new byte[PARTIAL_UPLOAD_BYTES];
            Arrays.fill(content, (byte) 0x5a);
            ObjectReference reference = new ObjectReference(
                    arguments.outputKey(),
                    content.length,
                    sha256(content),
                    "application/octet-stream"
            );
            objectStore.putIfAbsent(
                    reference,
                    new FailpointInputStream(
                            content,
                            FIRST_UPLOAD_CHUNK_BYTES,
                            arguments
                    )
            );
        }
    }

    private static void afterObjectUpload(Arguments arguments) throws Exception {
        try (DatabaseManager database = new DatabaseManager(arguments.databasePath());
             ObjectStore objectStore = openObjectStore(arguments)) {
            seedAssignment(database, arguments);
            byte[] content = "orphan-attempt-output"
                    .getBytes(StandardCharsets.UTF_8);
            ObjectReference reference = new ObjectReference(
                    arguments.outputKey(),
                    content.length,
                    sha256(content),
                    "application/octet-stream"
            );
            require(
                    reference.equals(objectStore.putIfAbsent(
                            reference,
                            new ByteArrayInputStream(content)
                    )),
                    "attempt output upload did not return exact metadata"
            );
            signalAndBlock(arguments);
        }
    }

    private static BrokerOutboxStore.CommittedTaskAssignment seedAssignment(
            DatabaseManager database,
            Arguments arguments
    ) {
        JobStateStore.JobSubmissionDecision submission =
                database.commitJobSubmission(
                        arguments.jobId(),
                        TASK_TYPE,
                        arguments.requesterId(),
                        RequesterTokens.hashToken(arguments.requesterToken()),
                        "",
                        arguments.requestHash(),
                        "",
                        List.of(new JobStateStore.TaskStartupState(
                                arguments.taskId(),
                                "alpha"
                        ))
                );
        require(
                submission.outcome() == JobStateStore.JobSubmissionOutcome.COMMITTED
                        || submission.outcome()
                        == JobStateStore.JobSubmissionOutcome.REPLAY,
                "job seed was neither committed nor replayed"
        );
        BrokerOutboxStore.OutboxMessage template =
                new BrokerOutboxStore.OutboxMessage(
                        TransportRoute.TASK_ASSIGN,
                        arguments.peerId(),
                        "COORDINATOR",
                        new TaskAssignMessage(
                                arguments.peerId(),
                                Instant.EPOCH.toString(),
                                arguments.taskId(),
                                arguments.jobId(),
                                TASK_TYPE,
                                "alpha",
                                ""
                        )
                );
        BrokerOutboxStore.TaskAssignmentCommit commit =
                database.commitTaskAssignmentAndEnqueueBrokerOutbox(
                        arguments.taskId(),
                        arguments.peerId(),
                        STARTED_AT,
                        "crash-window-lease-owner",
                        LEASE_EXPIRES_AT,
                        arguments.assignmentId(),
                        template
                );
        require(
                commit.outcome().projectionAllowed()
                        && commit.assignment() != null,
                "assignment/outbox seed did not commit"
        );
        return commit.assignment();
    }

    private static void seedFinalizingResult(
            DatabaseManager database,
            Arguments arguments
    ) {
        BrokerOutboxStore.CommittedTaskAssignment assignment =
                seedAssignment(database, arguments);
        require(
                database.markBrokerOutboxPublished(
                        assignment.outboxRecord().outboxId(),
                        STARTED_AT + 1L
                ),
                "assignment outbox could not be marked sent"
        );
        require(
                database.commitTaskResult(
                        arguments.taskId(),
                        assignment.identity().attemptNumber(),
                        assignment.identity().assignmentId(),
                        arguments.peerId(),
                        STARTED_AT + 100L,
                        100L,
                        RESULT_PAYLOAD
                ) == JobStateStore.ResultCommitOutcome.COMMITTED,
                "authoritative result did not commit"
        );
    }

    private static TaskResultMessage taskResult(Arguments arguments) {
        return new TaskResultMessage(
                arguments.peerId(),
                Instant.EPOCH.toString(),
                arguments.taskId(),
                arguments.jobId(),
                1,
                arguments.assignmentId(),
                RESULT_PAYLOAD,
                true,
                null
        );
    }

    private static BrokerOutboxStore.OutboxMessage finalResultOutbox(
            Arguments arguments
    ) {
        JobResultMessage result = new JobResultMessage(
                "COORDINATOR",
                Instant.EPOCH.toString(),
                arguments.jobId(),
                TASK_TYPE,
                true,
                List.of(RESULT_PAYLOAD),
                List.of(RESULT_PAYLOAD)
        );
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.JOB_RESULT,
                arguments.requesterId(),
                "COORDINATOR",
                result
        );
    }

    private static ObjectStore openObjectStore(Arguments arguments)
            throws Exception {
        System.setProperty("taskflow.minioEndpoint", arguments.minioEndpoint());
        System.setProperty("taskflow.minioAccessKey", arguments.minioAccessKey());
        System.setProperty("taskflow.minioSecretKey", arguments.minioSecretKey());
        System.setProperty("taskflow.minioBucket", arguments.minioBucket());
        return ObjectStores.open();
    }

    private static void signalAndBlock(Arguments arguments) throws Exception {
        Files.createDirectories(arguments.signalPath().getParent());
        Files.writeString(
                arguments.signalPath(),
                arguments.failpoint().name(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
        );
        new CountDownLatch(1).await();
        throw new AssertionError("Crash-window failpoint was released");
    }

    private static String sha256(byte[] content) throws Exception {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(content)
        );
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    enum Failpoint {
        AFTER_JOB_TRANSACTION_COMMIT,
        AFTER_ASSIGNMENT_OUTBOX_COMMIT,
        AFTER_ASSIGNMENT_BROKER_CONFIRM_BEFORE_MARK,
        AFTER_RESULT_PUBLISH_CONFIRM,
        AFTER_RESULT_COMMIT,
        AFTER_TERMINAL_OUTBOX_COMMIT,
        AFTER_FINAL_RESULT_CONFIRM_BEFORE_MARK,
        DURING_OBJECT_UPLOAD,
        AFTER_OBJECT_UPLOAD_BEFORE_RESULT
    }

    record Arguments(
            Failpoint failpoint,
            Path database,
            Path signalPath,
            String jobId,
            String assignmentId,
            String rabbitHost,
            int rabbitPort,
            String rabbitUsername,
            String rabbitPassword,
            String rabbitToken,
            String minioEndpoint,
            String minioAccessKey,
            String minioSecretKey,
            String minioBucket
    ) {
        static Arguments parse(String[] args) {
            if (args.length != 14) {
                throw new IllegalArgumentException(
                        "Expected 14 crash-window child arguments; received "
                                + args.length
                );
            }
            return new Arguments(
                    Failpoint.valueOf(args[0]),
                    Path.of(args[1]).toAbsolutePath(),
                    Path.of(args[2]).toAbsolutePath(),
                    args[3],
                    args[4],
                    args[5],
                    Integer.parseInt(args[6]),
                    args[7],
                    args[8],
                    args[9],
                    args[10],
                    args[11],
                    args[12],
                    args[13]
            );
        }

        String databasePath() {
            return database.toString();
        }

        String taskId() {
            return "task-" + jobId + "-0";
        }

        String requesterId() {
            return "requester-" + jobId;
        }

        String peerId() {
            return "peer-" + jobId;
        }

        String requesterToken() {
            return "token-" + jobId;
        }

        String requestHash() {
            return "request-hash-" + jobId;
        }

        String outputKey() {
            return TaskFlowObjectKeys.attemptOutputKey(
                    jobId,
                    taskId(),
                    1,
                    assignmentId
            );
        }

        RabbitMqTransportConfig rabbitConfig() {
            String name = "taskflow.crash." + rabbitToken;
            return new RabbitMqTransportConfig(
                    rabbitHost,
                    rabbitPort,
                    rabbitUsername,
                    rabbitPassword,
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
    }

    private static final class FailpointInputStream extends InputStream {
        private final byte[] content;
        private final int firstChunkBytes;
        private final Arguments arguments;
        private int position;
        private boolean failpointReached;

        private FailpointInputStream(
                byte[] content,
                int firstChunkBytes,
                Arguments arguments
        ) {
            this.content = content;
            this.firstChunkBytes = firstChunkBytes;
            this.arguments = arguments;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int read = read(one, 0, 1);
            return read == -1 ? -1 : Byte.toUnsignedInt(one[0]);
        }

        @Override
        public int read(byte[] target, int offset, int length)
                throws IOException {
            if (position >= content.length) {
                return -1;
            }
            if (position >= firstChunkBytes && !failpointReached) {
                failpointReached = true;
                try {
                    signalAndBlock(arguments);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Upload failpoint interrupted", e);
                } catch (Exception e) {
                    throw new IOException("Upload failpoint failed", e);
                }
            }
            int allowed = position < firstChunkBytes
                    ? firstChunkBytes - position
                    : content.length - position;
            int count = Math.min(length, Math.min(allowed, content.length - position));
            System.arraycopy(content, position, target, offset, count);
            position += count;
            return count;
        }
    }
}
