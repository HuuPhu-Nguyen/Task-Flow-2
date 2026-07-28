package server.scaling;

import protocol.MessageValidator;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.DeliveryDisposition;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRecoveryPolicy;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Separate-JVM executor fixture for TF-0707.
 *
 * <p>The process consumes real protocol-v2 assignments through the production
 * RabbitMQ adapter, performs bounded deterministic work, publishes confirmed
 * task results, and writes auditable utilization counters after a file-based
 * graceful-stop signal.</p>
 */
public final class ScalingWorkerProcessMain {
    static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    static final String MEASURED_JOB_PREFIX = "scaling-measured-";
    private static final RabbitMqRecoveryPolicy RECOVERY_POLICY =
            new RabbitMqRecoveryPolicy(1_000, 100L, 1_000L, 2.0D);

    private ScalingWorkerProcessMain() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        requireAbsent(arguments.readyPath(), "ready signal");
        requireAbsent(arguments.stopPath(), "stop signal");
        requireAbsent(arguments.failurePath(), "failure signal");
        requireAbsent(arguments.metricsPath(), "worker metrics");

        AtomicLong measuredTasks = new AtomicLong();
        AtomicLong measuredBusyNanos = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try (WatchService watchService =
                     FileSystems.getDefault().newWatchService();
             RabbitMqTransport transport = new RabbitMqTransport(
                     arguments.transportConfig(),
                     RECOVERY_POLICY
             )) {
            arguments.stopPath().getParent().register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY
            );
            transport.subscribePeer(
                    TransportRoute.TASK_ASSIGN,
                    arguments.workerId(),
                    delivery -> {
                        try {
                            if (!(delivery.message()
                                    instanceof TaskAssignMessage assignment)) {
                                delivery.acknowledgement().settle(
                                        DeliveryDisposition.REJECT_INVALID,
                                        "scaling_assignment_type_invalid"
                                );
                                return;
                            }
                            MessageValidator.validate(assignment);
                            if (!arguments.workerId().equals(
                                    assignment.getNodeId()
                            )) {
                                delivery.acknowledgement().settle(
                                        DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                                        "scaling_assignment_worker_mismatch"
                                );
                                return;
                            }
                            delivery.acknowledgement().defer();
                            long startedAt = System.nanoTime();
                            String resultPayload = performLightweightWork(
                                    String.valueOf(assignment.getPayload()),
                                    arguments.workUnitsPerTask()
                            );
                            long busyNanos = System.nanoTime() - startedAt;
                            TaskResultMessage result = new TaskResultMessage(
                                    arguments.workerId(),
                                    Instant.now().toString(),
                                    assignment.getTaskId(),
                                    assignment.getJobId(),
                                    assignment.getAttemptNumber(),
                                    assignment.getAssignmentId(),
                                    resultPayload,
                                    true,
                                    ""
                            );
                            if (!transport.publish(new OutboundTransportMessage(
                                    TransportRoute.TASK_RESULT,
                                    arguments.workerId(),
                                    result
                            ))) {
                                throw new IllegalStateException(
                                        "Task-result publish was not confirmed."
                                );
                            }
                            if (assignment.getJobId().startsWith(
                                    MEASURED_JOB_PREFIX
                            )) {
                                measuredBusyNanos.addAndGet(busyNanos);
                                measuredTasks.incrementAndGet();
                            }
                            delivery.acknowledgement().settle(
                                    DeliveryDisposition.ACK_SUCCESS,
                                    "scaling_result_published"
                            );
                        } catch (Throwable throwable) {
                            if (failure.compareAndSet(null, throwable)) {
                                writeFailure(arguments.failurePath(), throwable);
                            }
                            throw throwable;
                        }
                    }
            );
            Files.writeString(
                    arguments.readyPath(),
                    arguments.workerId(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
            );
            awaitStop(arguments, failure, watchService);
        }

        Throwable asynchronousFailure = failure.get();
        if (asynchronousFailure != null) {
            throw new IllegalStateException(
                    "Scaling worker failed before graceful stop.",
                    asynchronousFailure
            );
        }
        Files.write(
                arguments.metricsPath(),
                List.of(
                        "formatVersion=1",
                        "workerId=" + arguments.workerId(),
                        "measuredTasks=" + measuredTasks.get(),
                        "measuredBusyNanos=" + measuredBusyNanos.get(),
                        "workUnitsPerTask=" + arguments.workUnitsPerTask()
                ),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
        );
    }

    private static void awaitStop(
            Arguments arguments,
            AtomicReference<Throwable> failure,
            WatchService watchService
    ) throws Exception {
        while (true) {
            if (failure.get() != null) {
                throw new IllegalStateException(
                        "Scaling worker callback failed.",
                        failure.get()
                );
            }
            if (Files.exists(arguments.stopPath())) {
                return;
            }
            WatchKey key = watchService.poll(1L, TimeUnit.SECONDS);
            if (key == null) {
                continue;
            }
            key.pollEvents();
            if (!key.reset()) {
                throw new IllegalStateException(
                        "Scaling worker control directory became unavailable."
                );
            }
        }
    }

    private static String performLightweightWork(String payload, int workUnits) {
        long value = payload.hashCode() ^ 0x9E3779B97F4A7C15L;
        for (int index = 0; index < workUnits; index++) {
            value ^= Long.rotateLeft(
                    value + index + 0xD1B54A32D192ED03L,
                    17
            );
            value *= 0x94D049BB133111EBL;
        }
        return Long.toUnsignedString(value);
    }

    private static void writeFailure(Path path, Throwable failure) {
        String text = failure.getClass().getName()
                + ": "
                + String.valueOf(failure.getMessage())
                + System.lineSeparator();
        try {
            Files.writeString(
                    path,
                    text,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
            );
        } catch (Exception writeFailure) {
            failure.addSuppressed(writeFailure);
        }
    }

    private static void requireAbsent(Path path, String description) {
        if (Files.exists(path)) {
            throw new IllegalStateException(
                    "Scaling " + description + " already exists: " + path
            );
        }
    }

    record Arguments(
            String workerId,
            String host,
            int port,
            String username,
            String password,
            String token,
            int workUnitsPerTask,
            Path readyPath,
            Path stopPath,
            Path failurePath,
            Path metricsPath
    ) {
        private static Arguments parse(String[] args) {
            if (args.length != 11) {
                throw new IllegalArgumentException(
                        "Expected workerId, RabbitMQ host/port/user/password, "
                                + "token, work units, ready, stop, failure, and "
                                + "metrics paths."
                );
            }
            return new Arguments(
                    args[0],
                    args[1],
                    Integer.parseInt(args[2]),
                    args[3],
                    args[4],
                    args[5],
                    Integer.parseInt(args[6]),
                    Path.of(args[7]),
                    Path.of(args[8]),
                    Path.of(args[9]),
                    Path.of(args[10])
            );
        }

        private RabbitMqTransportConfig transportConfig() {
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

        Arguments {
            if (workerId == null || workerId.isBlank()) {
                throw new IllegalArgumentException("workerId is required.");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("RabbitMQ host is required.");
            }
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("RabbitMQ port is invalid.");
            }
            if (username == null || username.isBlank()
                    || password == null || password.isBlank()
                    || token == null || token.isBlank()) {
                throw new IllegalArgumentException(
                        "RabbitMQ credentials and token are required."
                );
            }
            if (workUnitsPerTask < 1
                    || workUnitsPerTask
                    > ScalingExperimentConfig.MAX_WORK_UNITS) {
                throw new IllegalArgumentException(
                        String.format(
                                Locale.ROOT,
                                "workUnitsPerTask must be in [1, %d].",
                                ScalingExperimentConfig.MAX_WORK_UNITS
                        )
                );
            }
            if (readyPath == null
                    || stopPath == null
                    || failurePath == null
                    || metricsPath == null) {
                throw new IllegalArgumentException(
                        "Worker control and metrics paths are required."
                );
            }
        }
    }
}
