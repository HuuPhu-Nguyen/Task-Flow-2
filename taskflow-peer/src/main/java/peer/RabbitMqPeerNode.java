package peer;

import com.google.gson.Gson;
import peer.engine.PeerExecutionEngine;
import protocol.FilePayload;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRuntimeDefaults;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RabbitMqPeerNode {
    private static final long HEARTBEAT_INTERVAL_MILLIS = 30_000;
    private static final long JOB_RESULT_TIMEOUT_MINUTES = 15;
    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws Exception {
        String nodeId = resolveNodeId();
        RabbitMqTransport transport = new RabbitMqTransport(RabbitMqTransportConfig.fromEnvironment());
        transport.declareTopology();

        BlockingQueue<JobResultMessage> jobResults = new LinkedBlockingQueue<>();
        PeerExecutionEngine engine = new PeerExecutionEngine(nodeId);
        System.out.println("RabbitMQ peer " + nodeId + " registered task processors: "
                + engine.getRegisteredTaskTypes());

        transport.subscribePeer(TransportRoute.TASK_ASSIGN, nodeId,
                delivery -> handleTaskAssignment(nodeId, transport, engine, delivery));
        transport.subscribePeer(TransportRoute.JOB_RESULT, nodeId,
                delivery -> handleJobResult(jobResults, delivery));

        ScheduledExecutorService heartbeats = startHeartbeats(transport, nodeId, engine.getRegisteredTaskTypes());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            heartbeats.shutdownNow();
            engine.shutdown();
            try {
                transport.close();
            } catch (Exception ignored) {
            }
        }));

        if (isSubmitCommand(args)) {
            String jobId = submitJob(nodeId, transport, args);
            try {
                waitForJobResult(jobId, jobResults);
            } finally {
                heartbeats.shutdownNow();
                engine.shutdown();
                transport.close();
            }
            return;
        }

        System.out.println("RabbitMqPeerNode consuming peer-specific task assignments.");
        Thread.currentThread().join();
    }

    private static void handleTaskAssignment(String nodeId,
                                             RabbitMqTransport transport,
                                             PeerExecutionEngine engine,
                                             InboundTransportMessage delivery) throws Exception {
        Message message = delivery.message();
        if (!(message instanceof TaskAssignMessage task)) {
            reject(delivery.acknowledgement());
            return;
        }
        if (!nodeId.equals(task.getNodeId())) {
            System.err.println("Ignoring task assigned to " + task.getNodeId() + " on peer " + nodeId);
            ack(delivery.acknowledgement());
            return;
        }

        try {
            TaskResultMessage result = engine.executeTask(task).get();
            transport.publish(new OutboundTransportMessage(
                    TransportRoute.TASK_RESULT,
                    result.getNodeId(),
                    result
            ));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw new RuntimeException("Peer execution failed before result publication.", e);
        }
    }

    private static void handleJobResult(BlockingQueue<JobResultMessage> jobResults,
                                        InboundTransportMessage delivery) throws Exception {
        Message message = delivery.message();
        if (!(message instanceof JobResultMessage result)) {
            reject(delivery.acknowledgement());
            return;
        }
        jobResults.offer(result);
        System.out.println("Received JOB_RESULT for " + result.getJobId()
                + " success=" + result.isSuccessful());
    }

    private static ScheduledExecutorService startHeartbeats(RabbitMqTransport transport,
                                                            String nodeId,
                                                            Collection<String> supportedTaskTypes) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "rabbitmq-peer-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        Runnable heartbeat = () -> {
            try {
                transport.publish(new OutboundTransportMessage(
                        TransportRoute.HEARTBEAT,
                        nodeId,
                        new PongMessage(nodeId, Instant.now().toString(), supportedTaskTypes)
                ));
            } catch (Exception e) {
                System.err.println("RabbitMQ heartbeat failed for " + nodeId + ": " + e.getMessage());
            }
        };
        heartbeat.run();
        scheduler.scheduleAtFixedRate(
                heartbeat,
                HEARTBEAT_INTERVAL_MILLIS,
                HEARTBEAT_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS
        );
        return scheduler;
    }

    private static String submitJob(String nodeId, RabbitMqTransport transport, String[] args) throws Exception {
        if (args.length < 4) {
            throw new IllegalArgumentException("""
                    Usage: TASKFLOW_TRANSPORT=rabbitmq peer.PeerNode submit <image|video|task-type> <target-format> <file> [file...]
                    Example: .\\mvnw.cmd -pl taskflow-peer exec:java -Dexec.args="submit image png sample.jpg"
                    """);
        }

        String taskType = normalizeTaskType(args[1]);
        String parameter = args[2];
        List<Object> payloads = new ArrayList<>();
        for (int i = 3; i < args.length; i++) {
            payloads.add(readPayload(Path.of(args[i])));
        }

        String jobId = "JOB_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8);
        JobSubmitMessage message = new JobSubmitMessage(
                nodeId,
                Instant.now().toString(),
                jobId,
                taskType,
                payloads,
                parameter
        );
        transport.publish(new OutboundTransportMessage(TransportRoute.JOB_SUBMIT, nodeId, message));
        System.out.println("Submitted RabbitMQ job " + jobId + " from peer " + nodeId);
        return jobId;
    }

    private static void waitForJobResult(String jobId,
                                         BlockingQueue<JobResultMessage> jobResults) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(JOB_RESULT_TIMEOUT_MINUTES);
        while (System.nanoTime() < deadline) {
            long remaining = deadline - System.nanoTime();
            JobResultMessage result = jobResults.poll(remaining, TimeUnit.NANOSECONDS);
            if (result == null) {
                break;
            }
            if (!jobId.equals(result.getJobId())) {
                continue;
            }
            if (!result.isSuccessful()) {
                System.err.println("Job " + jobId + " failed: " + result.getErrorMessage());
                return;
            }
            Path outputDir = writeJobResults(result);
            System.out.println("Job " + jobId + " completed. Results written to " + outputDir);
            return;
        }
        throw new IllegalStateException("Timed out waiting for JOB_RESULT for " + jobId);
    }

    private static Path writeJobResults(JobResultMessage result) throws IOException {
        Path outputDir = Path.of("target", "rabbitmq-results", result.getJobId());
        Files.createDirectories(outputDir);
        List<Object> results = result.getResultsByTaskId();
        if (results == null) {
            return outputDir;
        }

        for (int i = 0; i < results.size(); i++) {
            FilePayload payload = GSON.fromJson(GSON.toJson(results.get(i)), FilePayload.class);
            if (payload == null || payload.base64Data() == null) {
                continue;
            }
            String fileName = i + "-" + sanitizeFileName(payload.fileName());
            Files.write(outputDir.resolve(fileName), Base64.getDecoder().decode(payload.base64Data()));
        }
        return outputDir;
    }

    private static FilePayload readPayload(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IOException("Input file does not exist: " + path);
        }
        String base64 = Base64.getEncoder().encodeToString(Files.readAllBytes(path));
        return new FilePayload(path.getFileName().toString(), base64);
    }

    private static String normalizeTaskType(String raw) {
        String value = raw.trim().toUpperCase(Locale.ROOT);
        return switch (value) {
            case "IMAGE", "IMAGE_CONVERSION" -> "IMAGE_CONVERSION";
            case "VIDEO", "VIDEO_TRANSCODING" -> "VIDEO_TRANSCODING";
            default -> value;
        };
    }

    private static String resolveNodeId() {
        String configured = System.getenv(RabbitMqRuntimeDefaults.PEER_ID_ENV);
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        return RabbitMqRuntimeDefaults.PEER_ID_PREFIX + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static boolean isSubmitCommand(String[] args) {
        return args.length > 0 && "submit".equalsIgnoreCase(args[0]);
    }

    private static String sanitizeFileName(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return "result.bin";
        }
        String sanitized = fileName.replaceAll("[\\\\/:*?\"<>|]", "_");
        return sanitized.isBlank() ? "result.bin" : sanitized;
    }

    private static void ack(TransportAcknowledgement acknowledgement) throws Exception {
        if (acknowledgement != null) {
            acknowledgement.ack();
        }
    }

    private static void reject(TransportAcknowledgement acknowledgement) throws Exception {
        if (acknowledgement != null) {
            acknowledgement.reject();
        }
    }
}
