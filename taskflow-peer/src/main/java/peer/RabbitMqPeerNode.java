package peer;

import client.ClientJobPlugin;
import client.ClientJobPlugins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peer.engine.PeerExecutionEngine;
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

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RabbitMqPeerNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqPeerNode.class);

    private static final long HEARTBEAT_INTERVAL_MILLIS = 30_000;
    private static final long JOB_RESULT_TIMEOUT_MINUTES = 15;

    public static void main(String[] args) throws Exception {
        String nodeId = resolveNodeId();
        RabbitMqTransport transport = new RabbitMqTransport(RabbitMqTransportConfig.fromEnvironment());
        transport.declareTopology();

        BlockingQueue<JobResultMessage> jobResults = new LinkedBlockingQueue<>();
        PeerExecutionEngine engine = new PeerExecutionEngine(nodeId);
        LOGGER.info("event=peer_processors_registered transport=rabbitmq peer_id={} task_types={}",
                nodeId, engine.getRegisteredTaskTypes());

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
            Map<String, ClientJobPlugin> clientPlugins = ClientJobPlugins.byTaskType(ClientJobPlugins.discover());
            LOGGER.info("event=peer_client_plugins_registered task_types={}", clientPlugins.keySet());

            String jobId = submitJob(nodeId, transport, args, clientPlugins);
            try {
                waitForJobResult(jobId, jobResults, clientPlugins);
            } finally {
                heartbeats.shutdownNow();
                engine.shutdown();
                transport.close();
            }
            return;
        }

        LOGGER.info("event=peer_consuming_assignments transport=rabbitmq peer_id={}", nodeId);
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
            LOGGER.warn("event=task_assignment_ignored peer_id={} assigned_peer_id={}",
                    nodeId, task.getNodeId());
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
        LOGGER.info("event=job_result_received job_id={} success={}",
                result.getJobId(), result.isSuccessful());
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
                LOGGER.warn("event=rabbitmq_heartbeat_failed peer_id={} error={}",
                        nodeId, e.getMessage(), e);
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

    private static String submitJob(String nodeId,
                                    RabbitMqTransport transport,
                                    String[] args,
                                    Map<String, ClientJobPlugin> clientPlugins) throws Exception {
        if (args.length < 4) {
            throw new IllegalArgumentException("""
                    Usage: TASKFLOW_TRANSPORT=rabbitmq peer.PeerNode submit <image|video|task-type> <target-format> <file> [file...]
                    Example: .\\mvnw.cmd -pl taskflow-peer exec:java -Dexec.args="submit image png sample.jpg"
                    """);
        }

        ClientJobPlugin plugin = resolveClientPlugin(args[1], clientPlugins);
        String taskType = plugin.taskType();
        String parameter = plugin.normalizeParameter(args[2]);
        List<Path> inputPaths = new java.util.ArrayList<>();
        for (int i = 3; i < args.length; i++) {
            inputPaths.add(Path.of(args[i]));
        }
        List<Object> payloads = plugin.buildPayloads(inputPaths, parameter);

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
        LOGGER.info("event=job_submitted transport=rabbitmq job_id={} peer_id={}", jobId, nodeId);
        return jobId;
    }

    private static void waitForJobResult(String jobId,
                                         BlockingQueue<JobResultMessage> jobResults,
                                         Map<String, ClientJobPlugin> clientPlugins) throws Exception {
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
                LOGGER.error("event=job_failed job_id={} error={}", jobId, result.getErrorMessage());
                return;
            }
            Path outputDir = writeJobResults(result, clientPlugins);
            LOGGER.info("event=job_completed job_id={} output_dir={}", jobId, outputDir);
            return;
        }
        throw new IllegalStateException("Timed out waiting for JOB_RESULT for " + jobId);
    }

    private static Path writeJobResults(JobResultMessage result,
                                        Map<String, ClientJobPlugin> clientPlugins) throws Exception {
        Path outputDir = Path.of("target", "rabbitmq-results", result.getJobId());
        ClientJobPlugin plugin = resolveClientPlugin(result.getTaskType(), clientPlugins);
        plugin.saveResults(result.getResultsByTaskId(), outputDir);
        return outputDir;
    }

    private static ClientJobPlugin resolveClientPlugin(String rawTaskType,
                                                       Map<String, ClientJobPlugin> clientPlugins) {
        String taskType = normalizeTaskType(rawTaskType);
        ClientJobPlugin plugin = clientPlugins.get(taskType);
        if (plugin == null) {
            throw new IllegalArgumentException(
                    "No client job plugin found for task type '" + rawTaskType
                            + "'. Available task types: " + clientPlugins.keySet());
        }
        return plugin;
    }

    private static String normalizeTaskType(String raw) {
        String value = raw.trim().toUpperCase(Locale.ROOT);
        return switch (value) {
            case "IMAGE", "IMAGE_CONVERSION" -> "IMAGE_CONVERSION";
            case "VIDEO", "VIDEO_TRANSCODING" -> "VIDEO_TRANSCODING";
            case "TEXT", "TEXT_ANALYSIS" -> "TEXT_ANALYSIS";
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
