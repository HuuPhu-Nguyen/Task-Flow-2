package peer;

import client.ClientJobPlugin;
import client.ClientJobPlugins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peer.engine.AssignmentCacheConflictException;
import peer.engine.AssignmentCacheSnapshot;
import peer.engine.AssignmentExecution;
import peer.engine.PeerExecutionEngine;
import protocol.JobResultMessage;
import protocol.JobIds;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.PeerIdentity;
import protocol.PongMessage;
import protocol.RequesterIdentity;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.BrokerTransport;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqDlqClient;
import transport.rabbitmq.RabbitMqRuntimeDefaults;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RabbitMqPeerNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqPeerNode.class);

    private static final long HEARTBEAT_INTERVAL_MILLIS = 30_000;
    private static final long JOB_RESULT_TIMEOUT_MINUTES = 15;

    public static void main(String[] args) throws Exception {
        if (RabbitMqDlqCommand.isCommand(args)) {
            runDlqCommand(args);
            return;
        }

        String nodeId = resolveNodeId();
        RabbitMqTransport transport = new RabbitMqTransport(RabbitMqTransportConfig.fromEnvironment());
        transport.declareTopology();

        BlockingQueue<ReceivedJobResult> jobResults = new LinkedBlockingQueue<>();
        PeerExecutionEngine engine = new PeerExecutionEngine(nodeId);
        LOGGER.info(
                "event=peer_processors_registered transport=rabbitmq peer_id={} task_types={} "
                        + "assignment_cache_max_entries={} assignment_cache_ttl_ms={}",
                nodeId,
                engine.getRegisteredTaskTypes(),
                engine.assignmentCacheConfig().maxEntries(),
                engine.assignmentCacheConfig().ttlMillis()
        );

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

    static void handleTaskAssignment(String nodeId,
                                     BrokerTransport transport,
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
            if (delivery.acknowledgement() != null) {
                delivery.acknowledgement().defer();
            }
            AssignmentExecution execution = engine.executeAssignment(task);
            logAssignmentCacheDecision(nodeId, task, execution, engine.assignmentCacheSnapshot());
            if (execution.disposition() == AssignmentExecution.Disposition.DUPLICATE_RUNNING) {
                ack(delivery.acknowledgement());
                return;
            }
            execution.resultFuture().whenComplete((result, failure) -> {
                if (failure != null) {
                    LOGGER.warn("event=task_execution_future_failed peer_id={} task_id={} error={}",
                            nodeId, task.getTaskId(), failure.getMessage(), failure);
                    requeueQuietly(delivery.acknowledgement());
                    return;
                }
                try {
                    publishConfirmed(transport, new OutboundTransportMessage(
                            TransportRoute.TASK_RESULT,
                            result.getNodeId(),
                            result
                    ), "Task result publish was not confirmed for task " + task.getTaskId());
                    ack(delivery.acknowledgement());
                } catch (Exception publishError) {
                    LOGGER.warn("event=task_result_publish_failed peer_id={} task_id={} error={}",
                            nodeId, task.getTaskId(), publishError.getMessage(), publishError);
                    requeueQuietly(delivery.acknowledgement());
                }
            });
        } catch (AssignmentCacheConflictException conflict) {
            LOGGER.warn(
                    "event=task_assignment_cache_conflict peer_id={} task_id={} assignment_id={} error={}",
                    nodeId,
                    task.getTaskId(),
                    task.getAssignmentId(),
                    conflict.getMessage()
            );
            reject(delivery.acknowledgement());
        } catch (Exception e) {
            requeueQuietly(delivery.acknowledgement());
            throw e;
        }
    }

    private static void logAssignmentCacheDecision(String nodeId,
                                                   TaskAssignMessage task,
                                                   AssignmentExecution execution,
                                                   AssignmentCacheSnapshot snapshot) {
        if (execution.disposition() == AssignmentExecution.Disposition.STARTED) {
            return;
        }
        LOGGER.info(
                "event=task_assignment_duplicate transport=rabbitmq peer_id={} task_id={} assignment_id={} "
                        + "disposition={} cache_size={} cache_evictions_total={}",
                nodeId,
                task.getTaskId(),
                task.getAssignmentId(),
                execution.disposition(),
                snapshot.size(),
                snapshot.evictionCount()
        );
    }

    private static void handleJobResult(BlockingQueue<ReceivedJobResult> jobResults,
                                        InboundTransportMessage delivery) throws Exception {
        Message message = delivery.message();
        if (!(message instanceof JobResultMessage result)) {
            reject(delivery.acknowledgement());
            return;
        }
        if (delivery.acknowledgement() != null) {
            delivery.acknowledgement().defer();
        }
        jobResults.offer(new ReceivedJobResult(result, delivery.acknowledgement()));
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
                publishConfirmed(transport, new OutboundTransportMessage(
                        TransportRoute.HEARTBEAT,
                        nodeId,
                        new PongMessage(nodeId, Instant.now().toString(), supportedTaskTypes)
                ), "Heartbeat publish was not confirmed");
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

    static String submitJob(String nodeId,
                            BrokerTransport transport,
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

        String jobId = JobIds.newJobId(nodeId);
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        String requesterToken = RequesterTokens.newToken();
        String time = Instant.now().toString();
        String signature = RequesterIdentity.signJobSubmit(
                identity.privateKey(),
                nodeId,
                time,
                jobId,
                taskType,
                parameter,
                requesterToken
        );
        JobSubmitMessage message = new JobSubmitMessage(
                nodeId,
                time,
                jobId,
                taskType,
                payloads,
                parameter,
                requesterToken,
                identity.publicKey(),
                signature
        );
        publishConfirmed(
                transport,
                new OutboundTransportMessage(TransportRoute.JOB_SUBMIT, nodeId, message),
                "Job submit publish was not confirmed for job " + jobId
        );
        LOGGER.info("event=job_submitted transport=rabbitmq job_id={} peer_id={}", jobId, nodeId);
        return jobId;
    }

    static void publishConfirmed(BrokerTransport transport,
                                 OutboundTransportMessage message,
                                 String failureMessage) throws Exception {
        if (!transport.publish(message)) {
            throw new IllegalStateException(failureMessage);
        }
    }

    private static void waitForJobResult(String jobId,
                                         BlockingQueue<ReceivedJobResult> jobResults,
                                         Map<String, ClientJobPlugin> clientPlugins) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(JOB_RESULT_TIMEOUT_MINUTES);
        while (System.nanoTime() < deadline) {
            long remaining = deadline - System.nanoTime();
            ReceivedJobResult received = jobResults.poll(remaining, TimeUnit.NANOSECONDS);
            if (received == null) {
                break;
            }
            JobResultMessage result = received.result();
            if (!jobId.equals(result.getJobId())) {
                ack(received.acknowledgement());
                continue;
            }
            if (!result.isSuccessful()) {
                LOGGER.error("event=job_failed job_id={} error={}", jobId, result.getErrorMessage());
                ack(received.acknowledgement());
                return;
            }
            try {
                Path outputDir = writeJobResults(result, clientPlugins);
                ack(received.acknowledgement());
                LOGGER.info("event=job_completed job_id={} output_dir={}", jobId, outputDir);
                return;
            } catch (Exception saveError) {
                requeue(received.acknowledgement());
                throw saveError;
            }
        }
        throw new IllegalStateException("Timed out waiting for JOB_RESULT for " + jobId);
    }

    static Path writeJobResults(JobResultMessage result,
                                Map<String, ClientJobPlugin> clientPlugins) throws Exception {
        Path outputDir = Path.of("target", "rabbitmq-results", result.getJobId());
        ClientJobPlugin plugin = resolveClientPlugin(result.getTaskType(), clientPlugins);
        plugin.handleResult(result, outputDir);
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
        return PeerIdentity.configuredOrGenerated(RabbitMqRuntimeDefaults.PEER_ID_PREFIX);
    }

    private static boolean isSubmitCommand(String[] args) {
        return args.length > 0 && "submit".equalsIgnoreCase(args[0]);
    }

    private static void runDlqCommand(String[] args) throws Exception {
        try (RabbitMqDlqClient dlqClient = new RabbitMqDlqClient(RabbitMqTransportConfig.fromEnvironment())) {
            dlqClient.declareTopology();
            RabbitMqDlqCommand.run(dlqClient, args, System.out);
        }
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

    private static void requeue(TransportAcknowledgement acknowledgement) throws Exception {
        if (acknowledgement != null) {
            acknowledgement.requeue();
        }
    }

    private static void requeueQuietly(TransportAcknowledgement acknowledgement) {
        try {
            requeue(acknowledgement);
        } catch (Exception requeueError) {
            LOGGER.warn("event=rabbitmq_delivery_requeue_failed error={}",
                    requeueError.getMessage(), requeueError);
        }
    }

    private record ReceivedJobResult(JobResultMessage result, TransportAcknowledgement acknowledgement) {
    }
}
