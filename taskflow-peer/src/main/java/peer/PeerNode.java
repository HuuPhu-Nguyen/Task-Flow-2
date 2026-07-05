package peer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import messaging.MessageDispatcher;
import messaging.MessageFactory;
import messaging.SafeJsonWriter;
import messaging.handlers.PingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peer.engine.PeerExecutionEngine;
import protocol.*;

public class PeerNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeerNode.class);

    private final Set<String> myActiveJobIds = ConcurrentHashMap.newKeySet();
    private static final String TRANSPORT_ENV = "TASKFLOW_TRANSPORT";
    private static final String TCP_PEER_ID_PREFIX = "TCP_PEER";

    private final String nodeId;

    public PeerNode() {
        this(PeerIdentity.configuredOrGenerated(TCP_PEER_ID_PREFIX));
    }

    PeerNode(String nodeId) {
        this.nodeId = PeerIdentity.require(nodeId);
    }

    public static void main(String[] args) throws Exception {
        if (isHelpRequested(args)) {
            System.out.println(usage());
            return;
        }

        if (isRabbitMqTransportSelected()) {
            RabbitMqPeerNode.main(args);
            return;
        }

        logTcpDeprecationWarning();

        if (args.length < 2) {
            LOGGER.info("event=peer_usage command=\"{}\"", tcpUsageLine());
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        String nodeId = PeerIdentity.configuredOrGenerated(TCP_PEER_ID_PREFIX);
        PeerExecutionEngine engine = new PeerExecutionEngine(nodeId);
        runTcpPeer(host, port, engine, nodeId);
    }

    static void runTcpPeer(String host, int port, PeerExecutionEngine engine) {
        runTcpPeer(host, port, engine, engine.nodeId());
    }

    static void runTcpPeer(String host, int port, PeerExecutionEngine engine, String nodeId) {
        Objects.requireNonNull(engine, "engine");
        String peerId = PeerIdentity.require(nodeId);
        Gson gson = new Gson();

        LOGGER.info("event=peer_processors_registered peer_id={} task_types={}",
                peerId, engine.getRegisteredTaskTypes());

        MessageFactory factory = createFactory(gson);

        LOGGER.info("event=peer_connecting peer_id={} host={} port={}", peerId, host, port);

        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            MessageDispatcher dispatcher = createDispatcher(peerId, engine, out);

            LOGGER.info("event=peer_connected peer_id={} local_address={}",
                    peerId, socket.getLocalSocketAddress());

            String incomingJson;
            while ((incomingJson = in.readLine()) != null) {
                try {
                    if (incomingJson.trim().isEmpty()) continue;

                    Message msg = factory.fromJson(incomingJson);
                    dispatcher.dispatch(msg, out);
                } catch (Exception e) {
                    LOGGER.warn("event=peer_message_processing_failed error={}", e.getMessage(), e);
                }
            }
            LOGGER.info("event=server_connection_closed");

        } catch (IOException e) {
            LOGGER.warn("event=server_connection_lost error={}", e.getMessage(), e);
        } finally {
            engine.shutdown();
            try {
                if (!engine.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOGGER.warn("event=peer_engine_shutdown_timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public String submitJob(String taskType, List<?> payloads, String parameter, PrintWriter out) {
        String jobId = JobIds.newJobId(nodeId);
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        String requesterToken = RequesterTokens.newToken();
        String time = java.time.Instant.now().toString();
        String signature = RequesterIdentity.signJobSubmit(
                identity.privateKey(),
                nodeId,
                time,
                jobId,
                taskType,
                parameter,
                requesterToken
        );

        List<Object> taskPayloads = payloads == null ? List.of() : new ArrayList<>(payloads);

        JobSubmitMessage msg = new JobSubmitMessage(
                nodeId,
                time,
                jobId,
                taskType,
                taskPayloads,
                parameter,
                requesterToken,
                identity.publicKey(),
                signature
        );

        boolean sent = SafeJsonWriter.send(out, new Gson(), msg);
        if (!sent) {
            LOGGER.warn("event=peer_job_submit_send_failed job_id={} task_type={}", jobId, taskType);
            throw new IllegalStateException("Could not send job submit message to coordinator.");
        }
        myActiveJobIds.add(jobId);
        return jobId;
    }

    private static MessageFactory createFactory(Gson gson) {
        MessageFactory factory = new MessageFactory();
        factory.register(MessageType.PING, json -> gson.fromJson(json, PingMessage.class));
        factory.register(MessageType.TASK_ASSIGN, json -> gson.fromJson(json, TaskAssignMessage.class));
        factory.register(MessageType.JOB_RESULT, json -> gson.fromJson(json, JobResultMessage.class));
        return factory;
    }

    private static MessageDispatcher createDispatcher(String nodeId, PeerExecutionEngine engine, PrintWriter out) {
        MessageDispatcher dispatcher = new MessageDispatcher();
        // Static handler for immediate responses (PING)
        dispatcher.register(MessageType.PING, new PingHandler(nodeId, () -> engine.getRegisteredTaskTypes()));
        // TASK_ASSIGN -> Background Engine
        // prevents the networking thread from blocking during task execution
        dispatcher.register(MessageType.TASK_ASSIGN, (message, writer) -> {
            TaskAssignMessage task = (TaskAssignMessage) message;
            engine.submitTask(task, out);
        });

        //Handling Job Results (If this peer submitted a job)
        //dispatcher.register(MessageType.JOB_RESULT, new messaging.handlers.JobResultHandler());

        return dispatcher;
    }

    private static boolean isRabbitMqTransportSelected() {
        return "rabbitmq".equalsIgnoreCase(System.getenv().getOrDefault(TRANSPORT_ENV, "tcp"));
    }

    private static void logTcpDeprecationWarning() {
        LOGGER.warn("event=tcp_transport_deprecated component=peer recommendation=TASKFLOW_TRANSPORT=rabbitmq");
    }

    static boolean isHelpRequested(String[] args) {
        return args != null
                && args.length > 0
                && ("--help".equals(args[0]) || "-h".equals(args[0]));
    }

    static String usage() {
        return String.join(System.lineSeparator(),
                tcpUsageLine(),
                "TASKFLOW_TRANSPORT=rabbitmq java -jar taskflow-peer-<version>-submitter-runtime.jar submit <task-type> <parameter> <file> [file...]",
                "TASKFLOW_TRANSPORT=rabbitmq java -jar taskflow-peer-<version>-executor-runtime.jar",
                "TASKFLOW_TRANSPORT=rabbitmq java -jar taskflow-peer-<version>-combined-runtime.jar [submit <task-type> <parameter> <file> [file...]]",
                "TASKFLOW_TRANSPORT=rabbitmq java -jar taskflow-peer-<version>-combined-runtime.jar dlq <inspect|redrive|quarantine|discard> [count]");
    }

    private static String tcpUsageLine() {
        return "TASKFLOW_TRANSPORT=tcp java -jar taskflow-peer-<version>-combined-runtime.jar <host> <port> (legacy; prefer TASKFLOW_TRANSPORT=rabbitmq)";
    }
}
