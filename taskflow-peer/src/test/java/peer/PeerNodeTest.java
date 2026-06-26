package peer;

import com.google.gson.Gson;
import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;
import peer.engine.PeerExecutionEngine;
import protocol.JobSubmitMessage;
import protocol.PingMessage;
import protocol.RequesterIdentity;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerNodeTest {

    @Test
    void submitJobWritesJobSubmitMessage() {
        PeerNode node = new PeerNode();
        StringWriter buffer = new StringWriter();
        PrintWriter out = new PrintWriter(buffer);

        String jobId = node.submitJob("TEXT_ANALYSIS", List.of("payload"), "summary", out);

        String json = buffer.toString();
        assertFalse(jobId.isBlank());
        assertTrue(json.contains("\"type\":\"JOB_SUBMIT\""));
        assertTrue(json.contains("\"jobId\":\"" + jobId + "\""));
        assertTrue(json.contains("\"taskType\":\"TEXT_ANALYSIS\""));
        assertTrue(RequesterIdentity.verifyJobSubmit(new Gson().fromJson(json, JobSubmitMessage.class)));
    }

    @Test
    void submitJobFailsFastWhenMessageCannotBeWritten() {
        PeerNode node = new PeerNode();
        PrintWriter out = new PrintWriter(new FailingWriter());

        assertThrows(IllegalStateException.class,
                () -> node.submitJob("TEXT_ANALYSIS", List.of("payload"), "summary", out));
    }

    @Test
    void rabbitMqPublishConfirmedReturnsWhenBrokerConfirms() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport(true);
        OutboundTransportMessage message = new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                new PingMessage("peer-1", "now")
        );

        RabbitMqPeerNode.publishConfirmed(transport, message, "publish failed");

        assertEquals(message, transport.publishedMessage);
    }

    @Test
    void rabbitMqPublishConfirmedThrowsWhenBrokerDoesNotConfirm() {
        RecordingBrokerTransport transport = new RecordingBrokerTransport(false);
        OutboundTransportMessage message = new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                new PingMessage("peer-1", "now")
        );

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> RabbitMqPeerNode.publishConfirmed(transport, message, "publish failed"));

        assertEquals("publish failed", error.getMessage());
        assertEquals(message, transport.publishedMessage);
    }

    @Test
    void rabbitMqSubmitCommandBuildsPluginPayloadAndPublishesJobSubmit() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport(true);
        CapturingClientPlugin plugin = new CapturingClientPlugin();

        String jobId = RabbitMqPeerNode.submitJob(
                "peer-submit",
                transport,
                new String[] {"submit", "text", "csv", "notes-one.txt", "notes-two.txt"},
                Map.of("TEXT_ANALYSIS", plugin)
        );

        assertFalse(jobId.isBlank());
        assertEquals(List.of(Path.of("notes-one.txt"), Path.of("notes-two.txt")), plugin.inputPaths);
        assertEquals("CSV", plugin.parameter);
        assertEquals(TransportRoute.JOB_SUBMIT, transport.publishedMessage.route());
        assertEquals("peer-submit", transport.publishedMessage.fromNodeId());

        JobSubmitMessage message = (JobSubmitMessage) transport.publishedMessage.message();
        assertEquals(jobId, message.getJobId());
        assertEquals("TEXT_ANALYSIS", message.getTaskType());
        assertEquals("CSV", message.getParameter());
        assertEquals(List.of("payload:notes-one.txt:CSV", "payload:notes-two.txt:CSV"), message.getTaskPayloads());
        assertTrue(RequesterIdentity.verifyJobSubmit(message));
    }

    @Test
    void rabbitMqSubmitCommandFailsWhenBrokerDoesNotConfirmJobSubmit() {
        RecordingBrokerTransport transport = new RecordingBrokerTransport(false);

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> RabbitMqPeerNode.submitJob(
                "peer-submit",
                transport,
                new String[] {"submit", "TEXT_ANALYSIS", "csv", "notes.txt"},
                Map.of("TEXT_ANALYSIS", new CapturingClientPlugin())
        ));

        assertTrue(error.getMessage().contains("Job submit publish was not confirmed"));
        assertEquals(TransportRoute.JOB_SUBMIT, transport.publishedMessage.route());
    }

    @Test
    void tcpRuntimeShutsDownEngineWhenServerClosesConnection() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        AtomicReference<Exception> serverFailure = new AtomicReference<>();

        try (ServerSocket server = new ServerSocket(0)) {
            Thread serverThread = new Thread(() -> {
                try (Socket ignored = server.accept()) {
                    // Close immediately so the peer observes EOF and exits its read loop.
                } catch (Exception e) {
                    serverFailure.set(e);
                }
            }, "peer-node-test-server");
            serverThread.start();

            PeerNode.runTcpPeer("localhost", server.getLocalPort(), engine);
            serverThread.join(2_000);
        }

        assertNull(serverFailure.get());
        assertTrue(engine.isShutdown());
        assertTrue(engine.awaitTermination(2, TimeUnit.SECONDS));
    }

    @Test
    void tcpRuntimeShutsDownEngineWhenConnectionCannotBeOpened() throws Exception {
        int closedPort;
        try (ServerSocket reserved = new ServerSocket(0)) {
            closedPort = reserved.getLocalPort();
        }

        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");

        PeerNode.runTcpPeer("localhost", closedPort, engine);

        assertTrue(engine.isShutdown());
        assertTrue(engine.awaitTermination(2, TimeUnit.SECONDS));
    }

    @Test
    void tcpRuntimeKeepsReadingAfterMalformedMessageUntilDisconnect() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        Thread peerThread;
        String response;

        try (ServerSocket server = new ServerSocket(0)) {
            peerThread = new Thread(
                    () -> PeerNode.runTcpPeer("localhost", server.getLocalPort(), engine),
                    "peer-node-malformed-message-test");
            peerThread.start();

            try (Socket socket = server.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                socket.setSoTimeout(2_000);
                out.println("{\"type\":\"UNKNOWN\"}");
                out.println(new Gson().toJson(new PingMessage("coordinator", "2026-06-13T00:00:00Z")));
                response = in.readLine();
            }
        }

        peerThread.join(2_000);
        assertFalse(peerThread.isAlive());
        assertNotNull(response);
        assertTrue(response.contains("\"type\":\"PONG\""));
        assertTrue(engine.isShutdown());
        assertTrue(engine.awaitTermination(2, TimeUnit.SECONDS));
    }

    private static final class FailingWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            throw new IOException("write failed");
        }

        @Override
        public void flush() throws IOException {
            throw new IOException("flush failed");
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingBrokerTransport implements BrokerTransport {
        private final boolean publishConfirmed;
        private OutboundTransportMessage publishedMessage;

        private RecordingBrokerTransport(boolean publishConfirmed) {
            this.publishConfirmed = publishConfirmed;
        }

        @Override
        public void declareTopology() {
        }

        @Override
        public boolean publish(OutboundTransportMessage message) {
            this.publishedMessage = message;
            return publishConfirmed;
        }

        @Override
        public String subscribe(TransportRoute route, TransportMessageHandler handler) {
            return "consumer";
        }

        @Override
        public void cancel(String consumerTag) {
        }

        @Override
        public void close() {
        }
    }

    private static final class CapturingClientPlugin implements ClientJobPlugin {
        private List<Path> inputPaths;
        private String parameter;

        @Override
        public String taskType() {
            return "TEXT_ANALYSIS";
        }

        @Override
        public String displayName() {
            return "Text";
        }

        @Override
        public List<String> supportedInputExtensions() {
            return List.of("txt");
        }

        @Override
        public List<String> parameterOptions() {
            return List.of("CSV", "JSON");
        }

        @Override
        public String defaultParameter() {
            return "CSV";
        }

        @Override
        public List<Object> buildPayloads(List<Path> inputPaths, String parameter) {
            this.inputPaths = List.copyOf(inputPaths);
            this.parameter = parameter;
            return inputPaths.stream()
                    .map(path -> "payload:" + path + ":" + parameter)
                    .map(Object.class::cast)
                    .toList();
        }

        @Override
        public void saveResults(List<Object> results, Path outputDir) {
        }
    }
}
