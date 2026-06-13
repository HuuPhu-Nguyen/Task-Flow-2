package peer;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import peer.engine.PeerExecutionEngine;
import protocol.PingMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    }

    @Test
    void submitJobFailsFastWhenMessageCannotBeWritten() {
        PeerNode node = new PeerNode();
        PrintWriter out = new PrintWriter(new FailingWriter());

        assertThrows(IllegalStateException.class,
                () -> node.submitJob("TEXT_ANALYSIS", List.of("payload"), "summary", out));
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
}
