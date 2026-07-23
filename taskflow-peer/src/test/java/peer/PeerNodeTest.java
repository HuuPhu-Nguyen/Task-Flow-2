package peer;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PingMessage;
import protocol.RequesterIdentity;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;
import transport.TransientDeliveryException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerNodeTest {

    @Test
    void usageDescribesParticipantRolesAndProfileSpecificPackages() {
        String usage = PeerNode.usage();

        assertTrue(usage.contains("participant runtime"));
        assertTrue(usage.contains("requester role"));
        assertTrue(usage.contains("executor role"));
        assertTrue(usage.contains("taskflow-peer-<version>-combined-runtime.jar"));
        assertTrue(usage.contains("taskflow-peer-<version>-submitter-runtime.jar"));
        assertTrue(usage.contains("taskflow-peer-<version>-executor-runtime.jar"));
        assertTrue(usage.contains("submit <task-type> <parameter> <file> [file...]"));
        assertTrue(usage.contains("dlq <inspect|redrive|quarantine|discard> [count]"));
        assertTrue(usage.contains("RabbitMQ participant runtime"));
        assertFalse(usage.contains("TASKFLOW_TRANSPORT"));
        assertFalse(usage.contains("TCP"));
    }

    @Test
    void helpDetectionDoesNotStartTheBrokerRuntime() {
        assertTrue(PeerNode.isHelpRequested(new String[] {"--help"}));
        assertTrue(PeerNode.isHelpRequested(new String[] {"-h"}));
        assertFalse(PeerNode.isHelpRequested(new String[0]));
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

        TransientDeliveryException error = assertThrows(TransientDeliveryException.class,
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
        assertTrue(jobId.matches(
                "JOB_peer-submit_\\d+_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
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

        TransientDeliveryException error = assertThrows(
                TransientDeliveryException.class,
                () -> RabbitMqPeerNode.submitJob(
                        "peer-submit",
                        transport,
                        new String[] {"submit", "TEXT_ANALYSIS", "csv", "notes.txt"},
                        Map.of("TEXT_ANALYSIS", new CapturingClientPlugin())
                ));

        assertTrue(error.getMessage().contains("Job submit publish was not confirmed"));
        assertEquals(TransportRoute.JOB_SUBMIT, transport.publishedMessage.route());
    }

    @Test
    void rabbitMqSubmitCommandPropagatesBrokerPublishException() {
        RecordingBrokerTransport transport = new RecordingBrokerTransport(true);
        transport.publishFailure = new IOException("broker outage");

        IOException error = assertThrows(IOException.class, () -> RabbitMqPeerNode.submitJob(
                "peer-submit",
                transport,
                new String[] {"submit", "TEXT_ANALYSIS", "csv", "notes.txt"},
                Map.of("TEXT_ANALYSIS", new CapturingClientPlugin())
        ));

        assertEquals("broker outage", error.getMessage());
        assertEquals(TransportRoute.JOB_SUBMIT, transport.publishedMessage.route());
    }

    @Test
    void rabbitMqResultHandlingSavesThroughClientPlugin() throws Exception {
        CapturingClientPlugin plugin = new CapturingClientPlugin();
        JobResultMessage result = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-result",
                "TEXT_ANALYSIS",
                true,
                List.of("first", "second"));

        Path outputDir = RabbitMqPeerNode.writeJobResults(
                result,
                Map.of("TEXT_ANALYSIS", plugin));

        assertEquals(Path.of("target", "rabbitmq-results", "job-result"), outputDir);
        assertEquals(List.of("first", "second"), plugin.savedResults);
        assertEquals(outputDir, plugin.outputDir);
    }

    @Test
    void rabbitMqResultHandlingDispatchesWholeResultToClientPlugin() throws Exception {
        SemanticClientPlugin plugin = new SemanticClientPlugin();
        JobResultMessage result = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-result",
                "TEXT_ANALYSIS",
                true,
                Map.of("totalWords", 42),
                List.of("task-result"),
                null);

        Path outputDir = RabbitMqPeerNode.writeJobResults(
                result,
                Map.of("TEXT_ANALYSIS", plugin));

        assertEquals(Path.of("target", "rabbitmq-results", "job-result"), outputDir);
        assertEquals(Map.of("totalWords", 42), plugin.handledPayload);
        assertEquals(outputDir, plugin.outputDir);
        assertNull(plugin.savedResults);
    }

    private static final class RecordingBrokerTransport implements BrokerTransport {
        private final boolean publishConfirmed;
        private OutboundTransportMessage publishedMessage;
        private IOException publishFailure;

        private RecordingBrokerTransport(boolean publishConfirmed) {
            this.publishConfirmed = publishConfirmed;
        }

        @Override
        public void declareTopology() {
        }

        @Override
        public boolean publish(OutboundTransportMessage message) throws IOException {
            this.publishedMessage = message;
            if (publishFailure != null) {
                throw publishFailure;
            }
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
        private List<Object> savedResults;
        private Path outputDir;

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
            this.savedResults = List.copyOf(results);
            this.outputDir = outputDir;
        }
    }

    private static final class SemanticClientPlugin implements ClientJobPlugin {
        private Object handledPayload;
        private List<Object> savedResults;
        private Path outputDir;

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
            return List.of("CSV");
        }

        @Override
        public String defaultParameter() {
            return "CSV";
        }

        @Override
        public List<Object> buildPayloads(List<Path> inputPaths, String parameter) {
            return List.of();
        }

        @Override
        public void saveResults(List<Object> results, Path outputDir) {
            this.savedResults = List.copyOf(results);
            this.outputDir = outputDir;
        }

        @Override
        public void handleResult(JobResultMessage result, Path outputDir) {
            this.handledPayload = result.getResultPayload();
            this.outputDir = outputDir;
        }
    }
}
