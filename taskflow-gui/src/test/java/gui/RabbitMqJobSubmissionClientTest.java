package gui;

import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import protocol.RequesterIdentity;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqJobSubmissionClientTest {
    @Test
    void newJobIdIncludesSanitizedPeerIdAndFullUuid() {
        RabbitMqJobSubmissionClient client = new RabbitMqJobSubmissionClient(
                "gui peer/1",
                new RecordingRequesterTokenStore());

        String jobId = client.newJobId();

        assertTrue(jobId.matches(
                "JOB_gui_peer_1_\\d+_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
    }

    @Test
    void publishesSignedJobSubmitWithRequesterToken() {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingRequesterTokenStore tokenStore = new RecordingRequesterTokenStore();
        RabbitMqJobSubmissionClient client = new RabbitMqJobSubmissionClient("peer-1", tokenStore);

        client.submitJob(
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                new FakeRabbitMqConnection("peer-1", transport));

        assertEquals(1, transport.published.size());
        OutboundTransportMessage outbound = transport.published.getFirst();
        assertEquals(TransportRoute.JOB_SUBMIT, outbound.route());
        assertEquals("peer-1", outbound.fromNodeId());
        JobSubmitMessage submit = assertInstanceOf(JobSubmitMessage.class, outbound.message());
        assertEquals("job-1", submit.getJobId());
        assertEquals("TEXT_ANALYSIS", submit.getTaskType());
        assertEquals(List.of("payload"), submit.getTaskPayloads());
        assertEquals("summary", submit.getParameter());
        assertEquals("token-job-1", submit.getRequesterToken());
        assertEquals(Optional.of("token-job-1"), tokenStore.tokenForJob("job-1"));
        assertTrue(RequesterIdentity.verifyJobSubmit(submit));
    }

    @Test
    void uncertainPublishFailureRetainsRequesterTokenForIdempotentReplay() {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        transport.publishResult = false;
        RecordingRequesterTokenStore tokenStore = new RecordingRequesterTokenStore();
        RabbitMqJobSubmissionClient client = new RabbitMqJobSubmissionClient("peer-1", tokenStore);

        assertThrows(IllegalStateException.class, () -> client.submitJob(
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                new FakeRabbitMqConnection("peer-1", transport)));

        assertEquals(Optional.of("token-job-1"), tokenStore.tokenForJob("job-1"));
    }

    @Test
    void rejectsNonRabbitMqConnection() {
        RabbitMqJobSubmissionClient client = new RabbitMqJobSubmissionClient(
                "peer-1",
                new RecordingRequesterTokenStore());

        assertThrows(IllegalStateException.class, () -> client.submitJob(
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                new TestCoordinatorConnection(new PrintWriter(new StringWriter(), true))));
    }

    @Test
    void rejectsConnectionForDifferentPeerId() {
        RabbitMqJobSubmissionClient client = new RabbitMqJobSubmissionClient(
                "peer-1",
                new RecordingRequesterTokenStore());

        assertThrows(IllegalStateException.class, () -> client.submitJob(
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                new FakeRabbitMqConnection("peer-2", new RecordingBrokerTransport())));
    }

    @Test
    void resultRequestsAreNotSupportedForRabbitMqGuiYet() {
        RabbitMqJobSubmissionClient client = new RabbitMqJobSubmissionClient(
                "peer-1",
                new RecordingRequesterTokenStore());

        assertThrows(UnsupportedOperationException.class, () ->
                client.requestJobResult("job-1", new FakeRabbitMqConnection("peer-1", new RecordingBrokerTransport())));
    }

    private static final class FakeRabbitMqConnection implements RabbitMqBrokerConnection {
        private final String peerId;
        private final BrokerTransport transport;
        private boolean open = true;

        private FakeRabbitMqConnection(String peerId, BrokerTransport transport) {
            this.peerId = peerId;
            this.transport = transport;
        }

        @Override
        public BrokerTransport transport() {
            return transport;
        }

        @Override
        public String peerId() {
            return peerId;
        }

        @Override
        public PrintWriter writer() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }

    private static final class RecordingBrokerTransport implements BrokerTransport {
        private final List<OutboundTransportMessage> published = new ArrayList<>();
        private boolean publishResult = true;

        @Override
        public void declareTopology() {
        }

        @Override
        public boolean publish(OutboundTransportMessage message) {
            published.add(message);
            return publishResult;
        }

        @Override
        public String subscribe(TransportRoute route, TransportMessageHandler handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel(String consumerTag) {
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingRequesterTokenStore implements GuiRequesterTokenStore {
        private final Map<String, String> tokens = new ConcurrentHashMap<>();
        private final RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();

        @Override
        public RequesterIdentity.Credentials requesterIdentity() {
            return identity;
        }

        @Override
        public String createTokenForJob(String jobId) {
            String token = "token-" + jobId;
            tokens.put(jobId, token);
            return token;
        }

        @Override
        public Optional<String> tokenForJob(String jobId) {
            return Optional.ofNullable(tokens.get(jobId));
        }

        @Override
        public void forgetToken(String jobId) {
            tokens.remove(jobId);
        }
    }
}
