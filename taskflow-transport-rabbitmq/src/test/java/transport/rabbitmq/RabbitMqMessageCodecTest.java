package transport.rabbitmq;

import org.junit.jupiter.api.Test;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.MessageType;
import protocol.PongMessage;
import protocol.RequesterIdentity;
import protocol.TaskAssignMessage;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class RabbitMqMessageCodecTest {
    private final RabbitMqMessageCodec codec = new RabbitMqMessageCodec();

    @Test
    void roundTripsJobSubmitMessages() {
        List<Object> payloads = new ArrayList<>();
        payloads.add(new TestPayload("input.png", "abc123"));
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        String time = "2026-06-02T00:00:00Z";
        String signature = RequesterIdentity.signJobSubmit(
                identity.privateKey(),
                "client-1",
                time,
                "job-1",
                "IMAGE_CONVERSION",
                "png",
                "submit-token"
        );
        JobSubmitMessage message = new JobSubmitMessage(
                "client-1",
                time,
                "job-1",
                "IMAGE_CONVERSION",
                payloads,
                "png",
                "submit-token",
                identity.publicKey(),
                signature
        );

        InboundTransportMessage decoded = decode(new OutboundTransportMessage(
                TransportRoute.JOB_SUBMIT,
                "client-1",
                message
        ));

        assertEquals(TransportRoute.JOB_SUBMIT, decoded.route());
        assertEquals("client-1", decoded.fromNodeId());
        JobSubmitMessage decodedMessage = assertInstanceOf(JobSubmitMessage.class, decoded.message());
        assertEquals(MessageType.JOB_SUBMIT, decodedMessage.getType());
        assertEquals("job-1", decodedMessage.getJobId());
        assertEquals("IMAGE_CONVERSION", decodedMessage.getTaskType());
        assertEquals("png", decodedMessage.getParameter());
        assertEquals("submit-token", decodedMessage.getRequesterToken());
        assertEquals(identity.publicKey(), decodedMessage.getRequesterPublicKey());
        assertEquals(signature, decodedMessage.getRequesterSignature());
    }

    @Test
    void roundTripsTaskAssignMessages() {
        TaskAssignMessage message = new TaskAssignMessage(
                "coordinator",
                "2026-06-02T00:00:00Z",
                "task-1",
                "job-1",
                "IMAGE_CONVERSION",
                new TestPayload("input.png", "abc123"),
                "png"
        );

        InboundTransportMessage decoded = decode(new OutboundTransportMessage(
                TransportRoute.TASK_ASSIGN,
                "coordinator",
                message
        ));

        assertEquals(TransportRoute.TASK_ASSIGN, decoded.route());
        TaskAssignMessage decodedMessage = assertInstanceOf(TaskAssignMessage.class, decoded.message());
        assertEquals("task-1", decodedMessage.getTaskId());
        assertEquals("job-1", decodedMessage.getJobId());
        assertEquals("IMAGE_CONVERSION", decodedMessage.getTaskType());
        assertEquals("png", decodedMessage.getParam());
    }

    @Test
    void roundTripsHeartbeatMessages() {
        PongMessage message = new PongMessage("peer-1", "2026-06-04T00:00:00Z");

        InboundTransportMessage decoded = decode(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                message
        ));

        assertEquals(TransportRoute.HEARTBEAT, decoded.route());
        assertEquals("peer-1", decoded.fromNodeId());
        PongMessage decodedMessage = assertInstanceOf(PongMessage.class, decoded.message());
        assertEquals(MessageType.PONG, decodedMessage.getType());
        assertEquals("peer-1", decodedMessage.getNodeId());
    }

    @Test
    void roundTripsJobResultRequests() {
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        String time = "2026-06-25T00:00:00Z";
        String signature = RequesterIdentity.signJobResultRequest(
                identity.privateKey(),
                "requester-1",
                time,
                "job-123",
                "request-token"
        );
        JobResultRequestMessage message = new JobResultRequestMessage(
                "requester-1",
                time,
                "job-123",
                "request-token",
                identity.publicKey(),
                signature
        );

        InboundTransportMessage decoded = decode(new OutboundTransportMessage(
                TransportRoute.JOB_SUBMIT,
                "requester-1",
                message
        ));

        assertEquals(TransportRoute.JOB_SUBMIT, decoded.route());
        assertEquals("requester-1", decoded.fromNodeId());
        JobResultRequestMessage decodedMessage = assertInstanceOf(JobResultRequestMessage.class, decoded.message());
        assertEquals(MessageType.JOB_RESULT_REQUEST, decodedMessage.getType());
        assertEquals("job-123", decodedMessage.getJobId());
        assertEquals("request-token", decodedMessage.getRequesterToken());
        assertEquals(identity.publicKey(), decodedMessage.getRequesterPublicKey());
        assertEquals(signature, decodedMessage.getRequesterSignature());
    }

    private InboundTransportMessage decode(OutboundTransportMessage outbound) {
        return codec.decode(codec.encode(outbound), outbound.route(), new NoopAcknowledgement());
    }

    private record TestPayload(String fileName, String base64Data) {
    }

    private static class NoopAcknowledgement implements TransportAcknowledgement {
        @Override
        public void ack() {
        }

        @Override
        public void requeue() {
        }

        @Override
        public void reject() {
        }
    }
}
