package transport.rabbitmq;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.MessageValidationException;
import protocol.MessageValidator;
import protocol.MessageType;
import protocol.PayloadLimits;
import protocol.PongMessage;
import protocol.ProtocolVersions;
import protocol.RequesterIdentity;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                3,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
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
        assertEquals(3, decodedMessage.getAttemptNumber());
        assertEquals("550e8400-e29b-41d4-a716-446655440000", decodedMessage.getAssignmentId());
        assertEquals(1_780_000_000_000L, decodedMessage.getLeaseExpiresAtEpochMillis());
        assertEquals("png", decodedMessage.getParameter());
    }

    @Test
    void roundTripsVersionTwoTaskResultMessages() {
        TaskResultMessage message = new TaskResultMessage(
                "peer-1",
                "2026-07-22T06:00:00Z",
                "task-1",
                "job-1",
                3,
                "550e8400-e29b-41d4-a716-446655440000",
                "result",
                true,
                null
        );

        InboundTransportMessage decoded = decode(new OutboundTransportMessage(
                TransportRoute.TASK_RESULT,
                "peer-1",
                message
        ));

        TaskResultMessage decodedMessage = assertInstanceOf(TaskResultMessage.class, decoded.message());
        assertEquals(ProtocolVersions.ASSIGNMENT_IDENTITY, decodedMessage.getProtocolVersion());
        assertEquals(3, decodedMessage.getAttemptNumber());
        assertEquals("550e8400-e29b-41d4-a716-446655440000", decodedMessage.getAssignmentId());
        assertEquals("result", decodedMessage.getResultPayload());
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
    void roundTripsVersionThreeCapacityPongInsideVersionTwoEnvelope() {
        PongMessage message = new PongMessage(
                "peer-1",
                "2026-07-26T00:00:00Z",
                List.of("TEXT_ANALYSIS", "IMAGE_CONVERSION"),
                "550e8400-e29b-41d4-a716-446655440099",
                7L,
                8,
                5,
                Map.of("TEXT_ANALYSIS", 4, "IMAGE_CONVERSION", 2)
        );

        byte[] encoded = codec.encode(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                message
        ));
        JsonObject envelope = JsonParser.parseString(
                new String(encoded, StandardCharsets.UTF_8)
        ).getAsJsonObject();
        InboundTransportMessage decoded = codec.decode(
                encoded,
                TransportRoute.HEARTBEAT,
                new NoopAcknowledgement()
        );
        PongMessage decodedPong =
                assertInstanceOf(PongMessage.class, decoded.message());

        assertEquals(
                ProtocolVersions.ENVELOPE_CURRENT,
                envelope.get(ProtocolVersions.FIELD_NAME).getAsInt()
        );
        assertEquals(
                ProtocolVersions.CAPACITY_ADVERTISEMENT,
                envelope.getAsJsonObject("message")
                        .get(ProtocolVersions.FIELD_NAME)
                        .getAsInt()
        );
        assertEquals(7L, decodedPong.getCapacitySnapshotSequence());
        assertEquals(8, decodedPong.getTotalCapacityUnits());
        assertEquals(5, decodedPong.getAvailableCapacityUnits());
        assertEquals(
                Map.of("TEXT_ANALYSIS", 4, "IMAGE_CONVERSION", 2),
                decodedPong.getMaxConcurrencyByTaskType()
        );
    }

    @Test
    void encodesProtocolVersionOnEnvelopeAndMessage() {
        byte[] body = codec.encode(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                new PongMessage("peer-1", "2026-06-04T00:00:00Z")
        ));

        JsonObject envelope = JsonParser.parseString(new String(body, StandardCharsets.UTF_8)).getAsJsonObject();

        assertEquals(ProtocolVersions.CURRENT, envelope.get(ProtocolVersions.FIELD_NAME).getAsInt());
        assertEquals(ProtocolVersions.CURRENT,
                envelope.getAsJsonObject("message").get(ProtocolVersions.FIELD_NAME).getAsInt());
    }

    @Test
    void decodesLegacyEnvelopeAndMessageWithoutProtocolVersion() {
        JsonObject envelope = envelope(new PongMessage("peer-1", "2026-06-04T00:00:00Z"));
        envelope.remove(ProtocolVersions.FIELD_NAME);
        envelope.getAsJsonObject("message").remove(ProtocolVersions.FIELD_NAME);

        InboundTransportMessage decoded = codec.decode(body(envelope), TransportRoute.HEARTBEAT,
                new NoopAcknowledgement());

        assertEquals(TransportRoute.HEARTBEAT, decoded.route());
        PongMessage decodedMessage = assertInstanceOf(PongMessage.class, decoded.message());
        assertEquals(MessageType.PONG, decodedMessage.getType());
        assertEquals(ProtocolVersions.LEGACY, decodedMessage.getProtocolVersion());
        assertEquals("peer-1", decodedMessage.getNodeId());
    }

    @Test
    void rejectsUnsupportedFutureEnvelopeProtocolVersionWithClearError() {
        JsonObject envelope = envelope(new PongMessage("peer-1", "2026-06-04T00:00:00Z"));
        envelope.addProperty(ProtocolVersions.FIELD_NAME, ProtocolVersions.CURRENT + 1);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> codec.decode(body(envelope), TransportRoute.HEARTBEAT, new NoopAcknowledgement()));

        assertEquals("RabbitMQ envelope uses unsupported TaskFlow protocolVersion 3; "
                + "supported versions are 0 through 2.", error.getMessage());
    }

    @Test
    void rejectsUnsupportedFutureMessageProtocolVersionWithClearError() {
        JsonObject envelope = envelope(new PongMessage("peer-1", "2026-06-04T00:00:00Z"));
        envelope.getAsJsonObject("message")
                .addProperty(
                        ProtocolVersions.FIELD_NAME,
                        ProtocolVersions.MAX_MESSAGE_SUPPORTED + 1
                );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> codec.decode(body(envelope), TransportRoute.HEARTBEAT, new NoopAcknowledgement()));

        assertEquals("RabbitMQ message uses unsupported TaskFlow protocolVersion 4; "
                + "supported message versions are 0 through 3.", error.getMessage());
    }

    @Test
    void rejectsVersionThreeForNonPongMessages() {
        JobResultRequestMessage request = new JobResultRequestMessage(
                "requester-1",
                "2026-07-26T00:00:00Z",
                "job-1",
                "token-1"
        );
        JsonObject envelope = JsonParser.parseString(new String(
                codec.encode(new OutboundTransportMessage(
                        TransportRoute.JOB_SUBMIT,
                        "requester-1",
                        request
                )),
                StandardCharsets.UTF_8
        )).getAsJsonObject();
        envelope.getAsJsonObject("message").addProperty(
                ProtocolVersions.FIELD_NAME,
                ProtocolVersions.CAPACITY_ADVERTISEMENT
        );

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> codec.decode(
                        body(envelope),
                        TransportRoute.JOB_SUBMIT,
                        new NoopAcknowledgement()
                )
        );

        assertEquals(
                MessageValidator.REASON_UNSUPPORTED_PROTOCOL_VERSION,
                error.reasonCode()
        );
    }

    @Test
    void rejectsLegacyTaskResultWithStructuredNonRequeueReason() {
        TaskResultMessage result = new TaskResultMessage(
                "peer-1",
                "2026-07-22T06:00:00Z",
                "task-1",
                "job-1",
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                "result",
                true,
                null
        );
        JsonObject envelope = JsonParser.parseString(new String(codec.encode(
                new OutboundTransportMessage(TransportRoute.TASK_RESULT, "peer-1", result)
        ), StandardCharsets.UTF_8)).getAsJsonObject();
        envelope.getAsJsonObject("message")
                .addProperty(ProtocolVersions.FIELD_NAME, ProtocolVersions.VERSION_1);

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> codec.decode(body(envelope), TransportRoute.TASK_RESULT, new NoopAcknowledgement())
        );

        assertEquals(MessageValidator.REASON_ASSIGNMENT_PROTOCOL_V2_REQUIRED, error.reasonCode());
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

    @Test
    void rejectsInvalidEnvelopePeerIdsBeforeEncode() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> codec.encode(new OutboundTransportMessage(
                        TransportRoute.HEARTBEAT,
                        "peer/unsafe",
                        new PongMessage("peer-1", "2026-06-04T00:00:00Z")
                ))
        );

        assertEquals("RabbitMQ envelope fromNodeId contains unsupported characters.", error.getMessage());
    }

    @Test
    void rejectsInvalidDecodedJobIds() {
        JsonObject envelope = new JsonObject();
        envelope.addProperty(ProtocolVersions.FIELD_NAME, ProtocolVersions.CURRENT);
        envelope.addProperty("route", TransportRoute.JOB_SUBMIT.name());
        envelope.addProperty("fromNodeId", "requester-1");
        envelope.add("message", JsonParser.parseString("""
                {
                  "protocolVersion":1,
                  "type":"JOB_RESULT_REQUEST",
                  "nodeId":"requester-1",
                  "time":"2026-07-04T00:00:00Z",
                  "jobId":"../job",
                  "requesterToken":"token"
                }
                """).getAsJsonObject());

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> codec.decode(body(envelope), TransportRoute.JOB_SUBMIT, new NoopAcknowledgement())
        );

        assertTrue(error.getMessage().contains("Job id may contain only"));
    }

    @Test
    void rejectsOversizeOutboundResults() {
        System.setProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY, "48");
        try {
            JobResultMessage result = new JobResultMessage(
                    "COORDINATOR",
                    "2026-07-04T00:00:00Z",
                    "job-oversize-result",
                    "TEXT_ANALYSIS",
                    true,
                    List.of("this-result-is-longer-than-the-test-limit")
            );

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> codec.encode(new OutboundTransportMessage(
                            TransportRoute.JOB_RESULT,
                            "COORDINATOR",
                            result
                    ))
            );

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_RESULT_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY);
        }
    }

    private InboundTransportMessage decode(OutboundTransportMessage outbound) {
        return codec.decode(codec.encode(outbound), outbound.route(), new NoopAcknowledgement());
    }

    private JsonObject envelope(PongMessage message) {
        byte[] body = codec.encode(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                message
        ));
        return JsonParser.parseString(new String(body, StandardCharsets.UTF_8)).getAsJsonObject();
    }

    private byte[] body(JsonObject envelope) {
        return envelope.toString().getBytes(StandardCharsets.UTF_8);
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
