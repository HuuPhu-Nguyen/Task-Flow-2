package transport.rabbitmq;

import org.junit.jupiter.api.Test;
import protocol.FilePayload;
import protocol.JobSubmitMessage;
import protocol.MessageType;
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
        payloads.add(new FilePayload("input.png", "abc123"));
        JobSubmitMessage message = new JobSubmitMessage(
                "client-1",
                "2026-06-02T00:00:00Z",
                "job-1",
                "IMAGE_CONVERSION",
                payloads,
                "png"
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
    }

    @Test
    void roundTripsTaskAssignMessages() {
        TaskAssignMessage message = new TaskAssignMessage(
                "coordinator",
                "2026-06-02T00:00:00Z",
                "task-1",
                "job-1",
                "IMAGE_CONVERSION",
                new FilePayload("input.png", "abc123"),
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

    private InboundTransportMessage decode(OutboundTransportMessage outbound) {
        return codec.decode(codec.encode(outbound), outbound.route(), new NoopAcknowledgement());
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
