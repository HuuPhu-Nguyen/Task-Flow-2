package server.rabbitmq;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.registry.PeerInfo;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqSchedulerOutputTest {

    @Test
    void sendsTaskAssignmentsToSelectedPeerRoute() throws Exception {
        CapturingBrokerTransport transport = new CapturingBrokerTransport();
        RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(transport);

        output.sendTask(
                new PeerInfo("peer-1"),
                new TaskAssignMessage(
                        "COORDINATOR",
                        "2026-06-04T00:00:00Z",
                        "task-1",
                        "job-1",
                        "IMAGE_CONVERSION",
                        "payload",
                        "png"
                )
        );

        assertEquals(TransportRoute.TASK_ASSIGN, transport.peerRoute);
        assertEquals("peer-1", transport.peerNodeId);
        TaskAssignMessage routed = assertInstanceOf(TaskAssignMessage.class, transport.peerMessage.message());
        assertEquals("peer-1", routed.getNodeId());
        assertEquals("task-1", routed.getTaskId());
    }

    @Test
    void sendsJobResultsToRequesterPeerRoute() throws Exception {
        CapturingBrokerTransport transport = new CapturingBrokerTransport();
        RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(transport);

        boolean sent = output.sendJobResult(
                "requester-1",
                new JobResultMessage(
                        "COORDINATOR",
                        "2026-06-04T00:00:00Z",
                        "job-1",
                        "IMAGE_CONVERSION",
                        true,
                        List.of()
                )
        );

        assertTrue(sent);
        assertEquals(TransportRoute.JOB_RESULT, transport.peerRoute);
        assertEquals("requester-1", transport.peerNodeId);
        assertInstanceOf(JobResultMessage.class, transport.peerMessage.message());
    }

    @Test
    void refusesBlankRequesterForJobResult() throws Exception {
        CapturingBrokerTransport transport = new CapturingBrokerTransport();
        RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(transport);

        boolean sent = output.sendJobResult(
                " ",
                new JobResultMessage(
                        "COORDINATOR",
                        "2026-06-04T00:00:00Z",
                        "job-1",
                        "IMAGE_CONVERSION",
                        true,
                        List.of()
                )
        );

        assertFalse(sent);
    }

    @Test
    void returnsFalseWhenJobResultPublishIsUnroutable() throws Exception {
        CapturingBrokerTransport transport = new CapturingBrokerTransport(false);
        RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(transport);

        boolean sent = output.sendJobResult(
                "requester-1",
                new JobResultMessage(
                        "COORDINATOR",
                        "2026-06-04T00:00:00Z",
                        "job-1",
                        "IMAGE_CONVERSION",
                        true,
                        List.of()
                )
        );

        assertFalse(sent);
    }

    @Test
    void throwsWhenTaskAssignmentPublishIsUnroutable() {
        CapturingBrokerTransport transport = new CapturingBrokerTransport(false);
        RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(transport);

        assertThrows(IllegalStateException.class, () -> output.sendTask(
                new PeerInfo("peer-1"),
                new TaskAssignMessage(
                        "COORDINATOR",
                        "2026-06-04T00:00:00Z",
                        "task-1",
                        "job-1",
                        "IMAGE_CONVERSION",
                        "payload",
                        "png"
                )
        ));
    }

    private static class CapturingBrokerTransport implements BrokerTransport {
        private final boolean peerPublishRoutable;
        private TransportRoute peerRoute;
        private String peerNodeId;
        private OutboundTransportMessage peerMessage;

        private CapturingBrokerTransport() {
            this(true);
        }

        private CapturingBrokerTransport(boolean peerPublishRoutable) {
            this.peerPublishRoutable = peerPublishRoutable;
        }

        @Override
        public void declareTopology() {
        }

        @Override
        public boolean publish(OutboundTransportMessage message) {
            throw new AssertionError("Expected peer-targeted publish.");
        }

        @Override
        public boolean publishToPeer(TransportRoute route,
                                     String peerNodeId,
                                     OutboundTransportMessage message) {
            this.peerRoute = route;
            this.peerNodeId = peerNodeId;
            this.peerMessage = message;
            return peerPublishRoutable;
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
}
