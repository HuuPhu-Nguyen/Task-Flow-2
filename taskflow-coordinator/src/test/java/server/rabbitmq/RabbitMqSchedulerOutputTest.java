package server.rabbitmq;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.registry.PeerInfo;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRuntimeDefaults;

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
        assertEquals(RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID, transport.peerMessage.fromNodeId());
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
        assertEquals(RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID, transport.peerMessage.fromNodeId());
        JobResultMessage routed = assertInstanceOf(JobResultMessage.class, transport.peerMessage.message());
        assertEquals("COORDINATOR", routed.getNodeId());
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

    @Test
    void publishesTaskAssignmentOutboxRecordToPeerRoute() throws Exception {
        CapturingBrokerTransport transport = new CapturingBrokerTransport();
        RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(transport);
        BrokerOutboxStore.OutboxMessage message = output.taskAssignmentOutboxMessage(
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

        boolean published = output.publishOutbox(new BrokerOutboxStore.OutboxRecord(
                1L,
                message,
                100L,
                0,
                0L,
                ""
        ));

        assertTrue(published);
        assertEquals(TransportRoute.TASK_ASSIGN, transport.peerRoute);
        assertEquals("peer-1", transport.peerNodeId);
        TaskAssignMessage routed = assertInstanceOf(TaskAssignMessage.class, transport.peerMessage.message());
        assertEquals("task-1", routed.getTaskId());
        assertEquals("peer-1", routed.getNodeId());
    }

    @Test
    void returnsFalseWhenOutboxRecordIsUnroutable() throws Exception {
        CapturingBrokerTransport transport = new CapturingBrokerTransport(false);
        RabbitMqSchedulerOutput output = new RabbitMqSchedulerOutput(transport);
        BrokerOutboxStore.OutboxMessage message = output.jobResultOutboxMessage(
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

        boolean published = output.publishOutbox(new BrokerOutboxStore.OutboxRecord(
                2L,
                message,
                100L,
                0,
                0L,
                ""
        ));

        assertFalse(published);
        assertEquals(TransportRoute.JOB_RESULT, transport.peerRoute);
        assertEquals("requester-1", transport.peerNodeId);
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
