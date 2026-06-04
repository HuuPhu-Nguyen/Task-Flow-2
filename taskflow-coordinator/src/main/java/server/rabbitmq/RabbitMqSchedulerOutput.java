package server.rabbitmq;

import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.registry.PeerInfo;
import server.scheduler.SchedulerOutput;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRuntimeDefaults;

public class RabbitMqSchedulerOutput implements SchedulerOutput {
    private final BrokerTransport transport;

    public RabbitMqSchedulerOutput(BrokerTransport transport) {
        this.transport = transport;
    }

    @Override
    public void sendTask(PeerInfo peer, TaskAssignMessage message) throws Exception {
        String peerNodeId = peer.getNodeId();
        TaskAssignMessage brokerMessage = new TaskAssignMessage(
                peerNodeId,
                java.time.Instant.now().toString(),
                message.getTaskId(),
                message.getJobId(),
                message.getTaskType(),
                message.getPayload(),
                message.getParam()
        );
        transport.publishToPeer(
                TransportRoute.TASK_ASSIGN,
                peerNodeId,
                new OutboundTransportMessage(
                        TransportRoute.TASK_ASSIGN,
                        RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID,
                        brokerMessage
                )
        );
    }

    @Override
    public boolean sendJobResult(String requesterNodeId, JobResultMessage message) throws Exception {
        if (requesterNodeId == null || requesterNodeId.isBlank()) {
            return false;
        }
        transport.publishToPeer(
                TransportRoute.JOB_RESULT,
                requesterNodeId,
                new OutboundTransportMessage(
                        TransportRoute.JOB_RESULT,
                        RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID,
                        message
                )
        );
        return true;
    }
}
