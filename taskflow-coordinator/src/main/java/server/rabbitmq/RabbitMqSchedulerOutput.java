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
        TaskAssignMessage brokerMessage = new TaskAssignMessage(
                peer.getNodeId(),
                java.time.Instant.now().toString(),
                message.getTaskId(),
                message.getJobId(),
                message.getTaskType(),
                message.getPayload(),
                message.getParam()
        );
        transport.publish(new OutboundTransportMessage(
                TransportRoute.TASK_ASSIGN,
                RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID,
                brokerMessage
        ));
    }

    @Override
    public boolean sendJobResult(String requesterNodeId, JobResultMessage message) throws Exception {
        transport.publish(new OutboundTransportMessage(
                TransportRoute.JOB_RESULT,
                RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID,
                message
        ));
        return true;
    }
}
