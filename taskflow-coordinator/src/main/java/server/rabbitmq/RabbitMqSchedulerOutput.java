package server.rabbitmq;

import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.registry.PeerInfo;
import server.scheduler.BrokerOutboxPublisher;
import server.scheduler.SchedulerOutput;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRuntimeDefaults;

public class RabbitMqSchedulerOutput implements SchedulerOutput, BrokerOutboxPublisher {
    private final BrokerTransport transport;

    public RabbitMqSchedulerOutput(BrokerTransport transport) {
        this.transport = transport;
    }

    @Override
    public void sendTask(PeerInfo peer, TaskAssignMessage message) throws Exception {
        BrokerOutboxStore.OutboxMessage outboxMessage = taskAssignmentOutboxMessage(peer, message);
        if (!publishOutboxMessage(outboxMessage)) {
            throw new IllegalStateException("Task assignment was not routed to peer " + peer.getNodeId());
        }
    }

    @Override
    public boolean sendJobResult(String requesterNodeId, JobResultMessage message) throws Exception {
        if (requesterNodeId == null || requesterNodeId.isBlank()) {
            return false;
        }
        return publishOutboxMessage(jobResultOutboxMessage(requesterNodeId, message));
    }

    @Override
    public BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(PeerInfo peer, TaskAssignMessage message) {
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
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.TASK_ASSIGN,
                peerNodeId,
                RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID,
                brokerMessage
        );
    }

    @Override
    public BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(String requesterNodeId, JobResultMessage message) {
        if (requesterNodeId == null || requesterNodeId.isBlank()) {
            throw new IllegalArgumentException("requesterNodeId is required");
        }
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.JOB_RESULT,
                requesterNodeId,
                RabbitMqRuntimeDefaults.COORDINATOR_NODE_ID,
                message
        );
    }

    @Override
    public boolean publishOutbox(BrokerOutboxStore.OutboxRecord record) throws Exception {
        if (record == null) {
            return false;
        }
        return publishOutboxMessage(record.message());
    }

    private boolean publishOutboxMessage(BrokerOutboxStore.OutboxMessage message) throws Exception {
        if (message.peerNodeId() == null || message.peerNodeId().isBlank()) {
            return transport.publish(new OutboundTransportMessage(
                    message.route(),
                    message.fromNodeId(),
                    message.message()
            ));
        }
        return transport.publishToPeer(
                message.route(),
                message.peerNodeId(),
                new OutboundTransportMessage(
                        message.route(),
                        message.fromNodeId(),
                        message.message()
                )
        );
    }
}
