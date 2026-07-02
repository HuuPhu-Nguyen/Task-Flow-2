package server.scheduler;

import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.registry.PeerInfo;

public interface BrokerOutboxPublisher {
    BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(PeerInfo peer, TaskAssignMessage message);

    BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(String requesterNodeId, JobResultMessage message);

    boolean publishOutbox(BrokerOutboxStore.OutboxRecord record) throws Exception;
}
