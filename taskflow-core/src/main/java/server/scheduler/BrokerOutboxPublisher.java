package server.scheduler;

import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.registry.PeerInfo;

public interface BrokerOutboxPublisher {
    /**
     * Adds transport routing to a task-assignment payload template. In the
     * SQLite outbox path the template may not yet contain assignment identity;
     * the state store enriches and serializes it inside the assignment commit.
     */
    BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(PeerInfo peer, TaskAssignMessage message);

    BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(String requesterNodeId, JobResultMessage message);

    boolean publishOutbox(BrokerOutboxStore.OutboxRecord record) throws Exception;
}
