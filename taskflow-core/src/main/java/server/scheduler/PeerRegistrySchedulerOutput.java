package server.scheduler;

import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;

public class PeerRegistrySchedulerOutput implements SchedulerOutput {
    private final PeerRegistry registry;

    public PeerRegistrySchedulerOutput(PeerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void sendTask(PeerInfo peer, TaskAssignMessage message) throws Exception {
        if (!peer.send(message)) {
            throw new java.io.IOException("Could not send task assignment to peer " + peer.getNodeId());
        }
    }

    @Override
    public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
        PeerInfo requester = registry.get(requesterNodeId);
        if (requester == null) {
            return false;
        }
        return requester.send(message);
    }
}
