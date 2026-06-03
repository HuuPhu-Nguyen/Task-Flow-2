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
    public void sendTask(PeerInfo peer, TaskAssignMessage message) {
        peer.send(message);
    }

    @Override
    public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
        PeerInfo requester = registry.get(requesterNodeId);
        if (requester == null) {
            return false;
        }
        requester.send(message);
        return true;
    }
}
