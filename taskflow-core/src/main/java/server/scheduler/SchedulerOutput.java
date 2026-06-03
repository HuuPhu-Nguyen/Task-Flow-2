package server.scheduler;

import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.registry.PeerInfo;

public interface SchedulerOutput {
    void sendTask(PeerInfo peer, TaskAssignMessage message) throws Exception;

    boolean sendJobResult(String requesterNodeId, JobResultMessage message) throws Exception;
}
