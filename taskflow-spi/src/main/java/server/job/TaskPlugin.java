package server.job;

import protocol.JobSubmitMessage;

public interface TaskPlugin {
    String taskType();

    EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId);
}
