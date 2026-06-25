package server.job;

import protocol.JobSubmitMessage;

public interface TaskPlugin {
    String taskType();

    default void validateSubmission(JobSubmitMessage message) {
    }

    EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId);
}
