package server.job;

import plugin.RetrySafety;
import protocol.JobSubmitMessage;

public interface TaskPlugin {
    String taskType();

    /**
     * Coordinator-visible mirror of the paired executor processor declaration.
     * Both role artifacts for a task type must return the same stable value.
     */
    RetrySafety retrySafety();

    default void validateSubmission(JobSubmitMessage message) {
    }

    EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId);
}
