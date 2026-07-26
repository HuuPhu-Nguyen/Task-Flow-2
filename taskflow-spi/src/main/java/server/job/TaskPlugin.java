package server.job;

import plugin.RetrySafety;
import plugin.TaskResourceProfile;
import protocol.JobSubmitMessage;

public interface TaskPlugin {
    String taskType();

    /**
     * Coordinator-visible mirror of the paired executor processor declaration.
     * Both role artifacts for a task type must return the same stable value.
     */
    RetrySafety retrySafety();

    /**
     * Declares the fixed scalar scheduling cost and diagnostic estimates.
     * The paired executor plugin must return the same immutable value.
     */
    TaskResourceProfile resourceProfile();

    default void validateSubmission(JobSubmitMessage message) {
    }

    EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId);
}
