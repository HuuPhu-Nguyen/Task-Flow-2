package server.plugins.text;

import plugin.RetrySafety;
import plugin.TaskResourceProfile;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;
import text.model.TextAnalysisTaskTypes;

public class TextAnalysisTaskPlugin implements TaskPlugin {
    @Override
    public String taskType() {
        return TextAnalysisTaskTypes.TEXT_ANALYSIS;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    public TaskResourceProfile resourceProfile() {
        return TaskResourceProfile.ofCapacityUnits(1);
    }

    @Override
    public void validateSubmission(JobSubmitMessage message) {
        TextAnalysisTaskValidation.validate(message);
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new TextAnalysisJob(message.getJobId(), requesterId, message.getParameter());
    }
}
