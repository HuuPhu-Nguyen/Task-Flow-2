package server.concreteJobs.text;

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
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new TextAnalysisJob(message.getJobId(), requesterId, message.getParameter());
    }
}
