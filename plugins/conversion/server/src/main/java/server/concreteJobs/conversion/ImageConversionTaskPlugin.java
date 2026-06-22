package server.concreteJobs.conversion;

import conversion.model.ConversionTaskTypes;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

public class ImageConversionTaskPlugin implements TaskPlugin {
    @Override
    public String taskType() {
        return ConversionTaskTypes.IMAGE_CONVERSION;
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new ImageConversionJob(message.getJobId(), requesterId, message.getParameter());
    }
}
