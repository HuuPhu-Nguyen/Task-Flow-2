package server.concreteJobs.conversion;

import conversion.model.ConversionTaskTypes;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

public class VideoTranscodingTaskPlugin implements TaskPlugin {
    @Override
    public String taskType() {
        return ConversionTaskTypes.VIDEO_TRANSCODING;
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new VideoTranscodingJob(message.getJobId(), requesterId, message.getParameter());
    }
}
