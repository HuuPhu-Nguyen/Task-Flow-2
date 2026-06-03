package server.concreteJobs.conversion;

import peer.engine.TaskProcessor;
import peer.engine.WorkerPlugin;
import peer.processors.ImageConversionProcessor;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

public class ImageConversionTaskPlugin implements TaskPlugin, WorkerPlugin {
    public static final String TYPE = "IMAGE_CONVERSION";

    @Override
    public String taskType() {
        return TYPE;
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new ImageConversionJob(message.getJobId(), requesterId, message.getParameter());
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new ImageConversionProcessor();
    }
}
