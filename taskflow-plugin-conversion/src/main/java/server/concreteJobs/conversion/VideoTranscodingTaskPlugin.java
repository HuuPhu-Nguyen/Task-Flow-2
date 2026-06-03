package server.concreteJobs.conversion;

import peer.engine.TaskProcessor;
import peer.engine.WorkerPlugin;
import peer.processors.VideoTranscodingProcessor;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

public class VideoTranscodingTaskPlugin implements TaskPlugin, WorkerPlugin {
    public static final String TYPE = "VIDEO_TRANSCODING";

    @Override
    public String taskType() {
        return TYPE;
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new VideoTranscodingJob(message.getJobId(), requesterId, message.getParameter());
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new VideoTranscodingProcessor();
    }
}
