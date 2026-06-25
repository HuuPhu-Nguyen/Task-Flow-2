package server.concreteJobs.conversion;

import conversion.model.ConversionTaskTypes;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

import java.util.List;

public class VideoTranscodingTaskPlugin implements TaskPlugin {
    private static final List<String> INPUT_EXTENSIONS = List.of(
            ".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv"
    );
    private static final List<String> TARGET_FORMATS = List.of("mp4", "avi", "mkv", "mov", "webm");

    @Override
    public String taskType() {
        return ConversionTaskTypes.VIDEO_TRANSCODING;
    }

    @Override
    public void validateSubmission(JobSubmitMessage message) {
        ConversionTaskValidation.validate(message, TARGET_FORMATS, INPUT_EXTENSIONS);
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new VideoTranscodingJob(message.getJobId(), requesterId, message.getParameter());
    }
}
