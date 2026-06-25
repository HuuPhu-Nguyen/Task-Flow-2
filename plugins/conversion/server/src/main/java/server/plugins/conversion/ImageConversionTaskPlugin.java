package server.plugins.conversion;

import conversion.model.ConversionTaskTypes;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

import java.util.List;

public class ImageConversionTaskPlugin implements TaskPlugin {
    private static final List<String> INPUT_EXTENSIONS = List.of(
            ".png", ".jpg", ".jpeg", ".bmp", ".gif", ".pdf"
    );
    private static final List<String> TARGET_FORMATS = List.of("png", "jpg", "bmp", "gif");

    @Override
    public String taskType() {
        return ConversionTaskTypes.IMAGE_CONVERSION;
    }

    @Override
    public void validateSubmission(JobSubmitMessage message) {
        ConversionTaskValidation.validate(message, TARGET_FORMATS, INPUT_EXTENSIONS);
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new ImageConversionJob(message.getJobId(), requesterId, message.getParameter());
    }
}
