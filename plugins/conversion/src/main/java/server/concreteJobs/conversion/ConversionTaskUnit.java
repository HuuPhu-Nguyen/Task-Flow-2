package server.concreteJobs.conversion;

import protocol.FilePayload;
import server.job.TaskUnit;

public class ConversionTaskUnit extends TaskUnit<FilePayload> {
    private final String targetFormat;

    public ConversionTaskUnit(String taskId, String jobId, FilePayload payload, String targetFormat) {
        super(taskId, jobId, payload);
        this.targetFormat = targetFormat;
    }

    public String getTargetFormat() {
        return targetFormat;
    }
}