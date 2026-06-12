package server.concreteJobs.conversion;

import client.ClientJobPlugin;
import peer.engine.TaskProcessor;
import peer.engine.PeerProcessorPlugin;
import peer.processors.VideoTranscodingProcessor;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

import java.nio.file.Path;
import java.util.List;

public class VideoTranscodingTaskPlugin implements TaskPlugin, PeerProcessorPlugin, ClientJobPlugin {
    public static final String TYPE = "VIDEO_TRANSCODING";

    private static final List<String> INPUT_EXTENSIONS = List.of(
            ".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv"
    );
    private static final List<String> TARGET_FORMATS = List.of("mp4", "avi", "mkv", "mov", "webm");

    @Override
    public String taskType() {
        return TYPE;
    }

    @Override
    public String displayName() {
        return "Video Transcoding";
    }

    @Override
    public List<String> supportedInputExtensions() {
        return INPUT_EXTENSIONS;
    }

    @Override
    public List<String> parameterOptions() {
        return TARGET_FORMATS;
    }

    @Override
    public String defaultParameter() {
        return "mp4";
    }

    @Override
    public List<Object> buildPayloads(List<Path> inputPaths, String parameter) throws Exception {
        normalizeParameter(parameter);
        return ConversionClientPayloads.buildFilePayloads(inputPaths, INPUT_EXTENSIONS);
    }

    @Override
    public void saveResults(List<Object> results, Path outputDir) throws Exception {
        ConversionClientPayloads.saveFilePayloadResults(results, outputDir);
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
