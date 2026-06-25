package server.plugins.conversion;

import com.google.gson.Gson;
import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class VideoTranscodingJob extends EmbarrassinglyParallelJob<FilePayload, FilePayload> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VideoTranscodingJob.class);

    private final String targetFormat;
    private final Gson gson = new Gson();
    private final Map<String, FilePayload> results = new ConcurrentHashMap<>();

    public VideoTranscodingJob(String jobId, String requesterId, String targetFormat) {
        super(jobId, requesterId, ConversionTaskTypes.VIDEO_TRANSCODING);
        this.targetFormat = targetFormat;
    }

    @Override
    public void initializeTasks(JobSubmitMessage message) {
        List<Object> payloads = message.getTaskPayloads();
        if (payloads == null) {
            return;
        }

        for (int i = 0; i < payloads.size(); i++) {
            String taskId = "task-" + jobId + "-" + i;
            FilePayload filePayload = gson.fromJson(gson.toJson(payloads.get(i)), FilePayload.class);
            tasks.put(taskId, new ConversionTaskUnit(taskId, jobId, filePayload, targetFormat));
        }
    }

    @Override
    protected void onTaskSuccess(TaskUnit<FilePayload> task, FilePayload resultData) {
        results.put(task.getTaskId(), resultData);
        LOGGER.debug("event=video_task_result_stored job_id={} task_id={}", jobId, task.getTaskId());
    }

    @Override
    public List<Object> aggregateAndSendResult() {
        LOGGER.info("event=video_job_packaging job_id={} result_count={}", jobId, results.size());
        return tasks.keySet().stream()
                .sorted(Comparator.comparingInt(this::taskIndex))
                .map(results::get)
                .filter(Objects::nonNull)
                .map(Object.class::cast)
                .toList();
    }

    @Override
    protected FilePayload parseResult(Object rawData) {
        return gson.fromJson(gson.toJson(rawData), FilePayload.class);
    }

    @Override
    public TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task) {
        return new TaskAssignMessage(
                "COORDINATOR",
                Instant.now().toString(),
                task.getTaskId(),
                jobId,
                ConversionTaskTypes.VIDEO_TRANSCODING,
                task.getPayload(),
                targetFormat);
    }

    private int taskIndex(String taskId) {
        int marker = taskId.lastIndexOf('-');
        if (marker < 0 || marker == taskId.length() - 1) {
            return Integer.MAX_VALUE;
        }
        try {
            return Integer.parseInt(taskId.substring(marker + 1));
        } catch (NumberFormatException ignored) {
            return Integer.MAX_VALUE;
        }
    }
}
