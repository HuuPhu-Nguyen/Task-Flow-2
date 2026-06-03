package server.concreteJobs.conversion;

import com.google.gson.Gson;
import protocol.FilePayload;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ImageConversionJob extends EmbarrassinglyParallelJob<FilePayload, FilePayload> {

    private final String targetFormat;
    private final Gson gson = new Gson();

    // We still need this to hold the actual converted strings
    private final Map<String, FilePayload> results = new ConcurrentHashMap<>();

    public ImageConversionJob(String jobId, String requesterId, String targetFormat) {
        super(jobId, requesterId, "IMAGE_CONVERSION");
        this.targetFormat = targetFormat;
    }

    @Override
    public void initializeTasks(JobSubmitMessage message) {
        List<Object> payloads = message.getTaskPayloads();
        if (payloads == null) return;

        for (int i = 0; i < payloads.size(); i++) {
            String taskId = "task-" + this.getJobId() + "-" + i;

            // Convert the generic Object back to FilePayload
            // This is safer than a hard cast
            Object rawData = payloads.get(i);
            FilePayload filePayload = new Gson().fromJson(new Gson().toJson(rawData), FilePayload.class);

            //Wrap it in task unit
            tasks.put(taskId, new ConversionTaskUnit(taskId, this.getJobId(), filePayload, this.targetFormat));
        }
    }

    /**
     * The parent class calls this ONLY when a result is fresh and valid.
     */
    @Override
    protected void onTaskSuccess(TaskUnit<FilePayload> task, FilePayload resultData) {
        // Just store the data. The parent handles the "is it done?" logic.
        results.put(task.getTaskId(), resultData);
        System.out.println("  Task stored: " + task.getTaskId());
    }

    @Override
    public List<Object> aggregateAndSendResult() {
        System.out.println("Job [" + jobId + "] is finished. Packaging " + results.size() + " files.");
        return tasks.keySet().stream()
                .sorted(Comparator.comparingInt(this::taskIndex))
                .map(results::get)
                .filter(Objects::nonNull)
                .map(Object.class::cast)
                .toList();
    }

    @Override
    protected FilePayload parseResult(Object rawData) {
        // This turns the payload from the TaskResultMessage into a FilePayload
        return gson.fromJson(gson.toJson(rawData), FilePayload.class);
    }

    @Override
    public TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task) {
        return new TaskAssignMessage(
                "COORDINATOR",
                java.time.Instant.now().toString(),
                task.getTaskId(),
                task.getJobId(),
                "IMAGE_CONVERSION",
                task.getPayload(),
                this.targetFormat);

    }

    public Map<String, FilePayload> getResults() {
        return results;
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
