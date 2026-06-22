package server.concreteJobs.text;

import com.google.gson.Gson;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;
import text.model.TextAnalysisTaskTypes;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TextAnalysisJob extends EmbarrassinglyParallelJob<TextAnalysisPayload, TextAnalysisResult> {
    private final Gson gson = new Gson();
    private final String resultFormat;
    private final Map<String, TextAnalysisResult> results = new ConcurrentHashMap<>();

    public TextAnalysisJob(String jobId, String requesterId, String resultFormat) {
        super(jobId, requesterId, TextAnalysisTaskTypes.TEXT_ANALYSIS);
        this.resultFormat = resultFormat;
    }

    @Override
    public void initializeTasks(JobSubmitMessage message) {
        List<Object> payloads = message.getTaskPayloads();
        if (payloads == null) {
            return;
        }

        for (int i = 0; i < payloads.size(); i++) {
            TextAnalysisPayload payload = gson.fromJson(gson.toJson(payloads.get(i)), TextAnalysisPayload.class);
            String taskId = "task-" + getJobId() + "-" + i;
            tasks.put(taskId, new TextAnalysisTaskUnit(taskId, getJobId(), payload));
        }
    }

    @Override
    protected void onTaskSuccess(TaskUnit<TextAnalysisPayload> task, TextAnalysisResult resultData) {
        results.put(task.getTaskId(), resultData);
    }

    @Override
    public List<Object> aggregateAndSendResult() {
        return tasks.keySet().stream()
                .sorted(Comparator.comparingInt(this::taskIndex))
                .map(results::get)
                .filter(Objects::nonNull)
                .map(Object.class::cast)
                .toList();
    }

    @Override
    protected TextAnalysisResult parseResult(Object payloads) {
        return gson.fromJson(gson.toJson(payloads), TextAnalysisResult.class);
    }

    @Override
    public TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task) {
        return new TaskAssignMessage(
                "COORDINATOR",
                Instant.now().toString(),
                task.getTaskId(),
                task.getJobId(),
                TextAnalysisTaskTypes.TEXT_ANALYSIS,
                task.getPayload(),
                resultFormat
        );
    }

    public Map<String, TextAnalysisResult> getResults() {
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
