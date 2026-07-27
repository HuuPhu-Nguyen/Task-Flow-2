package server.plugins.example;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import example.model.ExampleJobSummary;
import example.model.ExamplePayload;
import example.model.ExampleTaskResult;
import example.model.ExampleTaskTypes;
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

public class ExampleWordCountJob extends EmbarrassinglyParallelJob<ExamplePayload, ExampleTaskResult> {
    private final Gson gson = new Gson();
    private final String resultFormat;
    private final Map<String, ExampleTaskResult> results = new ConcurrentHashMap<>();

    public ExampleWordCountJob(String jobId, String requesterId, String resultFormat) {
        super(jobId, requesterId, ExampleTaskTypes.WORD_COUNT);
        this.resultFormat = resultFormat;
    }

    @Override
    public void initializeTasks(JobSubmitMessage message) {
        List<Object> payloads = message.getTaskPayloads();
        if (payloads == null) {
            return;
        }

        for (int index = 0; index < payloads.size(); index++) {
            ExamplePayload payload = gson.fromJson(gson.toJson(payloads.get(index)), ExamplePayload.class);
            String taskId = "task-" + getJobId() + "-" + index;
            tasks.put(taskId, new ExampleTaskUnit(taskId, getJobId(), payload));
        }
    }

    @Override
    protected void onTaskSuccess(TaskUnit<ExamplePayload> task, ExampleTaskResult resultData) {
        results.put(task.getTaskId(), resultData);
    }

    @Override
    public List<Object> aggregateAndSendResult() {
        return orderedResults().stream()
                .map(Object.class::cast)
                .toList();
    }

    @Override
    public ExampleJobSummary aggregateResultPayload() {
        List<ExampleTaskResult> orderedResults = orderedResults();
        int totalWordCount = orderedResults.stream()
                .mapToInt(ExampleTaskResult::wordCount)
                .sum();
        return new ExampleJobSummary(orderedResults.size(), totalWordCount, orderedResults);
    }

    @Override
    protected ExampleTaskResult parseResult(Object payloads) {
        ExampleTaskResult result;
        try {
            result = gson.fromJson(gson.toJson(payloads), ExampleTaskResult.class);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(
                    "Example word count result has an invalid shape.",
                    e
            );
        }
        if (result == null) {
            throw new IllegalArgumentException("Example word count result is required.");
        }
        if (result.documentName() == null || result.documentName().isBlank()) {
            throw new IllegalArgumentException(
                    "Example word count result requires a document name."
            );
        }
        if (result.wordCount() < 0) {
            throw new IllegalArgumentException(
                    "Example word count result must not have a negative word count."
            );
        }
        return result;
    }

    @Override
    public TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task) {
        return new TaskAssignMessage(
                "COORDINATOR",
                Instant.now().toString(),
                task.getTaskId(),
                task.getJobId(),
                ExampleTaskTypes.WORD_COUNT,
                task.getPayload(),
                resultFormat
        );
    }

    private List<ExampleTaskResult> orderedResults() {
        return tasks.keySet().stream()
                .sorted(Comparator.comparingInt(this::taskIndex))
                .map(results::get)
                .filter(Objects::nonNull)
                .toList();
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
