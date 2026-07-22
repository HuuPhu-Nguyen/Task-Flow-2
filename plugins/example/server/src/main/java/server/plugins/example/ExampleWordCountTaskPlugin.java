package server.plugins.example;

import com.google.gson.Gson;
import example.model.ExamplePayload;
import example.model.ExampleTaskTypes;
import plugin.RetrySafety;
import protocol.JobSubmitMessage;
import protocol.PayloadLimits;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

import java.util.List;

public class ExampleWordCountTaskPlugin implements TaskPlugin {
    private static final List<String> RESULT_FORMATS = List.of("summary");
    private static final Gson GSON = new Gson();

    @Override
    public String taskType() {
        return ExampleTaskTypes.WORD_COUNT;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    public void validateSubmission(JobSubmitMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("Example word count submission is required.");
        }
        if (!ExampleTaskTypes.WORD_COUNT.equalsIgnoreCase(message.getTaskType())) {
            throw new IllegalArgumentException("Unsupported example task type: " + message.getTaskType());
        }
        normalizeResultFormat(message.getParameter());

        List<Object> payloads = message.getTaskPayloads();
        if (payloads == null || payloads.isEmpty()) {
            throw new IllegalArgumentException("Example word count requires at least one text payload.");
        }
        int maxTasks = PayloadLimits.maxTasksPerJob();
        if (payloads.size() > maxTasks) {
            throw new IllegalArgumentException("Example word count payload count exceeds "
                    + PayloadLimits.MAX_TASKS_PER_JOB_ENV + " (" + maxTasks + "): " + payloads.size());
        }
        for (Object rawPayload : payloads) {
            ExamplePayload payload = GSON.fromJson(GSON.toJson(rawPayload), ExamplePayload.class);
            validatePayload(payload);
        }
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new ExampleWordCountJob(
                message.getJobId(),
                requesterId,
                normalizeResultFormat(message.getParameter())
        );
    }

    static String normalizeResultFormat(String parameter) {
        String value = parameter == null || parameter.isBlank()
                ? "summary"
                : parameter.trim();
        return RESULT_FORMATS.stream()
                .filter(format -> format.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unsupported example word count result format '" + value
                                + "'. Supported values: " + RESULT_FORMATS));
    }

    private static void validatePayload(ExamplePayload payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Example word count payload is required.");
        }
        if (payload.documentName() == null || payload.documentName().isBlank()) {
            throw new IllegalArgumentException("Example word count payload requires a document name.");
        }
        if (payload.text() == null) {
            throw new IllegalArgumentException("Example word count payload requires text.");
        }
    }
}
