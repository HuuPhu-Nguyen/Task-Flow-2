package gui;

import com.google.gson.Gson;
import messaging.SafeJsonWriter;
import protocol.JobSubmitMessage;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

final class TcpJobSubmissionClient implements JobSubmissionClient {
    private final String nodeId;
    private final Gson gson = new Gson();

    TcpJobSubmissionClient(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId is required.");
        }
        this.nodeId = nodeId;
    }

    @Override
    public String newJobId() {
        return "JOB_" + System.currentTimeMillis() + "_"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    @Override
    public void submitJob(String jobId, String taskType, List<?> payloads, String parameter, PrintWriter out) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId is required.");
        }
        Objects.requireNonNull(out, "out");
        List<Object> taskPayloads = payloads == null ? List.of() : new ArrayList<>(payloads);
        JobSubmitMessage message = new JobSubmitMessage(
                nodeId,
                Instant.now().toString(),
                jobId,
                taskType,
                taskPayloads,
                parameter
        );

        if (!SafeJsonWriter.send(out, gson, message)) {
            throw new IllegalStateException("Could not send job submit message to coordinator.");
        }
    }
}
