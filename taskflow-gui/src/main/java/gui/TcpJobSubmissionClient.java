package gui;

import com.google.gson.Gson;
import messaging.SafeJsonWriter;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.RequesterTokens;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

final class TcpJobSubmissionClient implements JobSubmissionClient {
    private final String nodeId;
    private final Gson gson = new Gson();
    private final Map<String, String> requesterTokensByJobId = new ConcurrentHashMap<>();

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
        String requesterToken = RequesterTokens.newToken();
        requesterTokensByJobId.put(jobId, requesterToken);
        JobSubmitMessage message = new JobSubmitMessage(
                nodeId,
                Instant.now().toString(),
                jobId,
                taskType,
                taskPayloads,
                parameter,
                requesterToken
        );

        boolean sent;
        try {
            sent = SafeJsonWriter.send(out, gson, message);
        } catch (RuntimeException sendFailure) {
            requesterTokensByJobId.remove(jobId);
            throw sendFailure;
        }
        if (!sent) {
            requesterTokensByJobId.remove(jobId);
            throw new IllegalStateException("Could not send job submit message to coordinator.");
        }
    }

    @Override
    public void requestJobResult(String jobId, PrintWriter out) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId is required.");
        }
        Objects.requireNonNull(out, "out");
        String requesterToken = requesterTokensByJobId.get(jobId);
        if (!RequesterTokens.hasToken(requesterToken)) {
            throw new IllegalStateException("No requester token is available for job " + jobId + ".");
        }
        JobResultRequestMessage message = new JobResultRequestMessage(
                nodeId,
                Instant.now().toString(),
                jobId,
                requesterToken
        );

        if (!SafeJsonWriter.send(out, gson, message)) {
            throw new IllegalStateException("Could not send job result request to coordinator.");
        }
    }
}
