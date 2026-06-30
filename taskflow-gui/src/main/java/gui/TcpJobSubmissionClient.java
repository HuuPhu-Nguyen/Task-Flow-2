package gui;

import com.google.gson.Gson;
import messaging.SafeJsonWriter;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.RequesterIdentity;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

final class TcpJobSubmissionClient implements JobSubmissionClient {
    private final String nodeId;
    private final Gson gson = new Gson();
    private final GuiRequesterTokenStore requesterTokenStore;

    TcpJobSubmissionClient(String nodeId) {
        this(nodeId, FileGuiRequesterTokenStore.openDefault());
    }

    TcpJobSubmissionClient(String nodeId, GuiRequesterTokenStore requesterTokenStore) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId is required.");
        }
        this.nodeId = nodeId;
        this.requesterTokenStore = Objects.requireNonNull(requesterTokenStore, "requesterTokenStore");
    }

    @Override
    public String newJobId() {
        return "JOB_" + System.currentTimeMillis() + "_"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    @Override
    public void submitJob(String jobId,
                          String taskType,
                          List<?> payloads,
                          String parameter,
                          CoordinatorConnection connection) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId is required.");
        }
        PrintWriter out = requireWriter(connection);
        List<Object> taskPayloads = payloads == null ? List.of() : new ArrayList<>(payloads);
        RequesterIdentity.Credentials identity = requesterTokenStore.requesterIdentity();
        String requesterToken = requesterTokenStore.createTokenForJob(jobId);
        String time = Instant.now().toString();
        String signature = RequesterIdentity.signJobSubmit(
                identity.privateKey(),
                nodeId,
                time,
                jobId,
                taskType,
                parameter,
                requesterToken
        );
        JobSubmitMessage message = new JobSubmitMessage(
                nodeId,
                time,
                jobId,
                taskType,
                taskPayloads,
                parameter,
                requesterToken,
                identity.publicKey(),
                signature
        );

        boolean sent;
        try {
            sent = SafeJsonWriter.send(out, gson, message);
        } catch (RuntimeException sendFailure) {
            requesterTokenStore.forgetToken(jobId);
            throw sendFailure;
        }
        if (!sent) {
            requesterTokenStore.forgetToken(jobId);
            throw new IllegalStateException("Could not send job submit message to coordinator.");
        }
    }

    @Override
    public void requestJobResult(String jobId, CoordinatorConnection connection) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId is required.");
        }
        PrintWriter out = requireWriter(connection);
        String requesterToken = requesterTokenStore.tokenForJob(jobId)
                .orElseThrow(() -> new IllegalStateException(
                        "No requester token is available for job " + jobId + "."));
        RequesterIdentity.Credentials identity = requesterTokenStore.requesterIdentity();
        String time = Instant.now().toString();
        String signature = RequesterIdentity.signJobResultRequest(
                identity.privateKey(),
                nodeId,
                time,
                jobId,
                requesterToken
        );
        JobResultRequestMessage message = new JobResultRequestMessage(
                nodeId,
                time,
                jobId,
                requesterToken,
                identity.publicKey(),
                signature
        );

        if (!SafeJsonWriter.send(out, gson, message)) {
            throw new IllegalStateException("Could not send job result request to coordinator.");
        }
    }

    private PrintWriter requireWriter(CoordinatorConnection connection) {
        Objects.requireNonNull(connection, "connection");
        if (!connection.isOpen()) {
            throw new IllegalStateException("Connection to coordinator is not open.");
        }
        PrintWriter out = connection.writer();
        if (out == null) {
            throw new IllegalStateException("Connection does not provide a TCP writer.");
        }
        return out;
    }
}
