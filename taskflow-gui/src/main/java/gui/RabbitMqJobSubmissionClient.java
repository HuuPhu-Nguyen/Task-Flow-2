package gui;

import protocol.JobIds;
import protocol.JobSubmitMessage;
import protocol.PeerIdentity;
import protocol.RequesterIdentity;
import transport.OutboundTransportMessage;
import transport.TransportRoute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class RabbitMqJobSubmissionClient implements JobSubmissionClient {
    private final String nodeId;
    private final GuiRequesterTokenStore requesterTokenStore;

    RabbitMqJobSubmissionClient(String nodeId) {
        this(nodeId, FileGuiRequesterTokenStore.openDefault());
    }

    RabbitMqJobSubmissionClient(String nodeId, GuiRequesterTokenStore requesterTokenStore) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId is required.");
        }
        this.nodeId = PeerIdentity.require(nodeId);
        this.requesterTokenStore = Objects.requireNonNull(requesterTokenStore, "requesterTokenStore");
    }

    @Override
    public String newJobId() {
        return JobIds.newJobId(nodeId);
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
        if (!(connection instanceof RabbitMqBrokerConnection rabbitConnection)) {
            throw new IllegalStateException("RabbitMQ job submission requires a RabbitMQ coordinator connection.");
        }
        if (!nodeId.equals(rabbitConnection.peerId())) {
            throw new IllegalStateException("RabbitMQ job submission connection peer id does not match this client.");
        }
        if (!connection.isOpen()) {
            throw new IllegalStateException("RabbitMQ coordinator connection is not open.");
        }

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

        try {
            boolean published = rabbitConnection.transport().publish(new OutboundTransportMessage(
                    TransportRoute.JOB_SUBMIT,
                    nodeId,
                    message
            ));
            if (!published) {
                throw new IllegalStateException("Job submit publish was not confirmed for job " + jobId + ".");
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish job submit message to RabbitMQ.", e);
        }
    }

    @Override
    public void requestJobResult(String jobId, CoordinatorConnection connection) {
        throw new UnsupportedOperationException("RabbitMQ GUI result requests are not supported yet.");
    }
}
