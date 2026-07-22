package server.db;

import protocol.Message;
import protocol.MessageValidator;
import protocol.TaskAssignMessage;
import server.job.AssignmentIdentity;
import transport.TransportRoute;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface BrokerOutboxStore {
    record OutboxMessage(TransportRoute route,
                         String peerNodeId,
                         String fromNodeId,
                         Message message) {
        public OutboxMessage {
            if (route == null) {
                throw new IllegalArgumentException("route is required");
            }
            peerNodeId = peerNodeId == null ? "" : peerNodeId;
            if (fromNodeId == null || fromNodeId.isBlank()) {
                throw new IllegalArgumentException("fromNodeId is required");
            }
            if (message == null) {
                throw new IllegalArgumentException("message is required");
            }
        }
    }

    record OutboxRecord(long outboxId,
                        OutboxMessage message,
                        long createdAt,
                        int attemptCount,
                        long lastAttemptAt,
                        String lastError) {
        public OutboxRecord {
            if (outboxId <= 0L) {
                throw new IllegalArgumentException("outboxId must be positive");
            }
            if (message == null) {
                throw new IllegalArgumentException("message is required");
            }
            createdAt = Math.max(0L, createdAt);
            attemptCount = Math.max(0, attemptCount);
            lastAttemptAt = Math.max(0L, lastAttemptAt);
            lastError = lastError == null ? "" : lastError;
        }
    }

    /**
     * One database-committed task-assignment generation and the exact durable
     * broker envelope that carries it.
     */
    record CommittedTaskAssignment(AssignmentIdentity identity,
                                   OutboxRecord outboxRecord) {
        public CommittedTaskAssignment {
            if (identity == null) {
                throw new IllegalArgumentException("identity is required");
            }
            if (outboxRecord == null) {
                throw new IllegalArgumentException("outboxRecord is required");
            }
            OutboxMessage outboxMessage = outboxRecord.message();
            if (outboxMessage.route() != TransportRoute.TASK_ASSIGN
                    || !identity.workerId().equals(outboxMessage.peerNodeId())
                    || !(outboxMessage.message() instanceof TaskAssignMessage assignment)) {
                throw new IllegalArgumentException("outboxRecord must carry the committed task assignment");
            }
            MessageValidator.validate(assignment);
            if (!identity.taskId().equals(assignment.getTaskId())
                    || !identity.workerId().equals(assignment.getNodeId())
                    || identity.attemptNumber() != assignment.getAttemptNumber()
                    || !identity.assignmentId().equals(assignment.getAssignmentId())
                    || identity.leaseExpiresAtEpochMillis() != assignment.getLeaseExpiresAtEpochMillis()) {
                throw new IllegalArgumentException("outboxRecord assignment identity does not match identity");
            }
        }
    }

    record TaskFailureUpdate(String taskId,
                             JobStateStore.TaskAttemptOutcome outcome,
                             String failureReason,
                             long finishedAt) {
        public TaskFailureUpdate {
            if (taskId == null || taskId.isBlank()) {
                throw new IllegalArgumentException("taskId is required");
            }
            if (outcome == null) {
                outcome = JobStateStore.TaskAttemptOutcome.JOB_FAILED;
            }
            failureReason = failureReason == null ? "" : failureReason;
            finishedAt = Math.max(0L, finishedAt);
        }
    }

    Optional<OutboxRecord> enqueueBrokerOutbox(OutboxMessage message);

    /**
     * Creates the next assignment generation and its exact TASK_ASSIGN outbox
     * row in one durable transaction. The supplied message is an identity-free
     * routing/payload template; the store owns the generation and UUID.
     */
    Optional<CommittedTaskAssignment> createTaskAssignmentAndEnqueueBrokerOutbox(
            String taskId,
            String peerId,
            long startedAt,
            String leaseOwnerId,
            long leaseExpiresAt,
            OutboxMessage messageTemplate);

    Optional<OutboxRecord> markJobCompletedAndEnqueueBrokerOutbox(String jobId,
                                                                  Object resultPayload,
                                                                  OutboxMessage message);

    Optional<OutboxRecord> markJobFailedAndEnqueueBrokerOutbox(String jobId,
                                                               Collection<TaskFailureUpdate> taskFailures,
                                                               OutboxMessage message);

    List<OutboxRecord> loadPendingBrokerOutbox(int limit);

    boolean markBrokerOutboxPublished(long outboxId, long publishedAt);

    boolean markBrokerOutboxPublishFailed(long outboxId, String error, long attemptedAt);
}
