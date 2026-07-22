package server.db;

import protocol.Message;
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

    Optional<OutboxRecord> markTaskAssignedAndEnqueueBrokerOutbox(String taskId,
                                                                  String peerId,
                                                                  long startedAt,
                                                                  String leaseOwnerId,
                                                                  long leaseExpiresAt,
                                                                  OutboxMessage message);

    default Optional<OutboxRecord> markTaskAssignedAndEnqueueBrokerOutbox(String taskId,
                                                                          String peerId,
                                                                          long startedAt,
                                                                          String leaseOwnerId,
                                                                          long leaseExpiresAt,
                                                                          int attemptNumber,
                                                                          String assignmentId,
                                                                          OutboxMessage message) {
        return markTaskAssignedAndEnqueueBrokerOutbox(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                message
        );
    }

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
