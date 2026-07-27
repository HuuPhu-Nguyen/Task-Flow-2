package server.db;

import protocol.Message;
import protocol.MessageValidator;
import protocol.TaskAssignMessage;
import server.job.AssignmentIdentity;
import server.runtime.UuidAssignmentIdGenerator;
import transport.TransportRoute;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface BrokerOutboxStore {
    enum PendingOutboxCountOutcome {
        COUNTED,
        STORAGE_FAILURE
    }

    record PendingOutboxCount(PendingOutboxCountOutcome outcome, long count) {
        public PendingOutboxCount {
            outcome = outcome == null
                    ? PendingOutboxCountOutcome.STORAGE_FAILURE
                    : outcome;
            if (count < 0L) {
                throw new IllegalArgumentException("Pending outbox count must not be negative");
            }
            if (outcome == PendingOutboxCountOutcome.STORAGE_FAILURE && count != 0L) {
                throw new IllegalArgumentException(
                        "Storage-failure pending outbox count must be zero"
                );
            }
        }

        public static PendingOutboxCount counted(long count) {
            return new PendingOutboxCount(PendingOutboxCountOutcome.COUNTED, count);
        }

        public static PendingOutboxCount storageFailure() {
            return new PendingOutboxCount(PendingOutboxCountOutcome.STORAGE_FAILURE, 0L);
        }

        public boolean counted() {
            return outcome == PendingOutboxCountOutcome.COUNTED;
        }
    }

    record PendingOutboxMetrics(
            PendingOutboxCountOutcome outcome,
            long count,
            long oldestCreatedAt
    ) {
        public PendingOutboxMetrics {
            outcome = outcome == null
                    ? PendingOutboxCountOutcome.STORAGE_FAILURE
                    : outcome;
            if (count < 0L || oldestCreatedAt < 0L) {
                throw new IllegalArgumentException(
                        "Pending outbox metrics must not be negative"
                );
            }
            if ((outcome == PendingOutboxCountOutcome.STORAGE_FAILURE || count == 0L)
                    && oldestCreatedAt != 0L) {
                throw new IllegalArgumentException(
                        "Only a non-empty observed outbox may have an oldest creation time"
                );
            }
        }

        public static PendingOutboxMetrics observed(long count, long oldestCreatedAt) {
            return new PendingOutboxMetrics(
                    PendingOutboxCountOutcome.COUNTED,
                    count,
                    count == 0L ? 0L : oldestCreatedAt
            );
        }

        public static PendingOutboxMetrics storageFailure() {
            return new PendingOutboxMetrics(
                    PendingOutboxCountOutcome.STORAGE_FAILURE,
                    0L,
                    0L
            );
        }

        public boolean observed() {
            return outcome == PendingOutboxCountOutcome.COUNTED;
        }
    }

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

    record TaskAssignmentCommit(JobStateStore.DurableTransitionOutcome outcome,
                                CommittedTaskAssignment assignment) {
        public TaskAssignmentCommit {
            if (outcome == null) {
                outcome = JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE;
            }
            if (outcome.projectionAllowed() && assignment == null) {
                throw new IllegalArgumentException("Projectable assignment result requires assignment data");
            }
            if (!outcome.projectionAllowed() && assignment != null) {
                throw new IllegalArgumentException("Rejected assignment result cannot carry projection data");
            }
        }
    }

    record OutboxCommit(JobStateStore.DurableTransitionOutcome outcome,
                        OutboxRecord outboxRecord) {
        public OutboxCommit {
            if (outcome == null) {
                outcome = JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE;
            }
            if (outcome.projectionAllowed() && outboxRecord == null) {
                throw new IllegalArgumentException("Projectable outbox result requires an outbox record");
            }
            if (!outcome.projectionAllowed() && outboxRecord != null) {
                throw new IllegalArgumentException("Rejected outbox result cannot carry projection data");
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
     * routing/payload template. This compatibility overload uses the production
     * UUID adapter; deterministic callers should supply an assignment ID below.
     */
    default Optional<CommittedTaskAssignment> createTaskAssignmentAndEnqueueBrokerOutbox(
            String taskId,
            String peerId,
            long startedAt,
            String leaseOwnerId,
            long leaseExpiresAt,
            OutboxMessage messageTemplate) {
        return createTaskAssignmentAndEnqueueBrokerOutbox(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                UuidAssignmentIdGenerator.INSTANCE.nextAssignmentId(),
                messageTemplate
        );
    }

    /**
     * Creates the next durable attempt using the supplied assignment-ID
     * candidate. The transaction still owns the monotonic attempt number.
     */
    Optional<CommittedTaskAssignment> createTaskAssignmentAndEnqueueBrokerOutbox(
            String taskId,
            String peerId,
            long startedAt,
            String leaseOwnerId,
            long leaseExpiresAt,
            String assignmentId,
            OutboxMessage messageTemplate);

    default TaskAssignmentCommit commitTaskAssignmentAndEnqueueBrokerOutbox(
            String taskId,
            String peerId,
            long startedAt,
            String leaseOwnerId,
            long leaseExpiresAt,
            String assignmentId,
            OutboxMessage messageTemplate) {
        Optional<CommittedTaskAssignment> committed = createTaskAssignmentAndEnqueueBrokerOutbox(
                taskId,
                peerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAt,
                assignmentId,
                messageTemplate
        );
        return committed
                .map(value -> new TaskAssignmentCommit(
                        JobStateStore.DurableTransitionOutcome.COMMITTED,
                        value
                ))
                .orElseGet(() -> new TaskAssignmentCommit(
                        JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                        null
                ));
    }

    Optional<OutboxRecord> markJobCompletedAndEnqueueBrokerOutbox(String jobId,
                                                                  Object resultPayload,
                                                                  OutboxMessage message);

    Optional<OutboxRecord> markJobFailedAndEnqueueBrokerOutbox(String jobId,
                                                               Collection<TaskFailureUpdate> taskFailures,
                                                               OutboxMessage message);

    default OutboxCommit commitJobCompletedAndEnqueueBrokerOutbox(String jobId,
                                                                   Object resultPayload,
                                                                   OutboxMessage message) {
        return markJobCompletedAndEnqueueBrokerOutbox(jobId, resultPayload, message)
                .map(record -> new OutboxCommit(JobStateStore.DurableTransitionOutcome.COMMITTED, record))
                .orElseGet(() -> new OutboxCommit(
                        JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                        null
                ));
    }

    default OutboxCommit commitJobFailedAndEnqueueBrokerOutbox(
            String jobId,
            Collection<TaskFailureUpdate> taskFailures,
            OutboxMessage message) {
        return markJobFailedAndEnqueueBrokerOutbox(jobId, taskFailures, message)
                .map(record -> new OutboxCommit(JobStateStore.DurableTransitionOutcome.COMMITTED, record))
                .orElseGet(() -> new OutboxCommit(
                        JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                        null
                ));
    }

    List<OutboxRecord> loadPendingBrokerOutbox(int limit);

    PendingOutboxCount countPendingBrokerOutbox();

    default PendingOutboxMetrics observePendingBrokerOutbox() {
        PendingOutboxCount count = countPendingBrokerOutbox();
        if (count == null || !count.counted()) {
            return PendingOutboxMetrics.storageFailure();
        }
        if (count.count() == 0L) {
            return PendingOutboxMetrics.observed(0L, 0L);
        }
        List<OutboxRecord> oldest = loadPendingBrokerOutbox(1);
        return oldest == null || oldest.isEmpty()
                ? PendingOutboxMetrics.storageFailure()
                : PendingOutboxMetrics.observed(count.count(), oldest.getFirst().createdAt());
    }

    boolean markBrokerOutboxPublished(long outboxId, long publishedAt);

    boolean markBrokerOutboxPublishFailed(long outboxId, String error, long attemptedAt);
}
