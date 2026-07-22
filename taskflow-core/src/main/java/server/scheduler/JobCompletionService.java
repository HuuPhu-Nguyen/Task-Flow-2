package server.scheduler;

import protocol.JobResultMessage;
import server.db.BrokerOutboxStore;
import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.runtime.TaskFlowClock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Owns J1/J2 aggregation, result delivery, terminal persistence, and cleanup. */
final class JobCompletionService {
    private static final long RESULT_DELIVERY_RETRY_INTERVAL_MILLIS = 1_000L;

    private final SchedulerState state;
    private final PeerRegistry registry;
    private final SchedulerPersistence persistence;
    private final SchedulerOutput output;
    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final SchedulerMetrics metrics;
    private final SchedulerOutboxService outbox;
    private final SchedulerEventLog events;
    private final Map<String, PendingJobCompletion> pendingCompletions = new LinkedHashMap<>();

    JobCompletionService(SchedulerState state,
                         PeerRegistry registry,
                         SchedulerPersistence persistence,
                         SchedulerOutput output,
                         SchedulerConfig config,
                         TaskFlowClock clock,
                         SchedulerMetrics metrics,
                         SchedulerOutboxService outbox,
                         SchedulerEventLog events) {
        this.state = state;
        this.registry = registry;
        this.persistence = persistence;
        this.output = output;
        this.config = config;
        this.clock = clock;
        this.metrics = metrics;
        this.outbox = outbox;
        this.events = events;
    }

    boolean isPending(String jobId) {
        return pendingCompletions.containsKey(jobId);
    }

    JobResultMessage pendingResponse(String jobId) {
        PendingJobCompletion completion = pendingCompletions.get(jobId);
        return completion == null || !completion.durableTerminal
                ? null
                : completion.response;
    }

    void failJob(EmbarrassinglyParallelJob<?, ?> job, String reason) {
        completeJob(job, false, reason);
    }

    void completeJob(EmbarrassinglyParallelJob<?, ?> job, boolean success, String reason) {
        Object finalPayload = job.aggregateResultPayload();
        List<Object> compatibilityResults = compatibilityResults(job, finalPayload);
        JobResultMessage response = new JobResultMessage(
                "COORDINATOR",
                clock.now().toString(),
                job.getJobId(),
                job.getTaskType(),
                success,
                finalPayload,
                compatibilityResults,
                success ? null : reason
        );

        PendingJobCompletion completion = pendingCompletions.computeIfAbsent(
                job.getJobId(),
                ignored -> new PendingJobCompletion(job, response, success, reason)
        );
        tryDeliverJobResult(completion, true);
    }

    void retryPendingJobResults() {
        for (PendingJobCompletion completion : List.copyOf(pendingCompletions.values())) {
            tryDeliverJobResult(completion, false);
        }
    }

    private void tryDeliverJobResult(PendingJobCompletion completion, boolean force) {
        long now = clock.nowEpochMillis();
        if (!force && now - completion.lastAttemptAtMillis < RESULT_DELIVERY_RETRY_INTERVAL_MILLIS) {
            return;
        }
        completion.lastAttemptAtMillis = now;

        if (outbox.available()) {
            completion.attempts++;
            tryDeliverJobResultThroughOutbox(completion);
            return;
        }

        if (!completion.durableTerminal) {
            JobStateStore.DurableTransitionOutcome outcome = persistTerminalState(completion, now);
            if (!outcome.projectionAllowed()) {
                events.error("job_terminal_persistence_deferred", events.fields(
                        "job_id", completion.job.getJobId(),
                        "success", completion.success,
                        "outcome", outcome
                ));
                return;
            }
            completion.durableTerminal = true;
            projectTerminalState(completion);
        }

        completion.attempts++;

        try {
            boolean sent = output.sendJobResult(completion.job.getRequesterNodeId(), completion.response);
            if (!sent) {
                events.error("job_result_delivery_deferred", events.fields(
                        "job_id", completion.job.getJobId(),
                        "requester_id", completion.job.getRequesterNodeId(),
                        "attempt", completion.attempts,
                        "reason", "requester_missing"
                ));
                abandonIfResultDeliveryExhausted(completion, "requester_missing");
                return;
            }
        } catch (Exception e) {
            events.error("job_result_delivery_deferred", events.fields(
                    "job_id", completion.job.getJobId(),
                    "requester_id", completion.job.getRequesterNodeId(),
                    "attempt", completion.attempts,
                    "error", e.getMessage()
            ));
            abandonIfResultDeliveryExhausted(completion, e.getMessage());
            return;
        }

        finalizeJobCompletion(completion);
    }

    private void tryDeliverJobResultThroughOutbox(PendingJobCompletion completion) {
        BrokerOutboxStore.OutboxCommit outboxCommit = persistJobCompletionOutbox(completion);
        JobStateStore.DurableTransitionOutcome durableOutcome = persistence.record(
                completion.success
                        ? "markJobCompletedAndEnqueueBrokerOutbox"
                        : "markJobFailedAndEnqueueBrokerOutbox",
                completion.job.getJobId(),
                "",
                outboxCommit.outcome()
        );
        if (!durableOutcome.projectionAllowed() || outboxCommit.outboxRecord() == null) {
            events.error("job_result_delivery_deferred", events.fields(
                    "job_id", completion.job.getJobId(),
                    "requester_id", completion.job.getRequesterNodeId(),
                    "attempt", completion.attempts,
                    "reason", "broker_outbox_persistence_failed",
                    "outcome", durableOutcome
            ));
            return;
        }

        projectTerminalState(completion);
        boolean published = outbox.publish(outboxCommit.outboxRecord());
        finalizeJobCompletionAfterOutbox(completion, outboxCommit.outboxRecord(), published);
    }

    private BrokerOutboxStore.OutboxCommit persistJobCompletionOutbox(
            PendingJobCompletion completion) {
        BrokerOutboxStore outboxStore = outbox.store();
        BrokerOutboxPublisher outboxPublisher = outbox.publisher();
        if (outboxStore == null || outboxPublisher == null) {
            return failedOutboxCommit();
        }

        BrokerOutboxStore.OutboxMessage outboxMessage;
        try {
            outboxMessage = outboxPublisher.jobResultOutboxMessage(
                    completion.job.getRequesterNodeId(),
                    completion.response
            );
        } catch (RuntimeException e) {
            events.error("broker_outbox_prepare_failed", events.fields(
                    "job_id", completion.job.getJobId(),
                    "route", "JOB_RESULT",
                    "error", e.getMessage()
            ));
            return failedOutboxCommit();
        }

        if (completion.success) {
            return normalizeOutboxCommit(outboxStore.commitJobCompletedAndEnqueueBrokerOutbox(
                    completion.job.getJobId(),
                    completion.response.getResultPayload(),
                    outboxMessage
            ));
        }
        return normalizeOutboxCommit(outboxStore.commitJobFailedAndEnqueueBrokerOutbox(
                completion.job.getJobId(),
                brokerTaskFailureUpdatesForJobFailure(completion),
                outboxMessage
        ));
    }

    private static BrokerOutboxStore.OutboxCommit normalizeOutboxCommit(
            BrokerOutboxStore.OutboxCommit commit) {
        return commit == null ? failedOutboxCommit() : commit;
    }

    private static BrokerOutboxStore.OutboxCommit failedOutboxCommit() {
        return new BrokerOutboxStore.OutboxCommit(
                JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                null
        );
    }

    private List<BrokerOutboxStore.TaskFailureUpdate> brokerTaskFailureUpdatesForJobFailure(
            PendingJobCompletion completion) {
        return taskFailureUpdatesForJobFailure(completion).stream()
                .map(update -> new BrokerOutboxStore.TaskFailureUpdate(
                        update.taskId(),
                        update.outcome(),
                        update.failureReason(),
                        update.finishedAt()
                ))
                .toList();
    }

    private List<JobStateStore.TaskFailureUpdate> taskFailureUpdatesForJobFailure(
            PendingJobCompletion completion) {
        long failedAt = clock.nowEpochMillis();
        String reason = completion.reason == null || completion.reason.isBlank()
                ? "job_failed"
                : completion.reason;
        List<JobStateStore.TaskFailureUpdate> updates = new ArrayList<>();
        for (TaskUnit<?> task : completion.job.getTasks().values()) {
            if (task.getStatus() == TaskUnit.TaskStatus.COMPLETED
                    || task.getStatus() == TaskUnit.TaskStatus.FAILED) {
                continue;
            }
            updates.add(new JobStateStore.TaskFailureUpdate(
                    task.getTaskId(),
                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                    reason,
                    failedAt
            ));
        }
        return updates;
    }

    private void finalizeJobCompletionAfterOutbox(PendingJobCompletion completion,
                                                  BrokerOutboxStore.OutboxRecord outboxRecord,
                                                  boolean published) {
        EmbarrassinglyParallelJob<?, ?> job = completion.job;
        removeCompletion(job.getJobId());

        events.info("job_completed", events.fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "success", completion.success,
                "result_count", completion.response.getResultPayloadList().size(),
                "outbox_id", outboxRecord.outboxId(),
                "outbox_published", published
        ));
        logFailureIfPresent(completion);
    }

    private void abandonIfResultDeliveryExhausted(PendingJobCompletion completion, String reason) {
        if (completion.attempts < config.jobResultMaxDeliveryAttempts()) {
            return;
        }

        EmbarrassinglyParallelJob<?, ?> job = completion.job;
        removeCompletion(job.getJobId());

        events.error("job_result_delivery_abandoned", events.fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "attempts", completion.attempts,
                "success", completion.success,
                "reason", reason == null || reason.isBlank() ? "delivery_failed" : reason
        ));
    }

    private void finalizeJobCompletion(PendingJobCompletion completion) {
        EmbarrassinglyParallelJob<?, ?> job = completion.job;
        removeCompletion(job.getJobId());

        events.info("job_completed", events.fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "success", completion.success,
                "result_count", completion.response.getResultPayloadList().size()
        ));
        logFailureIfPresent(completion);
    }

    private JobStateStore.DurableTransitionOutcome persistTerminalState(
            PendingJobCompletion completion,
            long completedAt) {
        JobStateStore store = persistence.store();
        if (store == null) {
            return JobStateStore.DurableTransitionOutcome.COMMITTED;
        }
        String operation = completion.success ? "commitJobCompleted" : "commitJobFailed";
        JobStateStore.DurableTransitionOutcome outcome = completion.success
                ? store.commitJobCompleted(
                        completion.job.getJobId(),
                        completion.response.getResultPayload(),
                        completedAt
                )
                : store.commitJobFailed(
                        completion.job.getJobId(),
                        taskFailureUpdatesForJobFailure(completion),
                        completedAt
                );
        return persistence.record(operation, completion.job.getJobId(), "", outcome);
    }

    private void projectTerminalState(PendingJobCompletion completion) {
        if (completion.terminalProjectionApplied) {
            return;
        }
        if (!completion.success) {
            for (TaskUnit<?> task : completion.job.getTasks().values()) {
                task.projectCommittedJobFailure().ifPresent(peerId -> {
                    PeerInfo peer = registry.get(peerId);
                    if (peer != null) {
                        peer.decrementTasks();
                        registry.updateMetricsSnapshot(peerId);
                    }
                });
            }
        }
        completion.terminalProjectionApplied = true;
    }

    private void removeCompletion(String jobId) {
        state.removeJob(jobId);
        pendingCompletions.remove(jobId);
        metrics.setActiveJobs(state.activeJobCount());
    }

    private void logFailureIfPresent(PendingJobCompletion completion) {
        if (!completion.success && completion.reason != null && !completion.reason.isBlank()) {
            events.error("job_failed", events.fields(
                    "job_id", completion.job.getJobId(),
                    "failed_tasks", completion.job.getFailedCount(),
                    "reason", completion.reason
            ));
        }
    }

    private static List<Object> compatibilityResults(EmbarrassinglyParallelJob<?, ?> job,
                                                     Object finalPayload) {
        if (finalPayload instanceof List<?> list) {
            return list.stream().map(Object.class::cast).toList();
        }
        return job.aggregateAndSendResult();
    }

    private static final class PendingJobCompletion {
        private final EmbarrassinglyParallelJob<?, ?> job;
        private final JobResultMessage response;
        private final boolean success;
        private final String reason;
        private long lastAttemptAtMillis;
        private int attempts;
        private boolean durableTerminal;
        private boolean terminalProjectionApplied;

        private PendingJobCompletion(EmbarrassinglyParallelJob<?, ?> job,
                                     JobResultMessage response,
                                     boolean success,
                                     String reason) {
            this.job = job;
            this.response = response;
            this.success = success;
            this.reason = reason;
        }
    }
}
