package server.scheduler;

import protocol.JobResultMessage;
import server.db.BrokerOutboxStore;
import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.runtime.TaskFlowClock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Owns J1/J2 aggregation, result delivery, terminal persistence, and cleanup. */
final class JobCompletionService {
    private static final long RESULT_DELIVERY_RETRY_INTERVAL_MILLIS = 1_000L;

    private final SchedulerState state;
    private final SchedulerPersistence persistence;
    private final SchedulerOutput output;
    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final SchedulerMetrics metrics;
    private final SchedulerOutboxService outbox;
    private final SchedulerEventLog events;
    private final Map<String, PendingJobCompletion> pendingCompletions = new LinkedHashMap<>();

    JobCompletionService(SchedulerState state,
                         SchedulerPersistence persistence,
                         SchedulerOutput output,
                         SchedulerConfig config,
                         TaskFlowClock clock,
                         SchedulerMetrics metrics,
                         SchedulerOutboxService outbox,
                         SchedulerEventLog events) {
        this.state = state;
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
        return completion == null ? null : completion.response;
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
        completion.attempts++;

        if (outbox.available()) {
            tryDeliverJobResultThroughOutbox(completion);
            return;
        }

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
        Optional<BrokerOutboxStore.OutboxRecord> outboxRecord = persistJobCompletionOutbox(completion);
        if (outboxRecord.isEmpty()) {
            events.error("job_result_delivery_deferred", events.fields(
                    "job_id", completion.job.getJobId(),
                    "requester_id", completion.job.getRequesterNodeId(),
                    "attempt", completion.attempts,
                    "reason", "broker_outbox_persistence_failed"
            ));
            abandonIfResultDeliveryExhausted(completion, "broker_outbox_persistence_failed");
            return;
        }

        boolean published = outbox.publish(outboxRecord.get());
        finalizeJobCompletionAfterOutbox(completion, outboxRecord.get(), published);
    }

    private Optional<BrokerOutboxStore.OutboxRecord> persistJobCompletionOutbox(
            PendingJobCompletion completion) {
        BrokerOutboxStore outboxStore = outbox.store();
        BrokerOutboxPublisher outboxPublisher = outbox.publisher();
        if (outboxStore == null || outboxPublisher == null) {
            return Optional.empty();
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
            return Optional.empty();
        }

        if (completion.success) {
            return outboxStore.markJobCompletedAndEnqueueBrokerOutbox(
                    completion.job.getJobId(),
                    completion.response.getResultPayload(),
                    outboxMessage
            );
        }
        return outboxStore.markJobFailedAndEnqueueBrokerOutbox(
                completion.job.getJobId(),
                taskFailureUpdatesForJobFailure(completion),
                outboxMessage
        );
    }

    private List<BrokerOutboxStore.TaskFailureUpdate> taskFailureUpdatesForJobFailure(
            PendingJobCompletion completion) {
        long failedAt = clock.nowEpochMillis();
        String reason = completion.reason == null || completion.reason.isBlank()
                ? "job_failed"
                : completion.reason;
        List<BrokerOutboxStore.TaskFailureUpdate> updates = new ArrayList<>();
        for (TaskUnit<?> task : completion.job.getTasks().values()) {
            if (task.getStatus() == TaskUnit.TaskStatus.COMPLETED
                    || task.getStatus() == TaskUnit.TaskStatus.FAILED) {
                continue;
            }
            updates.add(new BrokerOutboxStore.TaskFailureUpdate(
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
        boolean persisted = true;
        JobStateStore store = persistence.store();
        if (store != null) {
            long failedAt = clock.nowEpochMillis();
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.COMPLETED
                        && task.getStatus() != TaskUnit.TaskStatus.FAILED) {
                    persisted &= persistence.record(
                            "markTaskFailed",
                            job.getJobId(),
                            task.getTaskId(),
                            store.markTaskFailed(
                                    task.getTaskId(),
                                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                                    "result_delivery_abandoned",
                                    failedAt
                            )
                    );
                }
            }
            persisted &= persistence.record(
                    "markJobFailed",
                    job.getJobId(),
                    "",
                    store.markJobFailed(job.getJobId())
            );
        }

        removeCompletion(job.getJobId());
        if (!persisted) {
            logTerminalPersistenceDegraded(job, "markJobFailed", "result_delivery_abandoned");
        }

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
        boolean persisted = true;
        JobStateStore store = persistence.store();
        if (!completion.success && store != null) {
            long failedAt = clock.nowEpochMillis();
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.COMPLETED
                        && task.getStatus() != TaskUnit.TaskStatus.FAILED) {
                    persisted &= persistence.record(
                            "markTaskFailed",
                            job.getJobId(),
                            task.getTaskId(),
                            store.markTaskFailed(
                                    task.getTaskId(),
                                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                                    completion.reason == null || completion.reason.isBlank()
                                            ? "job_failed"
                                            : completion.reason,
                                    failedAt
                            )
                    );
                }
            }
        }

        removeCompletion(job.getJobId());
        if (store != null) {
            if (completion.success) {
                persisted &= persistence.record(
                        "markJobCompleted",
                        job.getJobId(),
                        "",
                        store.markJobCompleted(job.getJobId(), completion.response.getResultPayload())
                );
            } else {
                persisted &= persistence.record(
                        "markJobFailed",
                        job.getJobId(),
                        "",
                        store.markJobFailed(job.getJobId())
                );
            }
        }
        if (!persisted) {
            logTerminalPersistenceDegraded(
                    job,
                    completion.success ? "markJobCompleted" : "markJobFailed",
                    "result_delivered"
            );
        }

        events.info("job_completed", events.fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "success", completion.success,
                "result_count", completion.response.getResultPayloadList().size()
        ));
        logFailureIfPresent(completion);
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

    private void logTerminalPersistenceDegraded(EmbarrassinglyParallelJob<?, ?> job,
                                                String operation,
                                                String policy) {
        events.error("job_terminal_persistence_degraded", events.fields(
                "operation", operation,
                "job_id", job.getJobId(),
                "policy", policy
        ));
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
