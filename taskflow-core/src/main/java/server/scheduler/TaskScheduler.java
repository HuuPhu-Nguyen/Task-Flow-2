package server.scheduler;

import protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.db.JobStateStore;
import server.job.*;
import server.model.MessageEnvelope;
import server.registry.*;

import java.util.*;
import java.util.concurrent.*;

public class TaskScheduler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskScheduler.class);
    private static final long RESULT_DELIVERY_RETRY_INTERVAL_MILLIS = 1_000L;

    private final BlockingQueue<MessageEnvelope> inboundMailbox;
    private final PeerRegistry registry;
    private final JobStateStore db;
    private final SchedulerOutput output;
    private final SchedulerConfig config;
    private final Map<String, EmbarrassinglyParallelJob<?,?>> activeJobs = new LinkedHashMap<>();
    private final Map<String, PendingJobCompletion> pendingJobCompletions = new LinkedHashMap<>();
    private final SchedulerMetrics metrics = new SchedulerMetrics();
    private long lastMetricsLogAtMillis = 0L;

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox, PeerRegistry registry, JobStateStore db) {
        this(mailbox, registry, db, new PeerRegistrySchedulerOutput(registry), SchedulerConfig.fromEnvironment());
    }

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox,
                         PeerRegistry registry,
                         JobStateStore db,
                         SchedulerOutput output) {
        this(mailbox, registry, db, output, SchedulerConfig.fromEnvironment());
    }

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox,
                         PeerRegistry registry,
                         JobStateStore db,
                         SchedulerOutput output,
                         SchedulerConfig config) {
        this.inboundMailbox = mailbox;
        this.registry = registry;
        this.db = db;
        this.output = output;
        this.config = config == null ? SchedulerConfig.defaults() : config;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                //Process new messages (results or new jobs)
                MessageEnvelope envelope = inboundMailbox.poll(500, TimeUnit.MILLISECONDS);
                if (envelope != null) {
                    processEnvelope(envelope);
                }
                //Check for stale tasks (Watchdog)
                checkTimeouts();
                // Dispatch pending work
                dispatchPendingTasks();
                retryPendingJobResults();
                updateMetricsAndMaybeLog();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void processEnvelope(MessageEnvelope envelope) {
        try {
            handleMessage(envelope);
        } catch (Exception e) {
            logErrorEvent("scheduler_message_processing_failed", fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "error", e.getMessage()
            ));
            requeueEnvelope(envelope);
            return;
        }

        ackEnvelope(envelope);
    }

    private void ackEnvelope(MessageEnvelope envelope) {
        if (envelope.acknowledgement() == null) {
            return;
        }
        try {
            envelope.acknowledgement().ack();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logErrorEvent("scheduler_message_ack_failed", fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "error", e.getMessage()
            ));
        }
    }

    private void requeueEnvelope(MessageEnvelope envelope) {
        if (envelope.acknowledgement() == null) {
            return;
        }
        try {
            envelope.acknowledgement().requeue();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logErrorEvent("scheduler_message_requeue_failed", fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "error", e.getMessage()
            ));
        }
    }

    private String messageType(MessageEnvelope envelope) {
        Message message = envelope.message();
        return message == null ? "null" : String.valueOf(message.getType());
    }

    private void checkTimeouts() {
        long now = System.currentTimeMillis();
        Map<String, String> jobsToFail = new LinkedHashMap<>();

        for (EmbarrassinglyParallelJob<?,?> job : activeJobs.values()) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.ASSIGNED) {
                    continue;
                }
                long startedAt = task.getStartTime();
                if (startedAt <= 0 || (now - startedAt) <= config.taskTimeoutMillis()) {
                    continue;
                }

                String assignedPeerId = task.getAssignedPeerId();
                if (assignedPeerId == null) {
                    continue;
                }

                TaskUnit.FailureOutcome outcome = task.failAttemptBy(assignedPeerId, config.maxTaskRetries());
                if (outcome == TaskUnit.FailureOutcome.IGNORED) {
                    continue;
                }

                if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
                    metrics.recordRetry();
                }
                onAttemptFailure(assignedPeerId, outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE);
                persistTaskFailure(task, outcome);
                logErrorEvent("task_timeout", fields(
                        "job_id", job.getJobId(),
                        "task_id", task.getTaskId(),
                        "assigned_peer_id", assignedPeerId,
                        "retry_count", task.getRetryCount(),
                        "terminal_failure", outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE
                ));

                if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
                    jobsToFail.putIfAbsent(job.getJobId(),
                            "Task " + task.getTaskId() + " exceeded max retries after timeout.");
                }
            }
        }

        for (Map.Entry<String, String> entry : jobsToFail.entrySet()) {
            EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(entry.getKey());
            if (job != null) {
                failJob(job, entry.getValue());
            }
        }
    }

    private void handleMessage(MessageEnvelope envelope) throws Exception {
        Message msg = envelope.message();

        if (msg instanceof JobSubmitMessage submit) {
            try {
                if (submit.getJobId() == null || submit.getJobId().isBlank()) {
                    throw new IllegalArgumentException("Job id is required.");
                }
                if (activeJobs.containsKey(submit.getJobId())) {
                    sendJobStartFailure(envelope.fromNodeId(), submit,
                            "Job id is already active: " + submit.getJobId());
                    return;
                }
                EmbarrassinglyParallelJob<?,?> job = JobFactory.create(submit, envelope.fromNodeId());
                job.initializeTasks(submit);
                if (job.getTasks().isEmpty()) {
                    throw new IllegalArgumentException("Job must create at least one task.");
                }
                persistJobStartup(job);
                activeJobs.put(job.getJobId(), job);
                metrics.setActiveJobs(activeJobs.size());
                logInfoEvent("job_started", fields(
                        "job_id", job.getJobId(),
                        "task_type", job.getTaskType(),
                        "requester_id", job.getRequesterNodeId(),
                        "task_count", job.getTasks().size()
                ));
            } catch (Exception e) {
                logErrorEvent("job_start_failed", fields(
                        "job_id", submit.getJobId(),
                        "task_type", submit.getTaskType(),
                        "requester_id", envelope.fromNodeId(),
                        "error", e.getMessage()
                ));
                sendJobStartFailure(envelope.fromNodeId(), submit, e.getMessage());
            }
        }
        else if (msg instanceof TaskResultMessage result) {
            handleTaskResult(envelope, result);
        }
        else if (msg instanceof PeerDisconnectedMessage disconnected) {
            String peerId = disconnected.getNodeId();
            if (peerId == null || peerId.isBlank()) {
                peerId = envelope.fromNodeId();
            }
            handlePeerUnavailable(peerId, disconnected.getReason());
        }
    }

    private void handlePeerUnavailable(String peerId, String reason) {
        if (peerId == null || peerId.isBlank()) {
            return;
        }

        String normalizedReason = reason == null || reason.isBlank() ? "peer_unavailable" : reason;
        Map<String, String> jobsToFail = new LinkedHashMap<>();
        int retryScheduled = 0;
        int terminalFailures = 0;

        for (EmbarrassinglyParallelJob<?, ?> job : activeJobs.values()) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.ASSIGNED) {
                    continue;
                }
                if (!peerId.equals(task.getAssignedPeerId())) {
                    continue;
                }

                TaskUnit.FailureOutcome outcome = task.failAttemptBy(peerId, config.maxTaskRetries());
                if (outcome == TaskUnit.FailureOutcome.IGNORED) {
                    continue;
                }

                if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
                    retryScheduled++;
                    metrics.recordRetry();
                } else if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
                    terminalFailures++;
                }

                onAttemptFailure(peerId, outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE);
                persistTaskFailure(task, outcome);
                logErrorEvent("task_peer_unavailable", fields(
                        "job_id", job.getJobId(),
                        "task_id", task.getTaskId(),
                        "peer_id", peerId,
                        "retry_count", task.getRetryCount(),
                        "terminal_failure", outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE,
                        "reason", normalizedReason
                ));

                if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
                    jobsToFail.putIfAbsent(job.getJobId(),
                            "Task " + task.getTaskId() + " exceeded max retries after peer became unavailable.");
                }
            }
        }

        if (retryScheduled > 0 || terminalFailures > 0) {
            logInfoEvent("peer_unavailable_tasks_released", fields(
                    "peer_id", peerId,
                    "retry_scheduled", retryScheduled,
                    "terminal_failures", terminalFailures,
                    "reason", normalizedReason
            ));
        }

        for (Map.Entry<String, String> entry : jobsToFail.entrySet()) {
            EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(entry.getKey());
            if (job != null) {
                failJob(job, entry.getValue());
            }
        }
    }

    private void persistJobStartup(EmbarrassinglyParallelJob<?, ?> job) {
        if (db == null) {
            return;
        }
        boolean persisted = db.insertJobWithTasks(
                job.getJobId(),
                job.getTaskType(),
                job.getRequesterNodeId(),
                job.getTasks().size(),
                job.getTasks().keySet()
        );
        if (!persisted) {
            recordPersistence("insertJobWithTasks", job.getJobId(), "", false);
            throw new IllegalStateException("Job could not be persisted.");
        }
    }

    private void handleTaskResult(MessageEnvelope envelope, TaskResultMessage result) {
        EmbarrassinglyParallelJob<?, ?> job = activeJobs.get(result.getJobId());
        if (job == null) {
            return;
        }

        TaskUnit<?> task = job.getTasks().get(result.getTaskId());
        if (task == null) {
            return;
        }

        if (!result.isSuccessful()) {
            TaskUnit.FailureOutcome outcome = task.failAttemptBy(envelope.fromNodeId(), config.maxTaskRetries());
            if (outcome == TaskUnit.FailureOutcome.IGNORED) {
                return;
            }

            if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
                metrics.recordRetry();
            }
            onAttemptFailure(envelope.fromNodeId(), outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE);
            persistTaskFailure(task, outcome);
            logErrorEvent("task_failed", fields(
                    "job_id", job.getJobId(),
                    "task_id", task.getTaskId(),
                    "peer_id", envelope.fromNodeId(),
                    "retry_count", task.getRetryCount(),
                    "terminal_failure", outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE,
                    "error", result.getErrorMessage()
            ));

            if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
                failJob(job, "Task " + task.getTaskId() + " reached max retries.");
            }
            return;
        }

        EmbarrassinglyParallelJob.TaskCompletion completion = job.recordResult(
                result.getTaskId(),
                envelope.fromNodeId(),
                result.getResultPayload()
        );
        if (!completion.accepted()) {
            return;
        }

        onAttemptSuccess(envelope.fromNodeId(), completion.durationMs());
        if (db != null) {
            recordPersistence(
                    "markTaskCompleted",
                    job.getJobId(),
                    task.getTaskId(),
                    db.markTaskCompleted(task.getTaskId(), System.currentTimeMillis(), Math.max(0L, completion.durationMs()))
            );
        }
        logInfoEvent("task_completed", fields(
                "job_id", job.getJobId(),
                "task_id", task.getTaskId(),
                "peer_id", envelope.fromNodeId(),
                "duration_ms", Math.max(0L, completion.durationMs())
        ));

        if (job.isJobComplete()) {
            completeJob(job, true, null);
        } else if (job.hasTerminalFailure()) {
            failJob(job, "Job has one or more terminal failed tasks.");
        }
    }

    private void dispatchPendingTasks() {
        //Process jobs in order
        for (EmbarrassinglyParallelJob<?,?> job : activeJobs.values()) {
            // Capabilities are evaluated per job type before score/load selection.
            List<PeerInfo> candidates = getAvailablePeers(job.getTaskType());
            if (candidates.isEmpty()) {
                continue;
            }

            //Prioritize high-retry tasks
            List<? extends TaskUnit<?>> pending = job.getPendingTasks().stream()
                    .sorted(Comparator.comparingInt((TaskUnit<?> t) -> t.getRetryCount()).reversed())
                    .toList();

            for (TaskUnit<?> task : pending) {
                // Find the first peer in our sorted list who still has room.
                PeerInfo bestPeer = candidates.stream()
                        .filter(p -> p.getActiveTasks() < config.maxTasksPerPeer())
                        .findFirst()
                        .orElse(null);

                if (bestPeer != null) {
                    assign(job, task, bestPeer);
                } else {
                    break; // Compatible peers for this job have hit the configured concurrency limit.
                }
            }
        }
    }

    private void assign(EmbarrassinglyParallelJob<?,?> job, TaskUnit<?> task, PeerInfo peer) {
        long pendingSince = task.getPendingSinceMillis();
        long startedAt = System.currentTimeMillis();
        if (!task.markAssigned(peer.getNodeId(), startedAt)) {
            return;
        }
        long dispatchLatencyMs = pendingSince > 0 ? Math.max(0L, startedAt - pendingSince) : 0L;
        metrics.recordDispatch(dispatchLatencyMs);
        peer.incrementTasks();
        TaskAssignMessage message = job.createTaskAssignMessage(task);
        try {
            output.sendTask(peer, message);
        } catch (Exception e) {
            task.resetToPending();
            peer.decrementTasks();
            logErrorEvent("task_dispatch_failed", fields(
                    "job_id", job.getJobId(),
                    "task_id", task.getTaskId(),
                    "peer_id", peer.getNodeId(),
                    "error", e.getMessage()
            ));
            return;
        }
        if (db != null) {
            recordPersistence(
                    "markTaskAssigned",
                    job.getJobId(),
                    task.getTaskId(),
                    db.markTaskAssigned(task.getTaskId(), peer.getNodeId(), startedAt)
            );
        }
        logInfoEvent("task_assigned", fields(
                "job_id", job.getJobId(),
                "task_id", task.getTaskId(),
                "peer_id", peer.getNodeId(),
                "dispatch_latency_ms", dispatchLatencyMs
        ));
    }

    private List<PeerInfo> getAvailablePeers(String taskType) {
        return registry.getAllPeers().stream()
                // 1. Filter out dead, incapable, or over-encumbered peers
                .filter(PeerInfo::isConnected)
                .filter(p -> p.supportsTaskType(taskType))
                .filter(p -> p.getActiveTasks() < config.maxTasksPerPeer())

                // 2. Sort by our composite score (Lowest score = Best peer)
                .sorted(Comparator.comparingDouble(PeerInfo::getSelectionScore))
                .toList();
    }

    private void onAttemptSuccess(String peerId, long durationMs) {
        metrics.recordAttemptSuccess();
        PeerInfo peer = registry.get(peerId);
        if (peer == null) {
            return;
        }
        peer.recordTaskSuccess(durationMs);
        peer.decrementTasks();
    }

    private void onAttemptFailure(String peerId, boolean terminalFailure) {
        metrics.recordAttemptFailure(terminalFailure);
        PeerInfo peer = registry.get(peerId);
        if (peer == null) {
            return;
        }
        peer.recordTaskFailure();
        peer.decrementTasks();
    }

    private void persistTaskFailure(TaskUnit<?> task, TaskUnit.FailureOutcome outcome) {
        if (db == null) {
            return;
        }
        if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
            recordPersistence("markTaskFailed", task.getJobId(), task.getTaskId(), db.markTaskFailed(task.getTaskId()));
        } else if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
            recordPersistence(
                    "markTaskRetried",
                    task.getJobId(),
                    task.getTaskId(),
                    db.markTaskRetried(task.getTaskId(), task.getRetryCount())
            );
        }
    }

    private void failJob(EmbarrassinglyParallelJob<?, ?> job, String reason) {
        completeJob(job, false, reason);
    }

    private void completeJob(EmbarrassinglyParallelJob<?, ?> job, boolean success, String reason) {
        List<Object> finalData = job.aggregateAndSendResult();
        JobResultMessage response = new JobResultMessage(
                "COORDINATOR",
                java.time.Instant.now().toString(),
                job.getJobId(),
                job.getTaskType(),
                success,
                finalData,
                success ? null : reason
        );

        PendingJobCompletion completion = pendingJobCompletions.computeIfAbsent(
                job.getJobId(),
                ignored -> new PendingJobCompletion(job, response, success, reason)
        );
        tryDeliverJobResult(completion, true);
    }

    private void retryPendingJobResults() {
        for (PendingJobCompletion completion : List.copyOf(pendingJobCompletions.values())) {
            tryDeliverJobResult(completion, false);
        }
    }

    private void tryDeliverJobResult(PendingJobCompletion completion, boolean force) {
        long now = System.currentTimeMillis();
        if (!force && now - completion.lastAttemptAtMillis < RESULT_DELIVERY_RETRY_INTERVAL_MILLIS) {
            return;
        }
        completion.lastAttemptAtMillis = now;
        completion.attempts++;

        try {
            boolean sent = output.sendJobResult(completion.job.getRequesterNodeId(), completion.response);
            if (!sent) {
                logErrorEvent("job_result_delivery_deferred", fields(
                        "job_id", completion.job.getJobId(),
                        "requester_id", completion.job.getRequesterNodeId(),
                        "attempt", completion.attempts,
                        "reason", "requester_missing"
                ));
                abandonIfResultDeliveryExhausted(completion, "requester_missing");
                return;
            }
        } catch (Exception e) {
            logErrorEvent("job_result_delivery_deferred", fields(
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

    private void abandonIfResultDeliveryExhausted(PendingJobCompletion completion, String reason) {
        if (completion.attempts < config.jobResultMaxDeliveryAttempts()) {
            return;
        }

        EmbarrassinglyParallelJob<?, ?> job = completion.job;
        if (db != null) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.COMPLETED
                        && task.getStatus() != TaskUnit.TaskStatus.FAILED) {
                    recordPersistence(
                            "markTaskFailed",
                            job.getJobId(),
                            task.getTaskId(),
                            db.markTaskFailed(task.getTaskId())
                    );
                }
            }
            recordPersistence("markJobFailed", job.getJobId(), "", db.markJobFailed(job.getJobId()));
        }

        activeJobs.remove(job.getJobId());
        pendingJobCompletions.remove(job.getJobId());
        metrics.setActiveJobs(activeJobs.size());

        logErrorEvent("job_result_delivery_abandoned", fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "attempts", completion.attempts,
                "success", completion.success,
                "reason", reason == null || reason.isBlank() ? "delivery_failed" : reason
        ));
    }

    private void finalizeJobCompletion(PendingJobCompletion completion) {
        EmbarrassinglyParallelJob<?, ?> job = completion.job;
        if (!completion.success && db != null) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.COMPLETED
                        && task.getStatus() != TaskUnit.TaskStatus.FAILED) {
                    recordPersistence("markTaskFailed", job.getJobId(), task.getTaskId(), db.markTaskFailed(task.getTaskId()));
                }
            }
        }

        activeJobs.remove(job.getJobId());
        pendingJobCompletions.remove(job.getJobId());
        metrics.setActiveJobs(activeJobs.size());
        if (db != null) {
            if (completion.success) {
                recordPersistence("markJobCompleted", job.getJobId(), "", db.markJobCompleted(job.getJobId()));
            } else {
                recordPersistence("markJobFailed", job.getJobId(), "", db.markJobFailed(job.getJobId()));
            }
        }

        logInfoEvent("job_completed", fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "success", completion.success,
                "result_count", completion.response.getResultsByTaskId() == null
                        ? 0
                        : completion.response.getResultsByTaskId().size()
        ));

        if (!completion.success && completion.reason != null && !completion.reason.isBlank()) {
            logErrorEvent("job_failed", fields(
                    "job_id", job.getJobId(),
                    "failed_tasks", job.getFailedCount(),
                    "reason", completion.reason
            ));
        }
    }

    private void sendJobStartFailure(String requesterNodeId, JobSubmitMessage submit, String reason) throws Exception {
        JobResultMessage response = new JobResultMessage(
                "COORDINATOR",
                java.time.Instant.now().toString(),
                submit.getJobId(),
                submit.getTaskType(),
                false,
                List.of(),
                reason == null || reason.isBlank() ? "Job could not be started." : reason
        );
        try {
            if (!output.sendJobResult(requesterNodeId, response)) {
                logErrorEvent("job_start_failure_requester_missing", fields(
                        "job_id", submit.getJobId(),
                        "requester_id", requesterNodeId
                ));
            }
        } catch (Exception sendError) {
            logErrorEvent("job_start_failure_send_failed", fields(
                    "job_id", submit.getJobId(),
                    "requester_id", requesterNodeId,
                    "error", sendError.getMessage()
            ));
            throw sendError;
        }
    }

    public SchedulerMetrics.Snapshot getMetricsSnapshot() {
        return metrics.snapshot();
    }

    private void updateMetricsAndMaybeLog() {
        long now = System.currentTimeMillis();
        metrics.setQueueDepth(inboundMailbox.size());
        metrics.setActiveJobs(activeJobs.size());
        if (now - lastMetricsLogAtMillis < config.metricsLogIntervalMillis()) {
            return;
        }
        lastMetricsLogAtMillis = now;
        SchedulerMetrics.Snapshot snapshot = metrics.snapshot();
        logInfoEvent("scheduler_metrics", fields(
                "queue_depth", snapshot.queueDepth(),
                "active_jobs", snapshot.activeJobs(),
                "dispatch_latency_ms", String.format(Locale.US, "%.2f", snapshot.avgDispatchLatencyMs()),
                "retry_count", snapshot.retryCount(),
                "task_success_rate", String.format(Locale.US, "%.4f", snapshot.taskSuccessRate()),
                "success_count", snapshot.successCount(),
                "failure_count", snapshot.failureCount()
        ));
    }

    private Map<String, Object> fields(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues must be in pairs");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            out.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return out;
    }

    private void logInfoEvent(String event, Map<String, Object> fields) {
        LOGGER.info("event={}{}", event, formatFields(fields));
    }

    private void logErrorEvent(String event, Map<String, Object> fields) {
        LOGGER.error("event={}{}", event, formatFields(fields));
    }

    private void recordPersistence(String operation, String jobId, String taskId, boolean success) {
        if (success) {
            return;
        }
        logErrorEvent("scheduler_persistence_failed", fields(
                "operation", operation,
                "job_id", jobId,
                "task_id", taskId
        ));
    }

    private String formatFields(Map<String, Object> fields) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            builder.append(' ')
                    .append(entry.getKey())
                    .append('=')
                    .append(String.valueOf(entry.getValue()));
        }
        return builder.toString();
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
