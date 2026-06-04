package server.scheduler;

import protocol.*;
import server.db.DatabaseManager;
import server.job.*;
import server.model.MessageEnvelope;
import server.registry.*;

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.*;

public class TaskScheduler implements Runnable {
    private final BlockingQueue<MessageEnvelope> inboundMailbox;
    private final PeerRegistry registry;
    private final DatabaseManager db;
    private final SchedulerOutput output;
    private final SchedulerConfig config;
    private final Map<String, EmbarrassinglyParallelJob<?,?>> activeJobs = new LinkedHashMap<>();
    private final SchedulerMetrics metrics = new SchedulerMetrics();
    private long lastMetricsLogAtMillis = 0L;

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox, PeerRegistry registry, DatabaseManager db) {
        this(mailbox, registry, db, new PeerRegistrySchedulerOutput(registry), SchedulerConfig.fromEnvironment());
    }

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox,
                         PeerRegistry registry,
                         DatabaseManager db,
                         SchedulerOutput output) {
        this(mailbox, registry, db, output, SchedulerConfig.fromEnvironment());
    }

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox,
                         PeerRegistry registry,
                         DatabaseManager db,
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
                    handleMessage(envelope);
                }
                //Check for stale tasks (Watchdog)
                checkTimeouts();
                // Dispatch pending work
                dispatchPendingTasks();
                updateMetricsAndMaybeLog();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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

    private void handleMessage(MessageEnvelope envelope) {
        Message msg = envelope.message();

        if (msg instanceof JobSubmitMessage submit) {
            try {
                EmbarrassinglyParallelJob<?,?> job = JobFactory.create(submit, envelope.fromNodeId());
                job.initializeTasks(submit);
                if (job.getTasks().isEmpty()) {
                    throw new IllegalArgumentException("Job must create at least one task.");
                }
                activeJobs.put(job.getJobId(), job);
                metrics.setActiveJobs(activeJobs.size());
                logInfoEvent("job_started", fields(
                        "job_id", job.getJobId(),
                        "task_type", job.getTaskType(),
                        "requester_id", job.getRequesterNodeId(),
                        "task_count", job.getTasks().size()
                ));

                if (db != null) {
                    db.insertJob(job.getJobId(), job.getTaskType(), job.getRequesterNodeId(), job.getTasks().size());
                    for (String taskId : job.getTasks().keySet()) {
                        db.insertTask(taskId, job.getJobId());
                    }
                }
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
            db.markTaskCompleted(task.getTaskId(), System.currentTimeMillis(), Math.max(0L, completion.durationMs()));
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
        if (db != null) db.markTaskAssigned(task.getTaskId(), peer.getNodeId(), startedAt);
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
            db.markTaskFailed(task.getTaskId());
        } else if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
            db.markTaskRetried(task.getTaskId(), task.getRetryCount());
        }
    }

    private void failJob(EmbarrassinglyParallelJob<?, ?> job, String reason) {
        completeJob(job, false, reason);
    }

    private void completeJob(EmbarrassinglyParallelJob<?, ?> job, boolean success, String reason) {
        if (!success && db != null) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.COMPLETED
                        && task.getStatus() != TaskUnit.TaskStatus.FAILED) {
                    db.markTaskFailed(task.getTaskId());
                }
            }
        }

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

        try {
            boolean sent = output.sendJobResult(job.getRequesterNodeId(), response);
            if (!sent) {
                logErrorEvent("job_requester_missing", fields(
                        "job_id", job.getJobId(),
                        "requester_id", job.getRequesterNodeId()
                ));
            } else {
                logInfoEvent("job_completed", fields(
                        "job_id", job.getJobId(),
                        "requester_id", job.getRequesterNodeId(),
                        "success", success,
                        "result_count", finalData.size()
                ));
            }
        } catch (Exception e) {
            logErrorEvent("job_result_send_failed", fields(
                    "job_id", job.getJobId(),
                    "requester_id", job.getRequesterNodeId(),
                    "error", e.getMessage()
            ));
        }
        activeJobs.remove(job.getJobId());
        metrics.setActiveJobs(activeJobs.size());
        if (db != null) {
            if (success) {
                db.markJobCompleted(job.getJobId());
            } else {
                db.markJobFailed(job.getJobId());
            }
        }

        if (!success && reason != null && !reason.isBlank()) {
            logErrorEvent("job_failed", fields(
                    "job_id", job.getJobId(),
                    "failed_tasks", job.getFailedCount(),
                    "reason", reason
            ));
        }
    }

    private void sendJobStartFailure(String requesterNodeId, JobSubmitMessage submit, String reason) {
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
        logEvent(System.out, "INFO", event, fields);
    }

    private void logErrorEvent(String event, Map<String, Object> fields) {
        logEvent(System.err, "ERROR", event, fields);
    }

    private void logEvent(PrintStream stream, String level, String event, Map<String, Object> fields) {
        StringBuilder builder = new StringBuilder("[Scheduler]");
        builder.append(" level=").append(level).append(" event=").append(event);
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            builder.append(' ')
                    .append(entry.getKey())
                    .append('=')
                    .append(String.valueOf(entry.getValue()));
        }
        stream.println(builder);
    }
}
