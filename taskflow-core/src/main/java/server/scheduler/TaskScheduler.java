package server.scheduler;

import protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.db.BrokerOutboxStore;
import server.db.JobStateStore;
import server.job.*;
import server.model.MessageEnvelope;
import server.registry.*;
import server.runtime.AssignmentIdGenerator;
import server.runtime.SystemTaskFlowClock;
import server.runtime.TaskFlowClock;
import server.runtime.UuidAssignmentIdGenerator;

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
    private final String leaseOwnerId;
    private final TaskFlowClock clock;
    private final AssignmentIdGenerator assignmentIdGenerator;
    private final Map<String, EmbarrassinglyParallelJob<?,?>> activeJobs = new LinkedHashMap<>();
    private final Map<String, PendingJobCompletion> pendingJobCompletions = new LinkedHashMap<>();
    private final Map<String, String> requesterTokenHashes = new LinkedHashMap<>();
    private final Map<String, String> requesterIdentityKeys = new LinkedHashMap<>();
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
        this(
                mailbox,
                registry,
                db,
                output,
                config,
                SystemTaskFlowClock.INSTANCE,
                UuidAssignmentIdGenerator.INSTANCE
        );
    }

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox,
                         PeerRegistry registry,
                         JobStateStore db,
                         SchedulerOutput output,
                         SchedulerConfig config,
                         TaskFlowClock clock,
                         AssignmentIdGenerator assignmentIdGenerator) {
        this(
                mailbox,
                registry,
                db,
                output,
                config,
                clock,
                assignmentIdGenerator,
                newLeaseOwnerId()
        );
    }

    public TaskScheduler(BlockingQueue<MessageEnvelope> mailbox,
                         PeerRegistry registry,
                         JobStateStore db,
                         SchedulerOutput output,
                         SchedulerConfig config,
                         TaskFlowClock clock,
                         AssignmentIdGenerator assignmentIdGenerator,
                         String leaseOwnerId) {
        this.inboundMailbox = mailbox;
        this.registry = registry;
        this.db = db;
        this.output = output;
        this.config = config == null ? SchedulerConfig.defaults() : config;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.assignmentIdGenerator = Objects.requireNonNull(
                assignmentIdGenerator,
                "assignmentIdGenerator"
        );
        if (leaseOwnerId == null || leaseOwnerId.isBlank()) {
            throw new IllegalArgumentException("leaseOwnerId is required");
        }
        this.leaseOwnerId = leaseOwnerId.trim();
    }

    private static String newLeaseOwnerId() {
        return "COORDINATOR_" + UuidAssignmentIdGenerator.INSTANCE.nextAssignmentId();
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
                checkLeaseExpirations();
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
        } catch (MessageValidationException e) {
            logErrorEvent("scheduler_message_validation_failed", fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "reason_code", e.reasonCode(),
                    "action", "reject",
                    "error", e.getMessage()
            ));
            rejectEnvelope(envelope);
            return;
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

    private void rejectEnvelope(MessageEnvelope envelope) {
        if (envelope.acknowledgement() == null) {
            return;
        }
        try {
            envelope.acknowledgement().reject();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logErrorEvent("scheduler_message_reject_failed", fields(
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
        long now = clock.nowEpochMillis();
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
                boolean persisted = persistTaskFailure(task, outcome, "task_timeout", now);
                logErrorEvent("task_timeout", fields(
                        "job_id", job.getJobId(),
                        "task_id", task.getTaskId(),
                        "assigned_peer_id", assignedPeerId,
                        "retry_count", task.getRetryCount(),
                        "terminal_failure", outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE
                ));

                if (!persisted) {
                    jobsToFail.putIfAbsent(job.getJobId(),
                            persistenceFailureReason(taskFailurePersistenceOperation(outcome)));
                } else if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
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
                MessageValidator.validate(submit);
                if (activeJobs.containsKey(submit.getJobId())) {
                    sendJobStartFailure(envelope.fromNodeId(), submit,
                            "Job id is already active: " + submit.getJobId());
                    return;
                }
                if (db != null && db.hasJob(submit.getJobId())) {
                    sendJobStartFailure(envelope.fromNodeId(), submit,
                            "Job id already exists in persisted history: " + submit.getJobId());
                    return;
                }
                String requesterTokenHash = RequesterTokens.hashToken(submit.getRequesterToken());
                if (!RequesterTokens.hasTokenHash(requesterTokenHash)) {
                    throw new IllegalArgumentException("Requester token is required.");
                }
                String requesterIdentityKey = requesterIdentityKey(submit);
                EmbarrassinglyParallelJob<?,?> job = JobFactory.create(submit, envelope.fromNodeId());
                job.initializeTasks(submit);
                job.configureTransitionPorts(clock, assignmentIdGenerator);
                if (job.getTasks().isEmpty()) {
                    throw new IllegalArgumentException("Job must create at least one task.");
                }
                persistJobStartup(job, submit.getParameter(), requesterTokenHash, requesterIdentityKey);
                activeJobs.put(job.getJobId(), job);
                requesterTokenHashes.put(job.getJobId(), requesterTokenHash);
                if (hasText(requesterIdentityKey)) {
                    requesterIdentityKeys.put(job.getJobId(), requesterIdentityKey);
                }
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
            MessageValidator.validate(result);
            handleTaskResult(envelope, result);
        }
        else if (msg instanceof JobResultRequestMessage request) {
            handleJobResultRequest(envelope, request);
        }
        else if (msg instanceof PeerDisconnectedMessage disconnected) {
            MessageValidator.validate(disconnected);
            String peerId = disconnected.getNodeId();
            if (peerId == null || peerId.isBlank()) {
                peerId = envelope.fromNodeId();
            }
            handlePeerUnavailable(peerId, disconnected.getReason());
        }
    }

    private void handleJobResultRequest(MessageEnvelope envelope, JobResultRequestMessage request) throws Exception {
        String requesterId = envelope.fromNodeId();
        String jobId = request.getJobId();
        try {
            MessageValidator.validate(request);
        } catch (MessageValidationException e) {
            sendRequestedJobResult(requesterId, new JobResultMessage(
                    "COORDINATOR",
                    clock.now().toString(),
                    safeJobResultId(jobId),
                    "",
                    false,
                    List.of(),
                    e.getMessage()
            ));
            return;
        }

        PendingJobCompletion pending = pendingJobCompletions.get(jobId);
        if (pending != null) {
            if (!authorizeJobResultRequest(
                    requesterId,
                    request,
                    jobId,
                    pending.response.getTaskType(),
                    requesterTokenHashes.get(jobId),
                    requesterIdentityKeys.get(jobId)
            )) {
                return;
            }
            sendRequestedJobResult(requesterId, pending.response);
            return;
        }

        EmbarrassinglyParallelJob<?, ?> activeJob = activeJobs.get(jobId);
        if (activeJob != null) {
            if (!authorizeJobResultRequest(
                    requesterId,
                    request,
                    jobId,
                    activeJob.getTaskType(),
                    requesterTokenHashes.get(jobId),
                    requesterIdentityKeys.get(jobId)
            )) {
                return;
            }
            sendRequestedJobResult(requesterId, new JobResultMessage(
                    "COORDINATOR",
                    clock.now().toString(),
                    jobId,
                    activeJob.getTaskType(),
                    false,
                    List.of(),
                    "Job is still running."
            ));
            return;
        }

        if (db == null) {
            sendRequestedJobResult(requesterId, new JobResultMessage(
                    "COORDINATOR",
                    clock.now().toString(),
                    jobId,
                    "",
                    false,
                    List.of(),
                    "Completed job result is unavailable because persistence is disabled."
            ));
            return;
        }

        Optional<JobStateStore.CompletedJobResultState> result = db.loadCompletedJobResult(jobId);
        if (result.isPresent()) {
            JobStateStore.CompletedJobResultState completed = result.get();
            if (!authorizeJobResultRequest(
                    requesterId,
                    request,
                    completed.jobId(),
                    completed.taskType(),
                    completed.requesterTokenHash(),
                    completed.requesterIdentityKey()
            )) {
                return;
            }
            sendRequestedJobResult(requesterId, new JobResultMessage(
                    "COORDINATOR",
                    clock.now().toString(),
                    completed.jobId(),
                    completed.taskType(),
                    true,
                    completed.resultPayload(),
                    completed.resultsByTaskId()
            ));
            return;
        }

        sendRequestedJobResult(requesterId, new JobResultMessage(
                "COORDINATOR",
                clock.now().toString(),
                jobId,
                "",
                false,
                List.of(),
                "Completed job result not found."
        ));
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
                boolean persisted = persistTaskFailure(
                        task,
                        outcome,
                        normalizedReason,
                        clock.nowEpochMillis()
                );
                logErrorEvent("task_peer_unavailable", fields(
                        "job_id", job.getJobId(),
                        "task_id", task.getTaskId(),
                        "peer_id", peerId,
                        "retry_count", task.getRetryCount(),
                        "terminal_failure", outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE,
                        "reason", normalizedReason
                ));

                if (!persisted) {
                    jobsToFail.putIfAbsent(job.getJobId(),
                            persistenceFailureReason(taskFailurePersistenceOperation(outcome)));
                } else if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
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

    private void persistJobStartup(EmbarrassinglyParallelJob<?, ?> job,
                                   String parameter,
                                   String requesterTokenHash,
                                   String requesterIdentityKey) {
        if (db == null) {
            return;
        }
        boolean persisted = db.insertJobWithTasks(
                job.getJobId(),
                job.getTaskType(),
                job.getRequesterNodeId(),
                requesterTokenHash,
                requesterIdentityKey,
                parameter,
                job.getTasks().values().stream()
                        .sorted(Comparator.comparingInt(task -> taskIndex(task.getTaskId())))
                        .map(task -> new JobStateStore.TaskStartupState(task.getTaskId(), task.getPayload()))
                        .toList()
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

        LeaseExpiryResult leaseExpiry = expireTaskLeaseIfNeeded(job, task, clock.nowEpochMillis());
        if (leaseExpiry.handled()) {
            if (leaseExpiry.jobFailureReason() != null) {
                failJob(job, leaseExpiry.jobFailureReason());
            }
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
            boolean persisted = persistTaskFailure(
                    task,
                    outcome,
                    result.getErrorMessage(),
                    clock.nowEpochMillis()
            );
            logErrorEvent("task_failed", fields(
                    "job_id", job.getJobId(),
                    "task_id", task.getTaskId(),
                    "peer_id", envelope.fromNodeId(),
                    "retry_count", task.getRetryCount(),
                    "terminal_failure", outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE,
                    "error", result.getErrorMessage()
            ));

            if (!persisted) {
                failJob(job, persistenceFailureReason(taskFailurePersistenceOperation(outcome)));
            } else if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
                failJob(job, "Task " + task.getTaskId() + " reached max retries.");
            }
            return;
        }

        long completedAt = clock.nowEpochMillis();
        long startedAt = task.getStartTime();
        long durationMs = startedAt > 0L ? Math.max(0L, completedAt - startedAt) : 0L;
        boolean matchesInMemory = task.matchesAssignment(
                envelope.fromNodeId(),
                result.getAttemptNumber(),
                result.getAssignmentId()
        );
        EmbarrassinglyParallelJob.PreparedTaskResult preparedResult = matchesInMemory
                ? job.prepareTaskResult(result.getResultPayload())
                : null;

        JobStateStore.ResultCommitOutcome commitOutcome;
        if (db == null) {
            commitOutcome = matchesInMemory
                    ? JobStateStore.ResultCommitOutcome.COMMITTED
                    : JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT;
        } else {
            commitOutcome = db.commitTaskResult(
                    task.getTaskId(),
                    result.getAttemptNumber(),
                    result.getAssignmentId(),
                    envelope.fromNodeId(),
                    completedAt,
                    durationMs,
                    result.getResultPayload()
            );
        }
        if (commitOutcome == null) {
            commitOutcome = JobStateStore.ResultCommitOutcome.STORAGE_FAILURE;
        }

        if (commitOutcome != JobStateStore.ResultCommitOutcome.COMMITTED) {
            metrics.recordResultCommitOutcome(commitOutcome);
            logResultCommitDisposition(job, task, envelope.fromNodeId(), result, commitOutcome);
            if (commitOutcome == JobStateStore.ResultCommitOutcome.STORAGE_FAILURE) {
                throw new IllegalStateException(persistenceFailureReason("commitTaskResult"));
            }
            return;
        }

        if (preparedResult == null) {
            throw new IllegalStateException(
                    "Authoritative result committed for an assignment that does not match scheduler memory."
            );
        }
        EmbarrassinglyParallelJob.TaskCompletion completion = job.applyCommittedResult(
                result.getTaskId(),
                envelope.fromNodeId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                completedAt,
                preparedResult
        );
        if (!completion.accepted()) {
            throw new IllegalStateException(
                    "Authoritative result commit could not be applied to scheduler memory."
            );
        }

        metrics.recordResultCommitOutcome(JobStateStore.ResultCommitOutcome.COMMITTED);
        onAttemptSuccess(envelope.fromNodeId(), completion.durationMs());
        logInfoEvent("task_result_committed", assignmentTraceFields(
                job.getJobId(),
                task.getTaskId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                envelope.fromNodeId(),
                "commit_outcome", commitOutcome,
                "duration_ms", Math.max(0L, completion.durationMs())
        ));

        if (job.isJobComplete()) {
            completeJob(job, true, null);
        } else if (job.hasTerminalFailure()) {
            failJob(job, "Job has one or more terminal failed tasks.");
        }
    }

    private void checkLeaseExpirations() {
        long now = clock.nowEpochMillis();
        Map<String, String> jobsToFail = new LinkedHashMap<>();

        for (EmbarrassinglyParallelJob<?, ?> job : activeJobs.values()) {
            for (TaskUnit<?> task : job.getTasks().values()) {
                LeaseExpiryResult result = expireTaskLeaseIfNeeded(job, task, now);
                if (result.jobFailureReason() != null) {
                    jobsToFail.putIfAbsent(job.getJobId(), result.jobFailureReason());
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

    private LeaseExpiryResult expireTaskLeaseIfNeeded(EmbarrassinglyParallelJob<?, ?> job,
                                                      TaskUnit<?> task,
                                                      long now) {
        if (task.getStatus() != TaskUnit.TaskStatus.ASSIGNED || !task.isLeaseExpired(now)) {
            return LeaseExpiryResult.notHandled();
        }

        String assignedPeerId = task.getAssignedPeerId();
        if (assignedPeerId == null || assignedPeerId.isBlank()) {
            return LeaseExpiryResult.notHandled();
        }

        long leaseExpiresAt = task.getLeaseExpiresAtMillis();
        TaskUnit.FailureOutcome outcome = task.failAttemptBy(assignedPeerId, config.maxTaskRetries());
        if (outcome == TaskUnit.FailureOutcome.IGNORED) {
            return LeaseExpiryResult.notHandled();
        }

        if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
            metrics.recordRetry();
        }
        onAttemptFailure(assignedPeerId, outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE);
        boolean persisted = persistTaskFailure(task, outcome, "lease_expired", now);
        logErrorEvent("task_lease_expired", fields(
                "job_id", job.getJobId(),
                "task_id", task.getTaskId(),
                "assigned_peer_id", assignedPeerId,
                "lease_expires_at", leaseExpiresAt,
                "retry_count", task.getRetryCount(),
                "terminal_failure", outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE
        ));

        if (!persisted) {
            return LeaseExpiryResult.handled(persistenceFailureReason(taskFailurePersistenceOperation(outcome)));
        }
        if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
            return LeaseExpiryResult.handled(
                    "Task " + task.getTaskId() + " lease expired before completion.");
        }
        return LeaseExpiryResult.handled(null);
    }

    private void dispatchPendingTasks() {
        //Process jobs in order
        for (EmbarrassinglyParallelJob<?,?> job : List.copyOf(activeJobs.values())) {
            if (!activeJobs.containsKey(job.getJobId()) || pendingJobCompletions.containsKey(job.getJobId())) {
                continue;
            }
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
                    if (!activeJobs.containsKey(job.getJobId())
                            || pendingJobCompletions.containsKey(job.getJobId())) {
                        break;
                    }
                } else {
                    break; // Compatible peers for this job have hit the configured concurrency limit.
                }
            }
        }
    }

    private void assign(EmbarrassinglyParallelJob<?,?> job, TaskUnit<?> task, PeerInfo peer) {
        long pendingSince = task.getPendingSinceMillis();
        long startedAt = clock.nowEpochMillis();
        long leaseExpiresAt = leaseExpiresAt(startedAt);
        long dispatchLatencyMs = pendingSince > 0 ? Math.max(0L, startedAt - pendingSince) : 0L;
        BrokerOutboxStore outboxStore = brokerOutboxStore();
        BrokerOutboxPublisher outboxPublisher = brokerOutboxPublisher();
        if (outboxStore != null && outboxPublisher != null) {
            assignWithBrokerOutbox(
                    job,
                    task,
                    peer,
                    startedAt,
                    leaseExpiresAt,
                    dispatchLatencyMs,
                    outboxStore,
                    outboxPublisher
            );
            return;
        }
        if (!task.markAssigned(peer.getNodeId(), startedAt, leaseOwnerId, leaseExpiresAt)) {
            return;
        }
        AssignmentIdentity assignmentIdentity;
        TaskAssignMessage message;
        try {
            assignmentIdentity = task.getAssignmentIdentity()
                    .orElseThrow(() -> new IllegalStateException(
                            "Assigned task is missing assignment identity: " + task.getTaskId()));
            message = job.createTaskAssignMessage(task).withAssignmentIdentity(
                    assignmentIdentity.attemptNumber(),
                    assignmentIdentity.assignmentId(),
                    assignmentIdentity.leaseExpiresAtEpochMillis()
            );
            MessageValidator.validate(message);
        } catch (RuntimeException e) {
            task.resetToPending();
            failJob(job, "Task assignment could not be prepared: " + e.getMessage());
            return;
        }
        if (db != null) {
            boolean persisted = recordPersistence(
                    "markTaskAssigned",
                    job.getJobId(),
                     task.getTaskId(),
                    db.markTaskAssigned(
                            task.getTaskId(),
                            peer.getNodeId(),
                            startedAt,
                            leaseOwnerId,
                            leaseExpiresAt,
                            assignmentIdentity.attemptNumber(),
                            assignmentIdentity.assignmentId()
                    )
            );
            if (!persisted) {
                task.resetToPending();
                failJob(job, persistenceFailureReason("markTaskAssigned"));
                return;
            }
        }
        metrics.recordAssignmentGeneration(dispatchLatencyMs);
        peer.incrementTasks();
        logInfoEvent("task_assignment_created", assignmentTraceFields(
                job.getJobId(),
                task.getTaskId(),
                assignmentIdentity.attemptNumber(),
                assignmentIdentity.assignmentId(),
                peer.getNodeId(),
                "dispatch_latency_ms", dispatchLatencyMs
        ));
        try {
            output.sendTask(peer, message);
        } catch (Exception e) {
            task.resetToPending();
            peer.decrementTasks();
            if (db != null && !recordPersistence(
                    "markTaskRetried",
                    job.getJobId(),
                    task.getTaskId(),
                    db.markTaskRetried(
                            task.getTaskId(),
                            task.getRetryCount(),
                            JobStateStore.TaskAttemptOutcome.DISPATCH_FAILED,
                            e.getMessage(),
                            clock.nowEpochMillis()
                    )
            )) {
                failJob(job, persistenceFailureReason("markTaskRetried"));
                return;
            }
            logErrorEvent("task_dispatch_failed", fields(
                    "job_id", job.getJobId(),
                    "task_id", task.getTaskId(),
                    "peer_id", peer.getNodeId(),
                    "error", e.getMessage()
            ));
            return;
        }
    }

    private void assignWithBrokerOutbox(EmbarrassinglyParallelJob<?, ?> job,
                                        TaskUnit<?> task,
                                        PeerInfo peer,
                                        long startedAt,
                                        long leaseExpiresAt,
                                        long dispatchLatencyMs,
                                        BrokerOutboxStore outboxStore,
                                        BrokerOutboxPublisher outboxPublisher) {
        BrokerOutboxStore.OutboxMessage outboxTemplate;
        try {
            TaskAssignMessage messageTemplate = job.createTaskAssignMessage(task);
            outboxTemplate = outboxPublisher.taskAssignmentOutboxMessage(peer, messageTemplate);
        } catch (RuntimeException e) {
            failJob(job, "Broker outbox task assignment could not be prepared: " + e.getMessage());
            return;
        }

        Optional<BrokerOutboxStore.CommittedTaskAssignment> committedAssignment =
                outboxStore.createTaskAssignmentAndEnqueueBrokerOutbox(
                        task.getTaskId(),
                        peer.getNodeId(),
                        startedAt,
                        leaseOwnerId,
                        leaseExpiresAt,
                        assignmentIdGenerator.nextAssignmentId(),
                        outboxTemplate
                );
        if (committedAssignment.isEmpty()) {
            failJob(job, persistenceFailureReason("createTaskAssignmentAndEnqueueBrokerOutbox"));
            return;
        }
        BrokerOutboxStore.CommittedTaskAssignment committed = committedAssignment.get();
        try {
            if (!task.markAssigned(committed.identity(), startedAt, leaseOwnerId)) {
                throw new IllegalStateException("Task was no longer pending after assignment commit.");
            }
        } catch (RuntimeException e) {
            failJob(job, "Committed broker assignment could not be installed in memory: " + e.getMessage());
            return;
        }

        metrics.recordAssignmentGeneration(dispatchLatencyMs);
        peer.incrementTasks();
        BrokerOutboxStore.OutboxRecord outboxRecord = committed.outboxRecord();
        boolean published = publishBrokerOutboxRecord(outboxStore, outboxPublisher, outboxRecord);
        logInfoEvent("task_assignment_created", assignmentTraceFields(
                job.getJobId(),
                task.getTaskId(),
                committed.identity().attemptNumber(),
                committed.identity().assignmentId(),
                peer.getNodeId(),
                "dispatch_latency_ms", dispatchLatencyMs,
                "outbox_id", outboxRecord.outboxId(),
                "outbox_published", published
        ));
    }

    private long leaseExpiresAt(long startedAt) {
        long leaseMillis = config.taskLeaseMillis();
        if (Long.MAX_VALUE - startedAt < leaseMillis) {
            return Long.MAX_VALUE;
        }
        return startedAt + leaseMillis;
    }

    private BrokerOutboxStore brokerOutboxStore() {
        if (db instanceof BrokerOutboxStore store) {
            return store;
        }
        return null;
    }

    private BrokerOutboxPublisher brokerOutboxPublisher() {
        if (output instanceof BrokerOutboxPublisher publisher) {
            return publisher;
        }
        return null;
    }

    private boolean publishBrokerOutboxRecord(BrokerOutboxStore outboxStore,
                                              BrokerOutboxPublisher outboxPublisher,
                                              BrokerOutboxStore.OutboxRecord record) {
        long attemptedAt = clock.nowEpochMillis();
        try {
            boolean published = outboxPublisher.publishOutbox(record);
            if (!published) {
                outboxStore.markBrokerOutboxPublishFailed(
                        record.outboxId(),
                        "publish_unconfirmed_or_unroutable",
                        attemptedAt
                );
                logErrorEvent("broker_outbox_publish_deferred", fields(
                        "outbox_id", record.outboxId(),
                        "route", record.message().route(),
                        "peer_id", record.message().peerNodeId(),
                        "reason", "publish_unconfirmed_or_unroutable"
                ));
                return false;
            }
            if (!outboxStore.markBrokerOutboxPublished(record.outboxId(), attemptedAt)) {
                logErrorEvent("broker_outbox_publish_mark_failed", fields(
                        "outbox_id", record.outboxId(),
                        "route", record.message().route(),
                        "peer_id", record.message().peerNodeId()
                ));
                return false;
            }
            return true;
        } catch (Exception e) {
            outboxStore.markBrokerOutboxPublishFailed(record.outboxId(), e.getMessage(), attemptedAt);
            logErrorEvent("broker_outbox_publish_failed", fields(
                    "outbox_id", record.outboxId(),
                    "route", record.message().route(),
                    "peer_id", record.message().peerNodeId(),
                    "error", e.getMessage()
            ));
            return false;
        }
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
        PeerInfo peer = registry.get(peerId);
        if (peer == null) {
            return;
        }
        peer.recordTaskSuccess(durationMs);
        peer.decrementTasks();
        registry.updateMetricsSnapshot(peerId);
    }

    private void onAttemptFailure(String peerId, boolean terminalFailure) {
        metrics.recordAttemptFailure(terminalFailure);
        PeerInfo peer = registry.get(peerId);
        if (peer == null) {
            return;
        }
        peer.recordTaskFailure();
        peer.decrementTasks();
        registry.updateMetricsSnapshot(peerId);
    }

    private void logResultCommitDisposition(EmbarrassinglyParallelJob<?, ?> job,
                                            TaskUnit<?> task,
                                            String reportingPeerId,
                                            TaskResultMessage result,
                                            JobStateStore.ResultCommitOutcome outcome) {
        Map<String, Object> eventFields = assignmentTraceFields(
                job.getJobId(),
                task.getTaskId(),
                result.getAttemptNumber(),
                result.getAssignmentId(),
                reportingPeerId,
                "commit_outcome", outcome
        );
        switch (outcome) {
            case STALE_ASSIGNMENT -> logInfoEvent("task_result_stale_rejected", eventFields);
            case DUPLICATE_ALREADY_COMPLETED -> logInfoEvent("task_result_duplicate_ignored", eventFields);
            case UNKNOWN_TASK, STORAGE_FAILURE -> logErrorEvent("task_result_not_committed", eventFields);
            case COMMITTED -> throw new IllegalArgumentException(
                    "Committed results must use the task_result_committed event."
            );
        }
    }

    private boolean persistTaskFailure(TaskUnit<?> task,
                                       TaskUnit.FailureOutcome outcome,
                                       String failureReason,
                                       long finishedAt) {
        if (db == null) {
            return true;
        }
        String reason = failureReason == null || failureReason.isBlank() ? "task_failed" : failureReason;
        if (outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE) {
            return recordPersistence(
                    "markTaskFailed",
                    task.getJobId(),
                    task.getTaskId(),
                    db.markTaskFailed(
                            task.getTaskId(),
                            JobStateStore.TaskAttemptOutcome.TERMINAL_FAILURE,
                            reason,
                            finishedAt
                    )
            );
        } else if (outcome == TaskUnit.FailureOutcome.RETRY_SCHEDULED) {
            return recordPersistence(
                    "markTaskRetried",
                    task.getJobId(),
                    task.getTaskId(),
                    db.markTaskRetried(
                            task.getTaskId(),
                            task.getRetryCount(),
                            JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                            reason,
                            finishedAt
                    )
            );
        }
        return true;
    }

    private void failJob(EmbarrassinglyParallelJob<?, ?> job, String reason) {
        completeJob(job, false, reason);
    }

    private void completeJob(EmbarrassinglyParallelJob<?, ?> job, boolean success, String reason) {
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
        long now = clock.nowEpochMillis();
        if (!force && now - completion.lastAttemptAtMillis < RESULT_DELIVERY_RETRY_INTERVAL_MILLIS) {
            return;
        }
        completion.lastAttemptAtMillis = now;
        completion.attempts++;

        BrokerOutboxStore outboxStore = brokerOutboxStore();
        BrokerOutboxPublisher outboxPublisher = brokerOutboxPublisher();
        if (outboxStore != null && outboxPublisher != null) {
            tryDeliverJobResultThroughOutbox(completion, outboxStore, outboxPublisher);
            return;
        }

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

    private void tryDeliverJobResultThroughOutbox(PendingJobCompletion completion,
                                                  BrokerOutboxStore outboxStore,
                                                  BrokerOutboxPublisher outboxPublisher) {
        Optional<BrokerOutboxStore.OutboxRecord> outboxRecord =
                persistJobCompletionOutbox(completion, outboxStore, outboxPublisher);
        if (outboxRecord.isEmpty()) {
            logErrorEvent("job_result_delivery_deferred", fields(
                    "job_id", completion.job.getJobId(),
                    "requester_id", completion.job.getRequesterNodeId(),
                    "attempt", completion.attempts,
                    "reason", "broker_outbox_persistence_failed"
            ));
            abandonIfResultDeliveryExhausted(completion, "broker_outbox_persistence_failed");
            return;
        }

        boolean published = publishBrokerOutboxRecord(outboxStore, outboxPublisher, outboxRecord.get());
        finalizeJobCompletionAfterOutbox(completion, outboxRecord.get(), published);
    }

    private Optional<BrokerOutboxStore.OutboxRecord> persistJobCompletionOutbox(
            PendingJobCompletion completion,
            BrokerOutboxStore outboxStore,
            BrokerOutboxPublisher outboxPublisher
    ) {
        BrokerOutboxStore.OutboxMessage outboxMessage;
        try {
            outboxMessage = outboxPublisher.jobResultOutboxMessage(
                    completion.job.getRequesterNodeId(),
                    completion.response
            );
        } catch (RuntimeException e) {
            logErrorEvent("broker_outbox_prepare_failed", fields(
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
            PendingJobCompletion completion
    ) {
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
        activeJobs.remove(job.getJobId());
        pendingJobCompletions.remove(job.getJobId());
        requesterTokenHashes.remove(job.getJobId());
        requesterIdentityKeys.remove(job.getJobId());
        metrics.setActiveJobs(activeJobs.size());

        logInfoEvent("job_completed", fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "success", completion.success,
                "result_count", completion.response.getResultPayloadList().size(),
                "outbox_id", outboxRecord.outboxId(),
                "outbox_published", published
        ));

        if (!completion.success && completion.reason != null && !completion.reason.isBlank()) {
            logErrorEvent("job_failed", fields(
                    "job_id", job.getJobId(),
                    "failed_tasks", job.getFailedCount(),
                    "reason", completion.reason
            ));
        }
    }

    private void abandonIfResultDeliveryExhausted(PendingJobCompletion completion, String reason) {
        if (completion.attempts < config.jobResultMaxDeliveryAttempts()) {
            return;
        }

        EmbarrassinglyParallelJob<?, ?> job = completion.job;
        boolean persisted = true;
        if (db != null) {
            long failedAt = clock.nowEpochMillis();
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.COMPLETED
                        && task.getStatus() != TaskUnit.TaskStatus.FAILED) {
                    persisted &= recordPersistence(
                            "markTaskFailed",
                            job.getJobId(),
                            task.getTaskId(),
                            db.markTaskFailed(
                                    task.getTaskId(),
                                    JobStateStore.TaskAttemptOutcome.JOB_FAILED,
                                    "result_delivery_abandoned",
                                    failedAt
                            )
                    );
                }
            }
            persisted &= recordPersistence("markJobFailed", job.getJobId(), "", db.markJobFailed(job.getJobId()));
        }

        activeJobs.remove(job.getJobId());
        pendingJobCompletions.remove(job.getJobId());
        requesterTokenHashes.remove(job.getJobId());
        requesterIdentityKeys.remove(job.getJobId());
        metrics.setActiveJobs(activeJobs.size());
        if (!persisted) {
            logTerminalPersistenceDegraded(job, "markJobFailed", "result_delivery_abandoned");
        }

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
        boolean persisted = true;
        if (!completion.success && db != null) {
            long failedAt = clock.nowEpochMillis();
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.COMPLETED
                        && task.getStatus() != TaskUnit.TaskStatus.FAILED) {
                    persisted &= recordPersistence(
                            "markTaskFailed",
                            job.getJobId(),
                            task.getTaskId(),
                            db.markTaskFailed(
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

        activeJobs.remove(job.getJobId());
        pendingJobCompletions.remove(job.getJobId());
        requesterTokenHashes.remove(job.getJobId());
        requesterIdentityKeys.remove(job.getJobId());
        metrics.setActiveJobs(activeJobs.size());
        if (db != null) {
            if (completion.success) {
                persisted &= recordPersistence(
                        "markJobCompleted",
                        job.getJobId(),
                        "",
                        db.markJobCompleted(job.getJobId(), completion.response.getResultPayload())
                );
            } else {
                persisted &= recordPersistence("markJobFailed", job.getJobId(), "", db.markJobFailed(job.getJobId()));
            }
        }
        if (!persisted) {
            logTerminalPersistenceDegraded(
                    job,
                    completion.success ? "markJobCompleted" : "markJobFailed",
                    "result_delivered"
            );
        }

        logInfoEvent("job_completed", fields(
                "job_id", job.getJobId(),
                "requester_id", job.getRequesterNodeId(),
                "success", completion.success,
                "result_count", completion.response.getResultPayloadList().size()
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
                clock.now().toString(),
                safeJobResultId(submit.getJobId()),
                safeTaskType(submit.getTaskType()),
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
                throw new IllegalStateException(
                        "Job start failure result was not routed to requester " + requesterNodeId);
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

    private static String safeJobResultId(String jobId) {
        try {
            MessageValidator.validateJobId(jobId, "Job id");
            return jobId;
        } catch (RuntimeException ignored) {
            return "";
        }
    }

    private static String safeTaskType(String taskType) {
        try {
            MessageValidator.validateTaskType(taskType, "Task type");
            return taskType;
        } catch (RuntimeException ignored) {
            return "";
        }
    }

    private void sendRequestedJobResult(String requesterNodeId, JobResultMessage response) throws Exception {
        try {
            if (!output.sendJobResult(requesterNodeId, response)) {
                logErrorEvent("job_result_requester_missing", fields(
                        "job_id", response.getJobId(),
                        "requester_id", requesterNodeId
                ));
                throw new IllegalStateException(
                        "Requested job result was not routed to requester " + requesterNodeId);
            }
        } catch (Exception sendError) {
            logErrorEvent("job_result_request_send_failed", fields(
                    "job_id", response.getJobId(),
                    "requester_id", requesterNodeId,
                    "error", sendError.getMessage()
            ));
            throw sendError;
        }
    }

    private boolean authorizeJobResultRequest(String requesterNodeId,
                                              JobResultRequestMessage request,
                                              String jobId,
                                              String taskType,
                                              String expectedTokenHash,
                                              String expectedIdentityKey) throws Exception {
        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                request,
                expectedTokenHash,
                expectedIdentityKey
        );
        if (authorization.authorized()) {
            return true;
        }

        sendRequestedJobResult(requesterNodeId, new JobResultMessage(
                "COORDINATOR",
                clock.now().toString(),
                jobId == null ? "" : jobId,
                taskType == null ? "" : taskType,
                false,
                List.of(),
                authorization.errorMessage()
        ));
        return false;
    }

    private List<Object> compatibilityResults(EmbarrassinglyParallelJob<?, ?> job, Object finalPayload) {
        if (finalPayload instanceof List<?> list) {
            return list.stream()
                    .map(Object.class::cast)
                    .toList();
        }
        return job.aggregateAndSendResult();
    }

    public SchedulerMetrics.Snapshot getMetricsSnapshot() {
        return metrics.snapshot();
    }

    public void restoreJobs(Collection<EmbarrassinglyParallelJob<?, ?>> jobs) {
        restoreJobs(jobs, Map.of(), Map.of());
    }

    public void restoreJobs(Collection<EmbarrassinglyParallelJob<?, ?>> jobs,
                            Map<String, String> restoredRequesterTokenHashes) {
        restoreJobs(jobs, restoredRequesterTokenHashes, Map.of());
    }

    public void restoreJobs(Collection<EmbarrassinglyParallelJob<?, ?>> jobs,
                            Map<String, String> restoredRequesterTokenHashes,
                            Map<String, String> restoredRequesterIdentityKeys) {
        Map<String, String> tokenHashes = restoredRequesterTokenHashes == null
                ? Map.of()
                : restoredRequesterTokenHashes;
        Map<String, String> identityKeys = restoredRequesterIdentityKeys == null
                ? Map.of()
                : restoredRequesterIdentityKeys;
        for (EmbarrassinglyParallelJob<?, ?> job : jobs) {
            if (job == null || activeJobs.containsKey(job.getJobId())) {
                continue;
            }
            job.configureTransitionPorts(clock, assignmentIdGenerator);
            activeJobs.put(job.getJobId(), job);
            String tokenHash = tokenHashes.get(job.getJobId());
            if (RequesterTokens.hasTokenHash(tokenHash)) {
                requesterTokenHashes.put(job.getJobId(), tokenHash);
            }
            String identityKey = identityKeys.get(job.getJobId());
            if (hasText(identityKey)) {
                requesterIdentityKeys.put(job.getJobId(), identityKey);
            }
            logInfoEvent("job_resumed", fields(
                    "job_id", job.getJobId(),
                    "task_type", job.getTaskType(),
                    "requester_id", job.getRequesterNodeId(),
                    "task_count", job.getTasks().size()
            ));
        }
        metrics.setActiveJobs(activeJobs.size());

        for (EmbarrassinglyParallelJob<?, ?> job : List.copyOf(activeJobs.values())) {
            if (job.isJobComplete()) {
                completeJob(job, true, null);
            } else if (job.hasTerminalFailure()) {
                failJob(job, "Job resumed with one or more terminal failed tasks.");
            }
        }
    }

    private void updateMetricsAndMaybeLog() {
        long now = clock.nowEpochMillis();
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
                "failure_count", snapshot.failureCount(),
                SchedulerMetrics.TASK_RESULTS_COMMITTED_TOTAL_NAME,
                snapshot.taskResultsCommittedTotal(),
                SchedulerMetrics.TASK_RESULTS_STALE_TOTAL_NAME,
                snapshot.taskResultsStaleTotal(),
                SchedulerMetrics.TASK_RESULTS_DUPLICATE_TOTAL_NAME,
                snapshot.taskResultsDuplicateTotal(),
                SchedulerMetrics.ASSIGNMENT_GENERATIONS_TOTAL_NAME,
                snapshot.assignmentGenerationsTotal(),
                "unknown_result_count", snapshot.unknownResultCount(),
                "result_storage_failure_count", snapshot.resultStorageFailureCount()
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

    private Map<String, Object> assignmentTraceFields(String jobId,
                                                      String taskId,
                                                      int attemptNumber,
                                                      String assignmentId,
                                                      String workerId,
                                                      Object... additionalFields) {
        Map<String, Object> out = fields(
                "job_id", jobId,
                "task_id", taskId,
                "attempt_number", attemptNumber,
                "assignment_id", assignmentId,
                "worker_id", workerId
        );
        Map<String, Object> extras = fields(additionalFields);
        for (Map.Entry<String, Object> entry : extras.entrySet()) {
            if (out.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
                throw new IllegalArgumentException(
                        "Assignment trace field cannot be replaced: " + entry.getKey()
                );
            }
        }
        return out;
    }

    private void logInfoEvent(String event, Map<String, Object> fields) {
        LOGGER.info("event={}{}", event, formatFields(fields));
    }

    private void logErrorEvent(String event, Map<String, Object> fields) {
        LOGGER.error("event={}{}", event, formatFields(fields));
    }

    private boolean recordPersistence(String operation, String jobId, String taskId, boolean success) {
        if (success) {
            return true;
        }
        logErrorEvent("scheduler_persistence_failed", fields(
                "operation", operation,
                "job_id", jobId,
                "task_id", taskId
        ));
        return false;
    }

    private String taskFailurePersistenceOperation(TaskUnit.FailureOutcome outcome) {
        return outcome == TaskUnit.FailureOutcome.TERMINAL_FAILURE ? "markTaskFailed" : "markTaskRetried";
    }

    private String persistenceFailureReason(String operation) {
        return "Persistence write failed during " + operation + ".";
    }

    private static String requesterIdentityKey(JobSubmitMessage submit) {
        if (RequesterIdentity.hasPartialIdentity(submit.getRequesterPublicKey(), submit.getRequesterSignature())) {
            throw new IllegalArgumentException(
                    "Requester identity public key and signature are both required when identity is provided.");
        }
        if (!RequesterIdentity.hasIdentity(submit.getRequesterPublicKey(), submit.getRequesterSignature())) {
            return "";
        }
        if (!RequesterIdentity.verifyJobSubmit(submit)) {
            throw new IllegalArgumentException("Requester identity signature is invalid.");
        }
        return submit.getRequesterPublicKey();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private void logTerminalPersistenceDegraded(EmbarrassinglyParallelJob<?, ?> job,
                                                String operation,
                                                String policy) {
        logErrorEvent("job_terminal_persistence_degraded", fields(
                "operation", operation,
                "job_id", job.getJobId(),
                "policy", policy
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

    private static int taskIndex(String taskId) {
        int marker = taskId == null ? -1 : taskId.lastIndexOf('-');
        if (marker < 0 || marker == taskId.length() - 1) {
            return Integer.MAX_VALUE;
        }
        try {
            return Integer.parseInt(taskId.substring(marker + 1));
        } catch (NumberFormatException ignored) {
            return Integer.MAX_VALUE;
        }
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

    private record LeaseExpiryResult(boolean handled, String jobFailureReason) {
        private static LeaseExpiryResult notHandled() {
            return new LeaseExpiryResult(false, null);
        }

        private static LeaseExpiryResult handled(String jobFailureReason) {
            return new LeaseExpiryResult(true, jobFailureReason);
        }
    }
}
