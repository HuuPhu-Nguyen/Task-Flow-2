package server.scheduler;

import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.JobSubmissionHasher;
import protocol.Message;
import protocol.MessageValidationException;
import protocol.MessageValidator;
import protocol.PeerDisconnectedMessage;
import protocol.RequesterIdentity;
import protocol.RequesterTokens;
import protocol.TaskResultMessage;
import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.model.MessageEnvelope;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TransitionDecision;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Owns envelope disposition, J0 submission, result requests, and event routing. */
final class SchedulerMessageService {
    private final SchedulerState state;
    private final SchedulerPersistence persistence;
    private final SchedulerOutput output;
    private final TaskFlowClock clock;
    private final AssignmentIdGenerator assignmentIdGenerator;
    private final SchedulerMetrics metrics;
    private final TaskTransitionDecisions transitions;
    private final ResultCommitService resultCommits;
    private final LeaseService leases;
    private final JobCompletionService jobCompletions;
    private final SchedulerEventLog events;

    SchedulerMessageService(SchedulerState state,
                            SchedulerPersistence persistence,
                            SchedulerOutput output,
                            TaskFlowClock clock,
                            AssignmentIdGenerator assignmentIdGenerator,
                            SchedulerMetrics metrics,
                            TaskTransitionDecisions transitions,
                            ResultCommitService resultCommits,
                            LeaseService leases,
                            JobCompletionService jobCompletions,
                            SchedulerEventLog events) {
        this.state = state;
        this.persistence = persistence;
        this.output = output;
        this.clock = clock;
        this.assignmentIdGenerator = assignmentIdGenerator;
        this.metrics = metrics;
        this.transitions = transitions;
        this.resultCommits = resultCommits;
        this.leases = leases;
        this.jobCompletions = jobCompletions;
        this.events = events;
    }

    void processEnvelope(MessageEnvelope envelope) {
        try {
            handleMessage(envelope);
        } catch (MessageValidationException e) {
            events.error("scheduler_message_validation_failed", events.fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "reason_code", e.reasonCode(),
                    "action", "reject",
                    "error", e.getMessage()
            ));
            rejectEnvelope(envelope);
            return;
        } catch (Exception e) {
            events.error("scheduler_message_processing_failed", events.fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "error", e.getMessage()
            ));
            requeueEnvelope(envelope);
            return;
        }
        ackEnvelope(envelope);
    }

    private void handleMessage(MessageEnvelope envelope) throws Exception {
        Message message = envelope.message();
        if (message instanceof JobSubmitMessage submit) {
            handleJobSubmit(envelope, submit);
        } else if (message instanceof TaskResultMessage result) {
            MessageValidator.validate(result);
            resultCommits.handleTaskResult(envelope, result);
        } else if (message instanceof JobResultRequestMessage request) {
            handleJobResultRequest(envelope, request);
        } else if (message instanceof PeerDisconnectedMessage disconnected) {
            MessageValidator.validate(disconnected);
            String peerId = disconnected.getNodeId();
            if (peerId == null || peerId.isBlank()) {
                peerId = envelope.fromNodeId();
            }
            leases.handlePeerUnavailable(peerId, disconnected.getReason());
        }
    }

    private void handleJobSubmit(MessageEnvelope envelope, JobSubmitMessage submit) throws Exception {
        try {
            MessageValidator.validate(submit);
            if (!Objects.equals(envelope.fromNodeId(), submit.getNodeId())) {
                throw new IllegalArgumentException(
                        "Requester route does not match job submission node id."
                );
            }
            String requesterTokenHash = RequesterTokens.hashToken(submit.getRequesterToken());
            if (!RequesterTokens.hasTokenHash(requesterTokenHash)) {
                throw new IllegalArgumentException("Requester token is required.");
            }
            String requesterIdentityKey = requesterIdentityKey(submit);
            String requestHash = JobSubmissionHasher.hash(
                    submit,
                    requesterTokenHash,
                    requesterIdentityKey
            );

            JobStateStore store = persistence.store();
            JobStateStore.JobSubmissionDecision preflight = inspectSubmission(
                    store,
                    submit.getJobId(),
                    requesterTokenHash,
                    requesterIdentityKey,
                    requestHash
            );
            if (handleExistingSubmission(envelope.fromNodeId(), submit, preflight)) {
                return;
            }

            long acceptedAt = clock.nowEpochMillis();
            TransitionDecision decision = transitions.jobSubmitted(acceptedAt);
            if (!decision.accepted()) {
                throw new IllegalStateException("Job submission was rejected by the state machine.");
            }

            EmbarrassinglyParallelJob<?, ?> job = JobFactory.create(submit, envelope.fromNodeId());
            job.initializeTasks(submit);
            job.configureTransitionPorts(clock, assignmentIdGenerator);
            if (job.getTasks().isEmpty()) {
                throw new IllegalArgumentException("Job must create at least one task.");
            }
            JobStateStore.JobSubmissionDecision persisted = persistJobStartup(
                    job,
                    submit.getParameter(),
                    requesterTokenHash,
                    requesterIdentityKey,
                    requestHash
            );
            if (persisted.outcome() == JobStateStore.JobSubmissionOutcome.STORAGE_FAILURE) {
                throw new IllegalStateException("Job could not be persisted.");
            }
            if (handleExistingSubmission(envelope.fromNodeId(), submit, persisted)) {
                return;
            }
            if (persisted.outcome() != JobStateStore.JobSubmissionOutcome.COMMITTED) {
                throw new IllegalStateException("Job could not be persisted.");
            }
            state.addActiveJob(job, requesterTokenHash, requesterIdentityKey, requestHash);
            metrics.setActiveJobs(state.activeJobCount());
            events.info("job_started", events.fields(
                    "job_id", job.getJobId(),
                    "task_type", job.getTaskType(),
                    "requester_id", job.getRequesterNodeId(),
                    "task_count", job.getTasks().size()
            ));
        } catch (Exception e) {
            events.error("job_start_failed", events.fields(
                    "job_id", submit.getJobId(),
                    "task_type", submit.getTaskType(),
                    "requester_id", envelope.fromNodeId(),
                    "error", e.getMessage()
            ));
            sendJobStartFailure(envelope.fromNodeId(), submit, e.getMessage());
        }
    }

    private JobStateStore.JobSubmissionDecision persistJobStartup(EmbarrassinglyParallelJob<?, ?> job,
                                                                  String parameter,
                                                                  String requesterTokenHash,
                                                                  String requesterIdentityKey,
                                                                  String requestHash) {
        JobStateStore store = persistence.store();
        if (store == null) {
            return JobStateStore.JobSubmissionDecision.committed(job.getTaskType());
        }
        JobStateStore.JobSubmissionDecision persisted = store.commitJobSubmission(
                job.getJobId(),
                job.getTaskType(),
                job.getRequesterNodeId(),
                requesterTokenHash,
                requesterIdentityKey,
                requestHash,
                parameter,
                job.getTasks().values().stream()
                        .sorted(Comparator.comparingInt(task -> taskIndex(task.getTaskId())))
                        .map(task -> new JobStateStore.TaskStartupState(task.getTaskId(), task.getPayload()))
                        .toList()
        );
        if (persisted == null
                || persisted.outcome() == JobStateStore.JobSubmissionOutcome.STORAGE_FAILURE) {
            persistence.record("insertJobWithTasks", job.getJobId(), "", false);
            return JobStateStore.JobSubmissionDecision.storageFailure();
        }
        return persisted;
    }

    private JobStateStore.JobSubmissionDecision inspectSubmission(JobStateStore store,
                                                                   String jobId,
                                                                   String requesterTokenHash,
                                                                   String requesterIdentityKey,
                                                                   String requestHash) {
        if (store != null) {
            JobStateStore.JobSubmissionDecision durable = store.inspectJobSubmission(
                    jobId,
                    requesterTokenHash,
                    requesterIdentityKey,
                    requestHash
            );
            if (durable == null) {
                return JobStateStore.JobSubmissionDecision.storageFailure();
            }
            if (durable.outcome() != JobStateStore.JobSubmissionOutcome.NEW_SUBMISSION) {
                if (durable.outcome() == JobStateStore.JobSubmissionOutcome.LEGACY_CONFLICT
                        && state.hasActiveJob(jobId)
                        && hasText(state.requestHash(jobId))) {
                    return classifyActiveSubmission(
                            jobId,
                            requesterTokenHash,
                            requesterIdentityKey,
                            requestHash
                    );
                }
                return durable;
            }
        }
        if (state.hasActiveJob(jobId)) {
            return classifyActiveSubmission(
                    jobId,
                    requesterTokenHash,
                    requesterIdentityKey,
                    requestHash
            );
        }
        return JobStateStore.JobSubmissionDecision.newSubmission();
    }

    private JobStateStore.JobSubmissionDecision classifyActiveSubmission(String jobId,
                                                                          String requesterTokenHash,
                                                                          String requesterIdentityKey,
                                                                          String requestHash) {
        EmbarrassinglyParallelJob<?, ?> active = state.activeJob(jobId);
        String taskType = active == null ? "" : active.getTaskType();
        if (!secureEquals(state.requesterTokenHash(jobId), requesterTokenHash)
                || !Objects.equals(value(state.requesterIdentityKey(jobId)), value(requesterIdentityKey))) {
            return new JobStateStore.JobSubmissionDecision(
                    JobStateStore.JobSubmissionOutcome.OWNER_CONFLICT,
                    "RUNNING",
                    taskType
            );
        }
        if (!hasText(state.requestHash(jobId))) {
            return new JobStateStore.JobSubmissionDecision(
                    JobStateStore.JobSubmissionOutcome.LEGACY_CONFLICT,
                    "RUNNING",
                    taskType
            );
        }
        if (!secureEquals(state.requestHash(jobId), requestHash)) {
            return new JobStateStore.JobSubmissionDecision(
                    JobStateStore.JobSubmissionOutcome.REQUEST_CONFLICT,
                    "RUNNING",
                    taskType
            );
        }
        return new JobStateStore.JobSubmissionDecision(
                JobStateStore.JobSubmissionOutcome.REPLAY,
                "RUNNING",
                taskType
        );
    }

    private boolean handleExistingSubmission(String requesterNodeId,
                                             JobSubmitMessage submit,
                                             JobStateStore.JobSubmissionDecision decision) throws Exception {
        JobStateStore.JobSubmissionOutcome outcome = decision == null
                ? JobStateStore.JobSubmissionOutcome.STORAGE_FAILURE
                : decision.outcome();
        if (outcome == JobStateStore.JobSubmissionOutcome.NEW_SUBMISSION
                || outcome == JobStateStore.JobSubmissionOutcome.COMMITTED) {
            return false;
        }
        if (outcome == JobStateStore.JobSubmissionOutcome.STORAGE_FAILURE) {
            throw new IllegalStateException("Job submission state could not be persisted.");
        }
        if (outcome == JobStateStore.JobSubmissionOutcome.REPLAY) {
            events.info("job_submission_replayed", events.fields(
                    "job_id", submit.getJobId(),
                    "task_type", decision.taskType(),
                    "requester_id", requesterNodeId,
                    "job_status", decision.status()
            ));
            sendSubmissionReplay(requesterNodeId, submit, decision);
            return true;
        }

        String reason = switch (outcome) {
            case REQUEST_CONFLICT -> "Idempotency conflict: job id is already bound to a different request.";
            case OWNER_CONFLICT -> "Job id is owned by a different requester.";
            case LEGACY_CONFLICT -> "Job id already exists, but its legacy submission cannot be verified for idempotent replay.";
            default -> "Job id could not be accepted.";
        };
        events.info("job_submission_conflict", events.fields(
                "job_id", submit.getJobId(),
                "task_type", decision.taskType(),
                "requester_id", requesterNodeId,
                "outcome", outcome
        ));
        sendJobStartFailure(requesterNodeId, submit, reason);
        return true;
    }

    private void sendSubmissionReplay(String requesterNodeId,
                                      JobSubmitMessage submit,
                                      JobStateStore.JobSubmissionDecision decision) throws Exception {
        String jobId = submit.getJobId();
        JobResultMessage pending = jobCompletions.pendingResponse(jobId);
        if (pending != null) {
            sendRequestedJobResult(requesterNodeId, pending);
            return;
        }

        EmbarrassinglyParallelJob<?, ?> activeJob = state.activeJob(jobId);
        if (activeJob != null) {
            sendRequestedJobResult(requesterNodeId, runningJobResponse(jobId, activeJob.getTaskType()));
            return;
        }

        JobStateStore store = persistence.store();
        if (store != null) {
            Optional<JobStateStore.CompletedJobResultState> completedResult = store.loadCompletedJobResult(jobId);
            if (completedResult.isPresent()) {
                JobStateStore.CompletedJobResultState completed = completedResult.get();
                sendRequestedJobResult(requesterNodeId, new JobResultMessage(
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
        }

        String status = value(decision.status());
        String taskType = hasText(decision.taskType()) ? decision.taskType() : safeTaskType(submit.getTaskType());
        if ("RUNNING".equals(status) || "FINALIZING".equals(status)) {
            sendRequestedJobResult(requesterNodeId, runningJobResponse(jobId, taskType));
            return;
        }
        sendRequestedJobResult(requesterNodeId, new JobResultMessage(
                "COORDINATOR",
                clock.now().toString(),
                jobId,
                taskType,
                false,
                List.of(),
                "Job is terminal with status " + (status.isBlank() ? "UNKNOWN" : status) + "."
        ));
    }

    private JobResultMessage runningJobResponse(String jobId, String taskType) {
        return new JobResultMessage(
                "COORDINATOR",
                clock.now().toString(),
                jobId,
                taskType,
                false,
                List.of(),
                "Job is still running."
        );
    }

    private void handleJobResultRequest(MessageEnvelope envelope,
                                        JobResultRequestMessage request) throws Exception {
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

        JobResultMessage pending = jobCompletions.pendingResponse(jobId);
        if (pending != null) {
            if (!authorizeJobResultRequest(
                    requesterId,
                    request,
                    jobId,
                    pending.getTaskType(),
                    state.requesterTokenHash(jobId),
                    state.requesterIdentityKey(jobId)
            )) {
                return;
            }
            sendRequestedJobResult(requesterId, pending);
            return;
        }

        EmbarrassinglyParallelJob<?, ?> activeJob = state.activeJob(jobId);
        if (activeJob != null) {
            if (!authorizeJobResultRequest(
                    requesterId,
                    request,
                    jobId,
                    activeJob.getTaskType(),
                    state.requesterTokenHash(jobId),
                    state.requesterIdentityKey(jobId)
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

        JobStateStore store = persistence.store();
        if (store == null) {
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

        Optional<JobStateStore.CompletedJobResultState> result = store.loadCompletedJobResult(jobId);
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

    private void sendJobStartFailure(String requesterNodeId,
                                     JobSubmitMessage submit,
                                     String reason) throws Exception {
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
                events.error("job_start_failure_requester_missing", events.fields(
                        "job_id", submit.getJobId(),
                        "requester_id", requesterNodeId
                ));
                throw new IllegalStateException(
                        "Job start failure result was not routed to requester " + requesterNodeId
                );
            }
        } catch (Exception sendError) {
            events.error("job_start_failure_send_failed", events.fields(
                    "job_id", submit.getJobId(),
                    "requester_id", requesterNodeId,
                    "error", sendError.getMessage()
            ));
            throw sendError;
        }
    }

    private void sendRequestedJobResult(String requesterNodeId, JobResultMessage response) throws Exception {
        try {
            if (!output.sendJobResult(requesterNodeId, response)) {
                events.error("job_result_requester_missing", events.fields(
                        "job_id", response.getJobId(),
                        "requester_id", requesterNodeId
                ));
                throw new IllegalStateException(
                        "Requested job result was not routed to requester " + requesterNodeId
                );
            }
        } catch (Exception sendError) {
            events.error("job_result_request_send_failed", events.fields(
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

    private void ackEnvelope(MessageEnvelope envelope) {
        if (envelope.acknowledgement() == null) {
            return;
        }
        try {
            envelope.acknowledgement().ack();
        } catch (Exception e) {
            preserveInterrupt(e);
            events.error("scheduler_message_ack_failed", events.fields(
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
            preserveInterrupt(e);
            events.error("scheduler_message_requeue_failed", events.fields(
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
            preserveInterrupt(e);
            events.error("scheduler_message_reject_failed", events.fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "error", e.getMessage()
            ));
        }
    }

    private static void preserveInterrupt(Exception error) {
        if (error instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private static String messageType(MessageEnvelope envelope) {
        Message message = envelope.message();
        return message == null ? "null" : String.valueOf(message.getType());
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

    private static String requesterIdentityKey(JobSubmitMessage submit) {
        if (RequesterIdentity.hasPartialIdentity(submit.getRequesterPublicKey(), submit.getRequesterSignature())) {
            throw new IllegalArgumentException(
                    "Requester identity public key and signature are both required when identity is provided."
            );
        }
        if (!RequesterIdentity.hasIdentity(submit.getRequesterPublicKey(), submit.getRequesterSignature())) {
            return "";
        }
        if (!RequesterIdentity.verifyJobSubmit(submit)) {
            throw new IllegalArgumentException("Requester identity signature is invalid.");
        }
        return submit.getRequesterPublicKey();
    }

    private static boolean secureEquals(String left, String right) {
        return MessageDigest.isEqual(
                value(left).getBytes(StandardCharsets.UTF_8),
                value(right).getBytes(StandardCharsets.UTF_8)
        );
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String value(String value) {
        return value == null ? "" : value;
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
}
