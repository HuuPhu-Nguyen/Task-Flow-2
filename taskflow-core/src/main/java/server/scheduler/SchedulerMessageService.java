package server.scheduler;

import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
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

import java.util.Comparator;
import java.util.List;
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
            if (state.hasActiveJob(submit.getJobId())) {
                sendJobStartFailure(
                        envelope.fromNodeId(),
                        submit,
                        "Job id is already active: " + submit.getJobId()
                );
                return;
            }
            JobStateStore store = persistence.store();
            if (store != null && store.hasJob(submit.getJobId())) {
                sendJobStartFailure(
                        envelope.fromNodeId(),
                        submit,
                        "Job id already exists in persisted history: " + submit.getJobId()
                );
                return;
            }
            String requesterTokenHash = RequesterTokens.hashToken(submit.getRequesterToken());
            if (!RequesterTokens.hasTokenHash(requesterTokenHash)) {
                throw new IllegalArgumentException("Requester token is required.");
            }
            String requesterIdentityKey = requesterIdentityKey(submit);
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
            persistJobStartup(job, submit.getParameter(), requesterTokenHash, requesterIdentityKey);
            state.addActiveJob(job, requesterTokenHash, requesterIdentityKey);
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

    private void persistJobStartup(EmbarrassinglyParallelJob<?, ?> job,
                                   String parameter,
                                   String requesterTokenHash,
                                   String requesterIdentityKey) {
        JobStateStore store = persistence.store();
        if (store == null) {
            return;
        }
        boolean persisted = store.insertJobWithTasks(
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
            persistence.record("insertJobWithTasks", job.getJobId(), "", false);
            throw new IllegalStateException("Job could not be persisted.");
        }
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
