package server.scheduler;

import plugin.RetrySafety;
import protocol.AdmissionRejection;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.JobSubmissionHasher;
import protocol.Message;
import protocol.MessageValidationException;
import protocol.MessageValidator;
import protocol.PayloadLimits;
import protocol.PeerDisconnectedMessage;
import protocol.RequesterIdentity;
import protocol.RequesterTokens;
import protocol.TaskResultMessage;
import server.db.JobStateStore;
import server.db.BrokerOutboxStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.model.MessageEnvelope;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TransitionDecision;
import transport.ClassifiedDeliveryFailure;
import transport.DeliveryDisposition;
import transport.DeliveryFailureClassifier;
import transport.TransientDeliveryException;

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
    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final AssignmentIdGenerator assignmentIdGenerator;
    private final SchedulerMetrics metrics;
    private final TaskTransitionDecisions transitions;
    private final ResultCommitService resultCommits;
    private final LeaseService leases;
    private final JobCompletionService jobCompletions;
    private final SchedulerOverloadStatus overloadStatus;
    private final SchedulerEventLog events;

    SchedulerMessageService(SchedulerState state,
                            SchedulerPersistence persistence,
                            SchedulerOutput output,
                            SchedulerConfig config,
                            TaskFlowClock clock,
                            AssignmentIdGenerator assignmentIdGenerator,
                            SchedulerMetrics metrics,
                            TaskTransitionDecisions transitions,
                            ResultCommitService resultCommits,
                            LeaseService leases,
                            JobCompletionService jobCompletions,
                            SchedulerOverloadStatus overloadStatus,
                            SchedulerEventLog events) {
        this.state = state;
        this.persistence = persistence;
        this.output = output;
        this.config = Objects.requireNonNull(config, "config");
        this.clock = clock;
        this.assignmentIdGenerator = assignmentIdGenerator;
        this.metrics = metrics;
        this.transitions = transitions;
        this.resultCommits = resultCommits;
        this.leases = leases;
        this.jobCompletions = jobCompletions;
        this.overloadStatus = overloadStatus;
        this.events = events;
    }

    void processEnvelope(MessageEnvelope envelope) {
        DeliveryDisposition disposition;
        String reasonCode;
        try {
            disposition = handleMessage(envelope);
            reasonCode = reasonCode(disposition);
        } catch (MessageValidationException e) {
            disposition = DeliveryDisposition.REJECT_INVALID;
            reasonCode = e.reasonCode();
            events.error("scheduler_message_validation_failed", events.fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "reason_code", e.reasonCode(),
                    "disposition", disposition,
                    "error", e.getMessage()
            ));
        } catch (Exception e) {
            preserveInterrupt(e);
            ClassifiedDeliveryFailure failure = DeliveryFailureClassifier.classify(e);
            disposition = failure.disposition();
            reasonCode = failure.reasonCode();
            events.error("scheduler_message_processing_failed", events.fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "reason_code", reasonCode,
                    "disposition", disposition,
                    "error", e.getMessage()
            ));
        }
        settleEnvelope(envelope, disposition, reasonCode);
    }

    private static String reasonCode(DeliveryDisposition disposition) {
        return switch (disposition) {
            case ACK_SUCCESS -> "handled";
            case ACK_DUPLICATE_OR_STALE -> "duplicate_or_stale_domain_event";
            case RETRY_TRANSIENT -> "transient_infrastructure_failure";
            case REJECT_INVALID -> "invalid_delivery";
            case QUARANTINE_POISON -> "deterministic_processing_failure";
        };
    }

    private DeliveryDisposition handleMessage(MessageEnvelope envelope) throws Exception {
        Message message = envelope.message();
        if (message instanceof JobSubmitMessage submit) {
            handleJobSubmit(envelope, submit);
            return DeliveryDisposition.ACK_SUCCESS;
        } else if (message instanceof TaskResultMessage result) {
            MessageValidator.validate(result);
            return resultCommits.handleTaskResult(envelope, result);
        } else if (message instanceof JobResultRequestMessage request) {
            handleJobResultRequest(envelope, request);
            return DeliveryDisposition.ACK_SUCCESS;
        } else if (message instanceof PeerDisconnectedMessage disconnected) {
            MessageValidator.validate(disconnected);
            String peerId = disconnected.getNodeId();
            if (peerId == null || peerId.isBlank()) {
                peerId = envelope.fromNodeId();
            }
            leases.handlePeerUnavailable(peerId, disconnected.getReason());
            return DeliveryDisposition.ACK_SUCCESS;
        }
        throw new MessageValidationException(
                "unsupported_scheduler_message_type",
                "Scheduler does not handle message type " + messageType(envelope) + "."
        );
    }

    private void handleJobSubmit(MessageEnvelope envelope, JobSubmitMessage submit) throws Exception {
        boolean failureResponseAttempted = false;
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

            BrokerOutboxStore.PendingOutboxCount pendingOutbox = pendingOutboxCount(store);
            AdmissionPolicy.Decision earlyAdmission = evaluateAdmission(
                    submit.getTaskPayloads().size(),
                    pendingOutbox
            );
            if (!earlyAdmission.allowedDecision()) {
                failureResponseAttempted = true;
                rejectAdmission(envelope.fromNodeId(), submit, earlyAdmission);
                return;
            }

            RetrySafety retrySafety = validatePluginRetrySafety(submit.getTaskType());
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
            long pluginTaskCount = job.getTasks().size();
            if (pluginTaskCount > PayloadLimits.maxTasksPerJob()) {
                failureResponseAttempted = true;
                sendAdmissionRejection(
                        envelope.fromNodeId(),
                        submit,
                        new AdmissionRejection(
                                AdmissionRejection.Limit.MAX_TASKS_PER_JOB,
                                PayloadLimits.maxTasksPerJob(),
                                pluginTaskCount
                        )
                );
                return;
            }
            AdmissionPolicy.Decision finalAdmission = evaluateAdmission(
                    pluginTaskCount,
                    pendingOutbox
            );
            if (!finalAdmission.allowedDecision()) {
                failureResponseAttempted = true;
                rejectAdmission(envelope.fromNodeId(), submit, finalAdmission);
                return;
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
            metrics.setActiveTasks(state.activeTaskCount());
            overloadStatus.refreshActive(state.activeJobCount(), state.activeTaskCount());
            events.info("job_started", events.fields(
                    "job_id", job.getJobId(),
                    "task_type", job.getTaskType(),
                    "requester_id", job.getRequesterNodeId(),
                    "task_count", job.getTasks().size(),
                    "retry_safety", retrySafety
            ));
        } catch (Exception e) {
            events.error("job_start_failed", events.fields(
                    "job_id", submit.getJobId(),
                    "task_type", submit.getTaskType(),
                    "requester_id", envelope.fromNodeId(),
                    "error", e.getMessage()
            ));
            if (failureResponseAttempted) {
                throw e;
            }
            sendJobStartFailure(
                    envelope.fromNodeId(),
                    submit,
                    e.getMessage(),
                    admissionRejectionFor(submit, e)
            );
        }
    }

    private AdmissionPolicy.Decision evaluateAdmission(
            long candidateTasks,
            BrokerOutboxStore.PendingOutboxCount pendingOutbox) {
        BrokerOutboxStore.PendingOutboxCount normalized = pendingOutbox == null
                ? BrokerOutboxStore.PendingOutboxCount.storageFailure()
                : pendingOutbox;
        return AdmissionPolicy.evaluate(
                state.activeJobCount(),
                state.activeTaskCount(),
                candidateTasks,
                normalized.count(),
                normalized.counted(),
                config
        );
    }

    private BrokerOutboxStore.PendingOutboxCount pendingOutboxCount(JobStateStore store) {
        if (!(store instanceof BrokerOutboxStore outboxStore)) {
            BrokerOutboxStore.PendingOutboxCount count =
                    BrokerOutboxStore.PendingOutboxCount.counted(0L);
            overloadStatus.refreshPendingOutbox(count);
            return count;
        }
        BrokerOutboxStore.PendingOutboxCount count;
        try {
            count = outboxStore.countPendingBrokerOutbox();
        } catch (RuntimeException e) {
            events.error("scheduler_pending_outbox_count_failed", events.fields(
                    "error", e.getMessage()
            ));
            count = BrokerOutboxStore.PendingOutboxCount.storageFailure();
        }
        BrokerOutboxStore.PendingOutboxCount normalized = count == null
                ? BrokerOutboxStore.PendingOutboxCount.storageFailure()
                : count;
        overloadStatus.refreshPendingOutbox(normalized);
        return normalized;
    }

    private void rejectAdmission(String requesterNodeId,
                                 JobSubmitMessage submit,
                                 AdmissionPolicy.Decision decision) throws Exception {
        if (decision.outcome() == AdmissionPolicy.Outcome.STORAGE_FAILURE) {
            sendJobStartFailure(
                    requesterNodeId,
                    submit,
                    "Job admission could not read the pending broker outbox count.",
                    null
            );
            return;
        }
        sendAdmissionRejection(requesterNodeId, submit, decision.rejection());
    }

    private void sendAdmissionRejection(String requesterNodeId,
                                        JobSubmitMessage submit,
                                        AdmissionRejection rejection) throws Exception {
        events.info("job_admission_rejected", events.fields(
                "job_id", submit.getJobId(),
                "task_type", submit.getTaskType(),
                "requester_id", requesterNodeId,
                "limit", rejection.limit(),
                "configured_maximum", rejection.configuredMaximum(),
                "observed_value", rejection.observedValue()
        ));
        sendJobStartFailure(
                requesterNodeId,
                submit,
                "Job admission rejected by " + rejection.limit()
                        + ": configured maximum " + rejection.configuredMaximum()
                        + ", observed " + rejection.observedValue() + ".",
                rejection
        );
    }

    private static AdmissionRejection admissionRejectionFor(
            JobSubmitMessage submit,
            Exception error) {
        if (!(error instanceof MessageValidationException validation)) {
            return null;
        }
        return switch (validation.reasonCode()) {
            case MessageValidator.REASON_MAX_TASKS_PER_JOB -> new AdmissionRejection(
                    AdmissionRejection.Limit.MAX_TASKS_PER_JOB,
                    PayloadLimits.maxTasksPerJob(),
                    submit.getTaskPayloads().size()
            );
            case MessageValidator.REASON_MAX_INLINE_MESSAGE_BYTES -> new AdmissionRejection(
                    AdmissionRejection.Limit.MAX_INLINE_MESSAGE_BYTES,
                    PayloadLimits.maxJobPayloadBytes(),
                    PayloadLimits.jobPayloadJsonBytes(
                            submit.getTaskPayloads(),
                            submit.getParameter()
                    )
            );
            case MessageValidator.REASON_MAX_INLINE_PAYLOAD_BYTES -> new AdmissionRejection(
                    AdmissionRejection.Limit.MAX_INLINE_PAYLOAD_BYTES,
                    PayloadLimits.maxInlinePayloadBytes(),
                    PayloadLimits.maximumInlinePayloadBytes(submit.getTaskPayloads())
            );
            case MessageValidator.REASON_MAX_REFERENCED_PAYLOAD_BYTES -> new AdmissionRejection(
                    AdmissionRejection.Limit.MAX_REFERENCED_PAYLOAD_BYTES,
                    PayloadLimits.maxInputBytes(),
                    PayloadLimits.maximumReferencedPayloadBytes(submit.getTaskPayloads())
            );
            default -> null;
        };
    }

    private RetrySafety validatePluginRetrySafety(String taskType) {
        RetrySafety retrySafety = JobFactory.retrySafety(taskType);
        if (!retrySafety.permitsAutomaticRetry() && config.maxTaskRetries() > 0) {
            throw new IllegalArgumentException(
                    "Task plugin " + safeTaskType(taskType) + " declares UNSAFE_TO_RETRY and cannot be "
                            + "accepted while maxTaskRetries is " + config.maxTaskRetries() + "."
            );
        }
        return retrySafety;
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
        sendJobStartFailure(requesterNodeId, submit, reason, null);
    }

    private void sendJobStartFailure(String requesterNodeId,
                                     JobSubmitMessage submit,
                                     String reason,
                                     AdmissionRejection admissionRejection) throws Exception {
        String errorMessage = reason == null || reason.isBlank()
                ? "Job could not be started."
                : reason;
        JobResultMessage response = admissionRejection == null
                ? new JobResultMessage(
                "COORDINATOR",
                clock.now().toString(),
                safeJobResultId(submit.getJobId()),
                safeTaskType(submit.getTaskType()),
                false,
                List.of(),
                errorMessage
        )
                : JobResultMessage.admissionRejected(
                "COORDINATOR",
                clock.now().toString(),
                safeJobResultId(submit.getJobId()),
                safeTaskType(submit.getTaskType()),
                errorMessage,
                admissionRejection
        );
        try {
            if (!output.sendJobResult(requesterNodeId, response)) {
                events.error("job_start_failure_requester_missing", events.fields(
                        "job_id", submit.getJobId(),
                        "requester_id", requesterNodeId
                ));
                throw new TransientDeliveryException(
                        "job_start_failure_unroutable",
                        "Job start failure result was not routed to requester " + requesterNodeId,
                        null
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
                throw new TransientDeliveryException(
                        "job_result_unroutable",
                        "Requested job result was not routed to requester " + requesterNodeId,
                        null
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

    private void settleEnvelope(MessageEnvelope envelope,
                                DeliveryDisposition disposition,
                                String reasonCode) {
        if (envelope.acknowledgement() == null) {
            return;
        }
        try {
            envelope.acknowledgement().settle(disposition, reasonCode);
            if (disposition != DeliveryDisposition.ACK_SUCCESS) {
                events.info("scheduler_delivery_disposed", events.fields(
                        "message_type", messageType(envelope),
                        "from_node_id", envelope.fromNodeId(),
                        "reason_code", reasonCode,
                        "disposition", disposition
                ));
            }
        } catch (Exception e) {
            preserveInterrupt(e);
            events.error("scheduler_message_settlement_failed", events.fields(
                    "message_type", messageType(envelope),
                    "from_node_id", envelope.fromNodeId(),
                    "reason_code", reasonCode,
                    "disposition", disposition,
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
