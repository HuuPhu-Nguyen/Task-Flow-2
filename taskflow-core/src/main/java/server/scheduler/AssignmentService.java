package server.scheduler;

import protocol.MessageValidator;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.db.JobStateStore;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.transition.TransitionDecision;

import java.util.Comparator;
import java.util.List;

/** Owns T1 placement, assignment persistence, projection, and outbound intent. */
final class AssignmentService {
    private final SchedulerState state;
    private final PeerRegistry registry;
    private final SchedulerPersistence persistence;
    private final SchedulerOutput output;
    private final SchedulerConfig config;
    private final String leaseOwnerId;
    private final TaskFlowClock clock;
    private final AssignmentIdGenerator assignmentIdGenerator;
    private final SchedulerMetrics metrics;
    private final SchedulerOutboxService outbox;
    private final TaskTransitionDecisions transitions;
    private final JobCompletionService jobCompletions;
    private final SchedulerEventLog events;

    AssignmentService(SchedulerState state,
                      PeerRegistry registry,
                      SchedulerPersistence persistence,
                      SchedulerOutput output,
                      SchedulerConfig config,
                      String leaseOwnerId,
                      TaskFlowClock clock,
                      AssignmentIdGenerator assignmentIdGenerator,
                      SchedulerMetrics metrics,
                      SchedulerOutboxService outbox,
                      TaskTransitionDecisions transitions,
                      JobCompletionService jobCompletions,
                      SchedulerEventLog events) {
        this.state = state;
        this.registry = registry;
        this.persistence = persistence;
        this.output = output;
        this.config = config;
        this.leaseOwnerId = leaseOwnerId;
        this.clock = clock;
        this.assignmentIdGenerator = assignmentIdGenerator;
        this.metrics = metrics;
        this.outbox = outbox;
        this.transitions = transitions;
        this.jobCompletions = jobCompletions;
        this.events = events;
    }

    void dispatchPendingTasks() {
        for (EmbarrassinglyParallelJob<?, ?> job : state.activeJobsSnapshot()) {
            if (!state.hasActiveJob(job.getJobId()) || jobCompletions.isPending(job.getJobId())) {
                continue;
            }
            List<PeerInfo> candidates = getAvailablePeers(job.getTaskType());
            if (candidates.isEmpty()) {
                continue;
            }

            List<? extends TaskUnit<?>> pending = job.getPendingTasks().stream()
                    .sorted(Comparator.comparingInt((TaskUnit<?> task) -> task.getRetryCount()).reversed())
                    .toList();

            for (TaskUnit<?> task : pending) {
                PeerInfo bestPeer = candidates.stream()
                        .filter(peer -> peer.getActiveTasks() < config.maxTasksPerPeer())
                        .findFirst()
                        .orElse(null);
                if (bestPeer == null) {
                    break;
                }
                assign(job, task, bestPeer);
                if (!state.hasActiveJob(job.getJobId()) || jobCompletions.isPending(job.getJobId())) {
                    break;
                }
            }
        }
    }

    private void assign(EmbarrassinglyParallelJob<?, ?> job, TaskUnit<?> task, PeerInfo peer) {
        long pendingSince = task.getPendingSinceMillis();
        long startedAt = clock.nowEpochMillis();
        long leaseExpiresAt = leaseExpiresAt(startedAt);
        long dispatchLatencyMs = pendingSince > 0L ? Math.max(0L, startedAt - pendingSince) : 0L;
        if (outbox.available()) {
            assignWithBrokerOutbox(job, task, peer, startedAt, leaseExpiresAt, dispatchLatencyMs);
            return;
        }

        AssignmentIdentity assignmentIdentity;
        TaskAssignMessage message;
        try {
            assignmentIdentity = AssignmentIdentity.create(
                    task.getTaskId(),
                    Math.incrementExact(task.getAttemptNumber()),
                    peer.getNodeId(),
                    leaseExpiresAt,
                    assignmentIdGenerator
            );
            TransitionDecision decision = transitions.assignmentRequested(
                    task,
                    assignmentIdentity,
                    leaseOwnerId,
                    startedAt
            );
            if (!decision.accepted()) {
                return;
            }
            message = job.createTaskAssignMessage(task).withAssignmentIdentity(
                    assignmentIdentity.attemptNumber(),
                    assignmentIdentity.assignmentId(),
                    assignmentIdentity.leaseExpiresAtEpochMillis()
            );
            MessageValidator.validate(message);
        } catch (RuntimeException e) {
            jobCompletions.failJob(job, "Task assignment could not be prepared: " + e.getMessage());
            return;
        }

        JobStateStore store = persistence.store();
        if (store != null) {
            JobStateStore.DurableTransitionOutcome durableOutcome = persistence.record(
                    "commitTaskAssignment",
                    job.getJobId(),
                    task.getTaskId(),
                    store.commitTaskAssignment(
                            task.getTaskId(),
                            peer.getNodeId(),
                            startedAt,
                            leaseOwnerId,
                            leaseExpiresAt,
                            assignmentIdentity.attemptNumber(),
                            assignmentIdentity.assignmentId()
                    )
            );
            if (!durableOutcome.projectionAllowed()) {
                if (durableStorageFailed(durableOutcome)) {
                    jobCompletions.failJob(job, persistence.failureReason("markTaskAssigned"));
                }
                return;
            }
        }

        if (!task.markAssigned(assignmentIdentity, startedAt, leaseOwnerId)) {
            jobCompletions.failJob(
                    job,
                    "Committed task assignment could not be installed in memory."
            );
            return;
        }

        metrics.recordAssignmentGeneration(dispatchLatencyMs);
        peer.incrementTasks();
        events.info("task_assignment_created", events.assignmentTraceFields(
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
            long failedAt = clock.nowEpochMillis();
            JobStateStore.DurableTransitionOutcome releaseOutcome = store == null
                    ? JobStateStore.DurableTransitionOutcome.COMMITTED
                    : persistence.record(
                            "commitAssignedTaskFailure:dispatch",
                            job.getJobId(),
                            task.getTaskId(),
                            store.commitAssignedTaskFailure(
                                    task.getTaskId(),
                                    assignmentIdentity.attemptNumber(),
                                    assignmentIdentity.assignmentId(),
                                    assignmentIdentity.workerId(),
                                    task.getRetryCount(),
                                    JobStateStore.TaskAttemptOutcome.DISPATCH_FAILED,
                                    e.getMessage(),
                                    failedAt
                            )
                    );
            if (!releaseOutcome.projectionAllowed()) {
                if (durableStorageFailed(releaseOutcome)) {
                    jobCompletions.failJob(
                            job,
                            persistence.failureReason("markTaskRetried")
                    );
                }
                return;
            }
            task.resetToPending();
            peer.decrementTasks();
            events.error("task_dispatch_failed", events.fields(
                    "job_id", job.getJobId(),
                    "task_id", task.getTaskId(),
                    "peer_id", peer.getNodeId(),
                    "error", e.getMessage()
            ));
        }
    }

    private void assignWithBrokerOutbox(EmbarrassinglyParallelJob<?, ?> job,
                                        TaskUnit<?> task,
                                        PeerInfo peer,
                                        long startedAt,
                                        long leaseExpiresAt,
                                        long dispatchLatencyMs) {
        BrokerOutboxStore outboxStore = outbox.store();
        BrokerOutboxPublisher outboxPublisher = outbox.publisher();
        if (outboxStore == null || outboxPublisher == null) {
            throw new IllegalStateException("Broker outbox assignment path is not configured.");
        }

        BrokerOutboxStore.OutboxMessage outboxTemplate;
        try {
            TaskAssignMessage messageTemplate = job.createTaskAssignMessage(task);
            outboxTemplate = outboxPublisher.taskAssignmentOutboxMessage(peer, messageTemplate);
        } catch (RuntimeException e) {
            jobCompletions.failJob(
                    job,
                    "Broker outbox task assignment could not be prepared: " + e.getMessage()
            );
            return;
        }

        BrokerOutboxStore.TaskAssignmentCommit assignmentCommit =
                outboxStore.commitTaskAssignmentAndEnqueueBrokerOutbox(
                        task.getTaskId(),
                        peer.getNodeId(),
                        startedAt,
                        leaseOwnerId,
                        leaseExpiresAt,
                        assignmentIdGenerator.nextAssignmentId(),
                        outboxTemplate
                );
        if (assignmentCommit == null) {
            assignmentCommit = new BrokerOutboxStore.TaskAssignmentCommit(
                    JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE,
                    null
            );
        }
        JobStateStore.DurableTransitionOutcome durableOutcome = persistence.record(
                "createTaskAssignmentAndEnqueueBrokerOutbox",
                job.getJobId(),
                task.getTaskId(),
                assignmentCommit.outcome()
        );
        if (!durableOutcome.projectionAllowed() || assignmentCommit.assignment() == null) {
            if (durableStorageFailed(durableOutcome)) {
                jobCompletions.failJob(
                        job,
                        persistence.failureReason("createTaskAssignmentAndEnqueueBrokerOutbox")
                );
            }
            return;
        }

        BrokerOutboxStore.CommittedTaskAssignment committed = assignmentCommit.assignment();
        try {
            TransitionDecision decision = transitions.assignmentRequested(
                    task,
                    committed.identity(),
                    leaseOwnerId,
                    startedAt
            );
            if (!decision.accepted()
                    || !task.markAssigned(committed.identity(), startedAt, leaseOwnerId)) {
                throw new IllegalStateException("Task was no longer pending after assignment commit.");
            }
        } catch (RuntimeException e) {
            jobCompletions.failJob(
                    job,
                    "Committed broker assignment could not be installed in memory: " + e.getMessage()
            );
            return;
        }

        metrics.recordAssignmentGeneration(dispatchLatencyMs);
        peer.incrementTasks();
        BrokerOutboxStore.OutboxRecord outboxRecord = committed.outboxRecord();
        boolean published = outbox.publish(outboxRecord);
        events.info("task_assignment_created", events.assignmentTraceFields(
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
        return Long.MAX_VALUE - startedAt < leaseMillis
                ? Long.MAX_VALUE
                : startedAt + leaseMillis;
    }

    private List<PeerInfo> getAvailablePeers(String taskType) {
        // RabbitMQ participants are eligible while present in the live registry;
        // heartbeat timeout removes them before a later dispatch cycle.
        return registry.getAllPeers().stream()
                .filter(peer -> peer.supportsTaskType(taskType))
                .filter(peer -> peer.getActiveTasks() < config.maxTasksPerPeer())
                .sorted(Comparator.comparingDouble(PeerInfo::getSelectionScore))
                .toList();
    }

    private static boolean durableStorageFailed(JobStateStore.DurableTransitionOutcome outcome) {
        return outcome == JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE
                || outcome == JobStateStore.DurableTransitionOutcome.UNKNOWN_ENTITY;
    }
}
