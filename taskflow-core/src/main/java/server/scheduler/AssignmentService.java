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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/** Owns T1 placement, assignment persistence, projection, and outbound intent. */
final class AssignmentService {
    private static final long NO_CAPACITY_RECHECK_MILLIS = 500L;

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
    private int runnableJobsRemainingInRound;
    private long roundCapacitySignalGeneration;
    private long capacitySignalGeneration;
    private long observedCapacityAvailabilityVersion;
    private long completedRounds;
    private boolean roundOpen;
    private long nextCapacityRecheckAtMillis = Long.MAX_VALUE;

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
        this.observedCapacityAvailabilityVersion = registry.capacityAvailabilityVersion();
    }

    SchedulerLoop.StageResult dispatchPendingTasks(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }

        observeCapacityAvailabilityChange();
        activateTimedCapacityRecheck();
        int attempts = 0;
        while (attempts < limit) {
            if (!roundOpen && !startRound()) {
                break;
            }
            EmbarrassinglyParallelJob<?, ?> job = pollNextJobInRound();
            if (job == null) {
                completeRound();
                continue;
            }
            if (!state.hasActiveJob(job.getJobId()) || jobCompletions.isPending(job.getJobId())) {
                attempts++;
                completeRoundIfExhausted();
                continue;
            }

            int assignmentsForJob = 0;
            boolean waitingForCapacity = false;
            boolean turnComplete = false;
            while (attempts < limit
                    && assignmentsForJob < config.schedulerMaxAssignmentsPerJobPerRound()
                    && !turnComplete) {
                PeerInfo bestPeer = bestAvailablePeer(job.getTaskType());
                attempts++;
                if (bestPeer == null) {
                    state.waitForCapacity(job.getJobId(), capacitySignalGeneration);
                    scheduleCapacityRecheck();
                    waitingForCapacity = true;
                    break;
                }

                TaskUnit<?> task = state.pollPendingTask(job.getJobId());
                if (task == null) {
                    break;
                }
                boolean progress = assign(job, task, bestPeer);
                if (task.getStatus() == TaskUnit.TaskStatus.PENDING
                        && state.hasActiveJob(job.getJobId())
                        && !jobCompletions.isPending(job.getJobId())) {
                    state.indexPendingTask(task, true);
                }
                if (progress) {
                    assignmentsForJob++;
                } else {
                    turnComplete = true;
                }
            }

            if (!waitingForCapacity
                    && state.hasActiveJob(job.getJobId())
                    && !jobCompletions.isPending(job.getJobId())) {
                state.requeueRunnableJob(job.getJobId());
            }
            completeRoundIfExhausted();
        }

        boolean immediateWorkRemaining = attempts >= limit
                && (currentRoundWorkRemaining() || nextRoundWorkAvailable());
        return new SchedulerLoop.StageResult(attempts, immediateWorkRemaining);
    }

    long millisUntilNextDispatchRecheck() {
        if (nextCapacityRecheckAtMillis == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long now = clock.nowEpochMillis();
        return nextCapacityRecheckAtMillis <= now ? 0L : nextCapacityRecheckAtMillis - now;
    }

    long completedRounds() {
        return completedRounds;
    }

    private boolean startRound() {
        runnableJobsRemainingInRound = state.runnableJobCount();
        roundCapacitySignalGeneration = capacitySignalGeneration;
        roundOpen = runnableJobsRemainingInRound > 0
                || state.hasCapacityWaitingJobEligibleBefore(roundCapacitySignalGeneration);
        return roundOpen;
    }

    private EmbarrassinglyParallelJob<?, ?> pollNextJobInRound() {
        if (runnableJobsRemainingInRound > 0) {
            EmbarrassinglyParallelJob<?, ?> job = state.pollRunnableJob();
            runnableJobsRemainingInRound--;
            if (job != null) {
                return job;
            }
            runnableJobsRemainingInRound = 0;
        }
        return state.pollCapacityWaitingJob(roundCapacitySignalGeneration);
    }

    private boolean currentRoundWorkRemaining() {
        return roundOpen
                && (runnableJobsRemainingInRound > 0
                || state.hasCapacityWaitingJobEligibleBefore(roundCapacitySignalGeneration));
    }

    private boolean nextRoundWorkAvailable() {
        return state.runnableJobCount() > 0
                || state.hasCapacityWaitingJobEligibleBefore(capacitySignalGeneration);
    }

    private void completeRoundIfExhausted() {
        if (!currentRoundWorkRemaining()) {
            completeRound();
        }
    }

    private void completeRound() {
        if (!roundOpen) {
            return;
        }
        roundOpen = false;
        runnableJobsRemainingInRound = 0;
        if (completedRounds < Long.MAX_VALUE) {
            completedRounds++;
        }
    }

    private void observeCapacityAvailabilityChange() {
        long currentVersion = registry.capacityAvailabilityVersion();
        if (currentVersion == observedCapacityAvailabilityVersion) {
            return;
        }
        observedCapacityAvailabilityVersion = currentVersion;
        advanceCapacitySignalGeneration();
        nextCapacityRecheckAtMillis = Long.MAX_VALUE;
    }

    private void activateTimedCapacityRecheck() {
        if (nextCapacityRecheckAtMillis == Long.MAX_VALUE
                || clock.nowEpochMillis() < nextCapacityRecheckAtMillis) {
            return;
        }
        advanceCapacitySignalGeneration();
        nextCapacityRecheckAtMillis = Long.MAX_VALUE;
    }

    private void advanceCapacitySignalGeneration() {
        if (capacitySignalGeneration < Long.MAX_VALUE) {
            capacitySignalGeneration++;
            return;
        }
        throw new IllegalStateException("Capacity signal generation exhausted.");
    }

    private void scheduleCapacityRecheck() {
        if (nextCapacityRecheckAtMillis != Long.MAX_VALUE) {
            return;
        }
        long now = clock.nowEpochMillis();
        nextCapacityRecheckAtMillis = now >= Long.MAX_VALUE - NO_CAPACITY_RECHECK_MILLIS
                ? Long.MAX_VALUE
                : now + NO_CAPACITY_RECHECK_MILLIS;
    }

    private PeerInfo bestAvailablePeer(String taskType) {
        Deque<PeerInfo> candidates = new ArrayDeque<>(getAvailablePeers(taskType));
        while (!candidates.isEmpty()
                && candidates.getFirst().getActiveTasks() >= config.maxTasksPerPeer()) {
            candidates.removeFirst();
        }
        return candidates.peekFirst();
    }

    private boolean assign(EmbarrassinglyParallelJob<?, ?> job, TaskUnit<?> task, PeerInfo peer) {
        long pendingSince = task.getPendingSinceMillis();
        long startedAt = clock.nowEpochMillis();
        long leaseExpiresAt = leaseExpiresAt(startedAt);
        long dispatchLatencyMs = pendingSince > 0L ? Math.max(0L, startedAt - pendingSince) : 0L;
        if (outbox.available()) {
            return assignWithBrokerOutbox(
                    job,
                    task,
                    peer,
                    startedAt,
                    leaseExpiresAt,
                    dispatchLatencyMs
            );
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
                return false;
            }
            message = job.createTaskAssignMessage(task).withAssignmentIdentity(
                    assignmentIdentity.attemptNumber(),
                    assignmentIdentity.assignmentId(),
                    assignmentIdentity.leaseExpiresAtEpochMillis()
            );
            MessageValidator.validate(message);
        } catch (RuntimeException e) {
            jobCompletions.failJob(job, "Task assignment could not be prepared: " + e.getMessage());
            return false;
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
                return false;
            }
        }

        if (!task.markAssigned(assignmentIdentity, startedAt, leaseOwnerId)) {
            jobCompletions.failJob(
                    job,
                    "Committed task assignment could not be installed in memory."
            );
            return false;
        }

        state.indexAssignedTask(task, assignmentIdentity);
        metrics.recordAssignmentGeneration(dispatchLatencyMs);
        registry.reserveTaskCapacity(peer);
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
                return false;
            }
            task.resetToPending();
            state.indexClosedAssignment(task, assignmentIdentity);
            registry.releaseTaskCapacity(peer);
            events.error("task_dispatch_failed", events.fields(
                    "job_id", job.getJobId(),
                    "task_id", task.getTaskId(),
                    "peer_id", peer.getNodeId(),
                    "error", e.getMessage()
            ));
            return false;
        }
        return true;
    }

    private boolean assignWithBrokerOutbox(EmbarrassinglyParallelJob<?, ?> job,
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
            return false;
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
            return false;
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
            return false;
        }

        state.indexAssignedTask(task, committed.identity());
        metrics.recordAssignmentGeneration(dispatchLatencyMs);
        registry.reserveTaskCapacity(peer);
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
        return true;
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
        return registry.getAvailablePeers(taskType, config.maxTasksPerPeer());
    }

    private static boolean durableStorageFailed(JobStateStore.DurableTransitionOutcome outcome) {
        return outcome == JobStateStore.DurableTransitionOutcome.STORAGE_FAILURE
                || outcome == JobStateStore.DurableTransitionOutcome.UNKNOWN_ENTITY;
    }
}
