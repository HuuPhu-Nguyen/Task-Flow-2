package server.scheduler;

import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.model.MessageEnvelope;
import server.registry.PeerRegistry;
import server.runtime.AssignmentIdGenerator;
import server.runtime.SystemTaskFlowClock;
import server.runtime.TaskFlowClock;
import server.runtime.UuidAssignmentIdGenerator;
import server.scheduler.transition.TaskStateMachine;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * Compatibility facade and composition root for the focused scheduler runtime.
 * Domain decisions and effects live in dedicated services; {@link SchedulerLoop}
 * owns only cycle orchestration.
 */
public class TaskScheduler implements Runnable {
    private final SchedulerLoop loop;
    private final RecoveryService recovery;
    private final SchedulerMetrics metrics;
    private final SchedulerState state;

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
        BlockingQueue<MessageEnvelope> checkedMailbox = Objects.requireNonNull(mailbox, "mailbox");
        PeerRegistry checkedRegistry = Objects.requireNonNull(registry, "registry");
        SchedulerOutput checkedOutput = Objects.requireNonNull(output, "output");
        SchedulerConfig effectiveConfig = config == null ? SchedulerConfig.defaults() : config;
        TaskFlowClock checkedClock = Objects.requireNonNull(clock, "clock");
        AssignmentIdGenerator checkedIdGenerator = Objects.requireNonNull(
                assignmentIdGenerator,
                "assignmentIdGenerator"
        );
        if (leaseOwnerId == null || leaseOwnerId.isBlank()) {
            throw new IllegalArgumentException("leaseOwnerId is required");
        }

        SchedulerState state = new SchedulerState(effectiveConfig);
        this.state = state;
        this.metrics = new SchedulerMetrics();
        SchedulerEventLog events = new SchedulerEventLog();
        SchedulerPersistence persistence = new SchedulerPersistence(db, events);
        SchedulerOutboxService outbox = new SchedulerOutboxService(
                persistence,
                checkedOutput,
                checkedClock,
                events
        );
        TaskTransitionDecisions transitions = new TaskTransitionDecisions(new TaskStateMachine());
        JobCompletionService jobCompletions = new JobCompletionService(
                state,
                checkedRegistry,
                persistence,
                checkedOutput,
                effectiveConfig,
                checkedClock,
                metrics,
                outbox,
                events
        );
        AttemptService attempts = new AttemptService(state, checkedRegistry, persistence, metrics);
        LeaseService leases = new LeaseService(
                state,
                effectiveConfig,
                checkedClock,
                transitions,
                attempts,
                jobCompletions,
                persistence,
                events
        );
        ResultCommitService resultCommits = new ResultCommitService(
                state,
                persistence,
                effectiveConfig,
                checkedClock,
                metrics,
                transitions,
                attempts,
                leases,
                jobCompletions,
                events
        );
        SchedulerMessageService messages = new SchedulerMessageService(
                state,
                persistence,
                checkedOutput,
                effectiveConfig,
                checkedClock,
                checkedIdGenerator,
                metrics,
                transitions,
                resultCommits,
                leases,
                jobCompletions,
                events
        );
        AssignmentService assignments = new AssignmentService(
                state,
                checkedRegistry,
                persistence,
                checkedOutput,
                effectiveConfig,
                leaseOwnerId.trim(),
                checkedClock,
                checkedIdGenerator,
                metrics,
                outbox,
                transitions,
                jobCompletions,
                events
        );
        SchedulerMetricsService metricUpdates = new SchedulerMetricsService(
                checkedMailbox,
                state,
                metrics,
                checkedRegistry,
                effectiveConfig,
                checkedClock,
                events
        );
        this.recovery = new RecoveryService(
                state,
                checkedClock,
                checkedIdGenerator,
                metrics,
                transitions,
                jobCompletions,
                events,
                checkedRegistry
        );
        this.loop = new SchedulerLoop(checkedMailbox, new SchedulerLoop.Work() {
            @Override
            public void processEnvelope(MessageEnvelope envelope) {
                messages.processEnvelope(envelope);
            }

            @Override
            public SchedulerLoop.StageResult processDueDeadlines(int limit) {
                return leases.processDueDeadlines(limit);
            }

            @Override
            public SchedulerLoop.StageResult dispatchPendingTasks(int limit) {
                return assignments.dispatchPendingTasks(limit);
            }

            @Override
            public SchedulerLoop.StageResult retryPendingOutbound(int limit) {
                return jobCompletions.retryPendingJobResults(limit);
            }

            @Override
            public void updateMetrics() {
                metricUpdates.updateAndMaybeLog();
            }

            @Override
            public long millisUntilNextScheduledWork() {
                return Math.min(
                        leases.millisUntilNextDeadline(),
                        Math.min(
                                assignments.millisUntilNextDispatchRecheck(),
                                Math.min(
                                        jobCompletions.millisUntilNextRetry(),
                                        metricUpdates.millisUntilNextUpdate()
                                )
                        )
                );
            }
        }, effectiveConfig);
    }

    private static String newLeaseOwnerId() {
        return "COORDINATOR_" + UuidAssignmentIdGenerator.INSTANCE.nextAssignmentId();
    }

    @Override
    public void run() {
        loop.run();
    }

    public void requestShutdownAfterDrain() {
        loop.requestShutdownAfterDrain();
    }

    /**
     * Wakes an idle scheduler after executor capability/capacity changed
     * outside the scheduler mailbox.
     */
    public void requestSchedulingRecheck() {
        loop.requestExternalWakeup();
    }

    public SchedulerMetrics.Snapshot getMetricsSnapshot() {
        return metrics.snapshot();
    }

    SchedulerWorkloadIndex.Snapshot getWorkloadSnapshot() {
        return state.workloadSnapshot();
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
        recovery.restoreJobs(jobs, restoredRequesterTokenHashes, restoredRequesterIdentityKeys);
    }
}
