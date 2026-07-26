package server.scheduler;

import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.job.AssignmentIdentity;
import server.registry.PeerRegistry;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;

import java.util.Collection;
import java.util.Map;

/** Owns H1 scheduler projection hydration after coordinator/store reconciliation. */
final class RecoveryService {
    private final SchedulerState state;
    private final TaskFlowClock clock;
    private final AssignmentIdGenerator assignmentIdGenerator;
    private final SchedulerMetrics metrics;
    private final TaskTransitionDecisions transitions;
    private final JobCompletionService jobCompletions;
    private final SchedulerEventLog events;
    private final PeerRegistry registry;

    RecoveryService(SchedulerState state,
                    TaskFlowClock clock,
                    AssignmentIdGenerator assignmentIdGenerator,
                    SchedulerMetrics metrics,
                    TaskTransitionDecisions transitions,
                    JobCompletionService jobCompletions,
                    SchedulerEventLog events,
                    PeerRegistry registry) {
        this.state = state;
        this.clock = clock;
        this.assignmentIdGenerator = assignmentIdGenerator;
        this.metrics = metrics;
        this.transitions = transitions;
        this.jobCompletions = jobCompletions;
        this.events = events;
        this.registry = registry;
    }

    void restoreJobs(Collection<EmbarrassinglyParallelJob<?, ?>> jobs,
                     Map<String, String> restoredRequesterTokenHashes,
                     Map<String, String> restoredRequesterIdentityKeys) {
        Map<String, String> tokenHashes = restoredRequesterTokenHashes == null
                ? Map.of()
                : restoredRequesterTokenHashes;
        Map<String, String> identityKeys = restoredRequesterIdentityKeys == null
                ? Map.of()
                : restoredRequesterIdentityKeys;
        long recoveredAt = clock.nowEpochMillis();

        for (EmbarrassinglyParallelJob<?, ?> job : jobs) {
            if (job == null || state.hasActiveJob(job.getJobId())) {
                continue;
            }
            job.configureTransitionPorts(clock, assignmentIdGenerator);
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (!transitions.coordinatorRecovered(task, recoveredAt).accepted()) {
                    throw new IllegalStateException(
                            "Recovered task projection was rejected: " + task.getTaskId()
                    );
                }
            }
            state.addActiveJob(
                    job,
                    tokenHashes.get(job.getJobId()),
                    identityKeys.get(job.getJobId())
            );
            for (TaskUnit<?> task : job.getTasks().values()) {
                if (task.getStatus() != TaskUnit.TaskStatus.ASSIGNED) {
                    continue;
                }
                AssignmentIdentity identity = task.getAssignmentIdentity()
                        .orElseThrow(() -> new IllegalStateException(
                                "Recovered assigned task is missing assignment identity: "
                                        + task.getTaskId()
                        ));
                if (!registry.reserveTaskCapacity(
                        CapacityReservations.forAssignment(job, task, identity)
                )) {
                    throw new IllegalStateException(
                            "Recovered capacity projection is invalid; restart is required."
                    );
                }
            }
            events.info("job_resumed", events.fields(
                    "job_id", job.getJobId(),
                    "task_type", job.getTaskType(),
                    "requester_id", job.getRequesterNodeId(),
                    "task_count", job.getTasks().size()
            ));
        }
        metrics.setActiveJobs(state.activeJobCount());

        for (EmbarrassinglyParallelJob<?, ?> job : state.activeJobsSnapshot()) {
            if (job.isJobComplete()) {
                jobCompletions.completeJob(job, true, null);
            } else if (job.hasTerminalFailure()) {
                jobCompletions.failJob(job, "Job resumed with one or more terminal failed tasks.");
            }
        }
    }
}
