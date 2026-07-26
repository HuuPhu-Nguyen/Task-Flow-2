package server.scheduler;

import server.db.BrokerOutboxStore;
import server.runtime.TaskFlowClock;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Owns the recomputable, process-local overload projection. */
final class SchedulerOverloadStatus {
    static final int JOB_SUBMIT_PREFETCH = 1;

    private final SchedulerConfig config;
    private final TaskFlowClock clock;
    private final SchedulerEventLog events;
    private final EnumMap<SchedulerOverloadSnapshot.Reason, Long> activatedAt =
            new EnumMap<>(SchedulerOverloadSnapshot.Reason.class);

    private SchedulerMailbox.DepthSnapshot mailbox;
    private long activeJobs;
    private long activeTasks;
    private long pendingOutboxRows;
    private boolean pendingOutboxObservationHealthy = true;
    private SchedulerOverloadSnapshot snapshot;

    SchedulerOverloadStatus(SchedulerConfig config,
                            TaskFlowClock clock,
                            SchedulerEventLog events) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.events = Objects.requireNonNull(events, "events");
        this.mailbox = new SchedulerMailbox.DepthSnapshot(
                0,
                config.inboundQueueCapacity(),
                0,
                SchedulerPriorityMailbox.TASK_RESULT_RESERVE_CAPACITY,
                false
        );
        long now = clock.nowEpochMillis();
        this.snapshot = new SchedulerOverloadSnapshot(
                false,
                List.of(),
                null,
                JOB_SUBMIT_PREFETCH,
                true,
                now
        );
    }

    synchronized void refreshMailbox(SchedulerMailbox.DepthSnapshot observed) {
        mailbox = Objects.requireNonNull(observed, "observed");
        recompute();
    }

    synchronized void refreshActive(long observedJobs, long observedTasks) {
        if (observedJobs < 0L || observedTasks < 0L) {
            throw new IllegalArgumentException("active counts must not be negative");
        }
        activeJobs = observedJobs;
        activeTasks = observedTasks;
        recompute();
    }

    synchronized void refreshPendingOutbox(long observedRows, boolean observationHealthy) {
        if (observationHealthy) {
            if (observedRows < 0L) {
                throw new IllegalArgumentException("pending outbox rows must not be negative");
            }
            pendingOutboxRows = observedRows;
        }
        pendingOutboxObservationHealthy = observationHealthy;
        recompute();
    }

    void refreshPendingOutbox(BrokerOutboxStore.PendingOutboxCount observed) {
        if (observed == null || !observed.counted()) {
            refreshPendingOutbox(0L, false);
            return;
        }
        refreshPendingOutbox(observed.count(), true);
    }

    synchronized SchedulerOverloadSnapshot snapshot() {
        return snapshot;
    }

    private void recompute() {
        SchedulerOverloadSnapshot previous = snapshot;
        long now = clock.nowEpochMillis();
        List<CurrentPressure> current = currentPressures();
        EnumMap<SchedulerOverloadSnapshot.Reason, Boolean> present =
                new EnumMap<>(SchedulerOverloadSnapshot.Reason.class);
        for (CurrentPressure pressure : current) {
            present.put(pressure.reason(), true);
            activatedAt.putIfAbsent(pressure.reason(), now);
        }
        activatedAt.keySet().removeIf(reason -> !present.containsKey(reason));

        List<SchedulerOverloadSnapshot.Pressure> reasons = current.stream()
                .map(pressure -> new SchedulerOverloadSnapshot.Pressure(
                        pressure.reason(),
                        pressure.configuredMaximum(),
                        pressure.observedValue(),
                        activatedAt.get(pressure.reason())
                ))
                .toList();
        List<SchedulerOverloadSnapshot.Reason> previousReasons = reasonNames(previous);
        List<SchedulerOverloadSnapshot.Reason> currentReasons =
                reasons.stream().map(SchedulerOverloadSnapshot.Pressure::reason).toList();
        boolean transition = !previousReasons.equals(currentReasons)
                || previous.pendingOutboxObservationHealthy()
                != pendingOutboxObservationHealthy;
        long changedAt = transition ? now : previous.changedAtEpochMillis();
        SchedulerOverloadSnapshot updated = new SchedulerOverloadSnapshot(
                !reasons.isEmpty(),
                reasons,
                reasons.isEmpty() ? null : reasons.getFirst().reason(),
                JOB_SUBMIT_PREFETCH,
                pendingOutboxObservationHealthy,
                changedAt
        );
        snapshot = updated;
        if (transition) {
            emitTransition(previous, updated);
        }
    }

    private List<CurrentPressure> currentPressures() {
        List<CurrentPressure> pressures = new ArrayList<>(5);
        if (mailbox.taskResultReserveSaturated()) {
            pressures.add(new CurrentPressure(
                    SchedulerOverloadSnapshot.Reason.TASK_RESULT_RESERVE_CAPACITY,
                    mailbox.taskResultCapacity(),
                    mailbox.taskResultDepth()
            ));
        }
        if (mailbox.submissionDepth() >= mailbox.submissionCapacity()) {
            pressures.add(new CurrentPressure(
                    SchedulerOverloadSnapshot.Reason.SUBMISSION_MAILBOX_CAPACITY,
                    mailbox.submissionCapacity(),
                    mailbox.submissionDepth()
            ));
        }
        if (pendingOutboxRows >= config.maxPendingOutboxRows()) {
            pressures.add(new CurrentPressure(
                    SchedulerOverloadSnapshot.Reason.MAX_PENDING_OUTBOX_ROWS,
                    config.maxPendingOutboxRows(),
                    pendingOutboxRows
            ));
        }
        if (activeJobs >= config.maxActiveJobs()) {
            pressures.add(new CurrentPressure(
                    SchedulerOverloadSnapshot.Reason.MAX_ACTIVE_JOBS,
                    config.maxActiveJobs(),
                    activeJobs
            ));
        }
        if (activeTasks >= config.maxActiveTasks()) {
            pressures.add(new CurrentPressure(
                    SchedulerOverloadSnapshot.Reason.MAX_ACTIVE_TASKS,
                    config.maxActiveTasks(),
                    activeTasks
            ));
        }
        return pressures;
    }

    private void emitTransition(SchedulerOverloadSnapshot previous,
                                SchedulerOverloadSnapshot current) {
        String event;
        if (!previous.overloaded() && current.overloaded()) {
            event = "scheduler_overload_started";
        } else if (previous.overloaded() && !current.overloaded()) {
            event = "scheduler_overload_recovered";
        } else {
            event = "scheduler_overload_changed";
        }
        SchedulerOverloadSnapshot.Pressure primary =
                current.reasons().isEmpty() ? null : current.reasons().getFirst();
        events.info(event, events.fields(
                "overloaded", current.overloaded(),
                "primary_reason", primary == null ? "NONE" : primary.reason(),
                "configured_maximum", primary == null ? 0L : primary.configuredMaximum(),
                "observed_value", primary == null ? 0L : primary.observedValue(),
                "reasons", reasonSummary(current),
                "job_submit_prefetch", current.jobSubmitPrefetch(),
                "pending_outbox_observation_healthy",
                current.pendingOutboxObservationHealthy()
        ));
    }

    private static List<SchedulerOverloadSnapshot.Reason> reasonNames(
            SchedulerOverloadSnapshot value) {
        return value.reasons().stream()
                .map(SchedulerOverloadSnapshot.Pressure::reason)
                .toList();
    }

    private static String reasonSummary(SchedulerOverloadSnapshot value) {
        return value.reasons().stream()
                .map(pressure -> pressure.reason()
                        + ":" + pressure.observedValue()
                        + "/" + pressure.configuredMaximum())
                .collect(Collectors.joining(","));
    }

    private record CurrentPressure(
            SchedulerOverloadSnapshot.Reason reason,
            long configuredMaximum,
            long observedValue
    ) {
    }
}
