package server.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Composes coordinator lifecycle and dependency observations without latching
 * failures. Liveness never calls a dependency probe; readiness is recomputed
 * on every observation so recovered dependencies restore service
 * automatically.
 */
public final class CoordinatorHealth {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorHealth.class);

    private final AtomicReference<Transition> lastTransition = new AtomicReference<>();
    private volatile BooleanSupplier processLoopRunning;
    private volatile Supplier<ReadinessInputs> readinessProbe;

    public void activate(
            BooleanSupplier processLoopRunning,
            Supplier<ReadinessInputs> readinessProbe
    ) {
        this.processLoopRunning = Objects.requireNonNull(
                processLoopRunning,
                "processLoopRunning"
        );
        this.readinessProbe = Objects.requireNonNull(readinessProbe, "readinessProbe");
    }

    public LivenessSnapshot liveness() {
        BooleanSupplier loop = processLoopRunning;
        if (loop == null) {
            return new LivenessSnapshot(
                    LivenessState.STARTING,
                    false,
                    List.of(Reason.STARTING)
            );
        }
        try {
            if (loop.getAsBoolean()) {
                return new LivenessSnapshot(LivenessState.UP, true, List.of());
            }
        } catch (RuntimeException e) {
            LOGGER.warn(
                    "event=coordinator_liveness_probe_failed error={}",
                    e.getMessage(),
                    e
            );
        }
        return new LivenessSnapshot(
                LivenessState.DOWN,
                false,
                List.of(Reason.PROCESS_LOOP_NOT_RUNNING)
        );
    }

    public ReadinessSnapshot readiness() {
        LivenessSnapshot liveness = liveness();
        Supplier<ReadinessInputs> probe = readinessProbe;
        ReadinessSnapshot snapshot;
        if (probe == null) {
            snapshot = ReadinessSnapshot.starting();
        } else if (!liveness.live()) {
            snapshot = ReadinessSnapshot.down(liveness.reasons());
        } else {
            snapshot = readinessFrom(probe);
        }
        emitTransition(snapshot);
        return snapshot;
    }

    public boolean readyForNewJobs() {
        return readiness().ready();
    }

    private static ReadinessSnapshot readinessFrom(Supplier<ReadinessInputs> probe) {
        ReadinessInputs inputs;
        try {
            inputs = Objects.requireNonNull(probe.get(), "readiness probe result");
        } catch (RuntimeException e) {
            return new ReadinessSnapshot(
                    State.DEGRADED,
                    true,
                    false,
                    true,
                    false,
                    false,
                    false,
                    -1L,
                    -1L,
                    false,
                    false,
                    List.of(Reason.READINESS_PROBE_FAILED)
            );
        }

        List<Reason> reasons = new ArrayList<>(6);
        if (!inputs.sqliteWritable()) {
            reasons.add(Reason.SQLITE_NOT_WRITABLE);
        }
        if (!inputs.brokerUsable()) {
            reasons.add(Reason.BROKER_NOT_USABLE);
        }
        if (!inputs.outboxObserved()) {
            reasons.add(Reason.OUTBOX_OBSERVATION_UNAVAILABLE);
        } else if (inputs.pendingOutboxRows() >= inputs.maxPendingOutboxRows()) {
            reasons.add(Reason.OUTBOX_THRESHOLD_REACHED);
        }
        if (inputs.schedulerAdmissionBlocked()) {
            reasons.add(Reason.SCHEDULER_ADMISSION_BLOCKED);
        }
        if (inputs.schedulerTerminalOverload()) {
            reasons.add(Reason.SCHEDULER_TERMINAL_OVERLOAD);
        }

        boolean ready = reasons.isEmpty();
        return new ReadinessSnapshot(
                ready ? State.READY : State.DEGRADED,
                true,
                ready,
                !ready,
                inputs.sqliteWritable(),
                inputs.brokerUsable(),
                inputs.outboxObserved(),
                inputs.outboxObserved() ? inputs.pendingOutboxRows() : -1L,
                inputs.maxPendingOutboxRows(),
                inputs.schedulerAdmissionBlocked(),
                inputs.schedulerTerminalOverload(),
                reasons
        );
    }

    private void emitTransition(ReadinessSnapshot snapshot) {
        Transition current = new Transition(snapshot.state(), snapshot.reasons());
        Transition previous = lastTransition.getAndSet(current);
        if (current.equals(previous)) {
            return;
        }
        LOGGER.info(
                "event=coordinator_health_changed status={} live={} ready={} degraded={} reasons={}",
                snapshot.state(),
                snapshot.live(),
                snapshot.ready(),
                snapshot.degraded(),
                snapshot.reasons().stream()
                        .map(Enum::name)
                        .collect(Collectors.joining(","))
        );
    }

    public enum State {
        STARTING,
        READY,
        DEGRADED,
        DOWN
    }

    public enum LivenessState {
        STARTING,
        UP,
        DOWN
    }

    public enum Reason {
        STARTING,
        PROCESS_LOOP_NOT_RUNNING,
        SQLITE_NOT_WRITABLE,
        BROKER_NOT_USABLE,
        OUTBOX_OBSERVATION_UNAVAILABLE,
        OUTBOX_THRESHOLD_REACHED,
        SCHEDULER_ADMISSION_BLOCKED,
        SCHEDULER_TERMINAL_OVERLOAD,
        READINESS_PROBE_FAILED
    }

    public record ReadinessInputs(
            boolean sqliteWritable,
            boolean brokerUsable,
            boolean outboxObserved,
            long pendingOutboxRows,
            long maxPendingOutboxRows,
            boolean schedulerAdmissionBlocked,
            boolean schedulerTerminalOverload
    ) {
        public ReadinessInputs {
            if (maxPendingOutboxRows <= 0L) {
                throw new IllegalArgumentException(
                        "maxPendingOutboxRows must be positive"
                );
            }
            if (outboxObserved && pendingOutboxRows < 0L) {
                throw new IllegalArgumentException(
                        "Observed pending outbox rows must not be negative"
                );
            }
        }
    }

    public record LivenessSnapshot(
            LivenessState state,
            boolean live,
            List<Reason> reasons
    ) {
        public LivenessSnapshot {
            Objects.requireNonNull(state, "state");
            reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
        }
    }

    public record ReadinessSnapshot(
            State state,
            boolean live,
            boolean ready,
            boolean degraded,
            boolean sqliteWritable,
            boolean brokerUsable,
            boolean outboxObserved,
            long pendingOutboxRows,
            long maxPendingOutboxRows,
            boolean schedulerAdmissionBlocked,
            boolean schedulerTerminalOverload,
            List<Reason> reasons
    ) {
        public ReadinessSnapshot {
            Objects.requireNonNull(state, "state");
            reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
        }

        private static ReadinessSnapshot starting() {
            return new ReadinessSnapshot(
                    State.STARTING,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    -1L,
                    -1L,
                    false,
                    false,
                    List.of(Reason.STARTING)
            );
        }

        private static ReadinessSnapshot down(List<Reason> reasons) {
            return new ReadinessSnapshot(
                    State.DOWN,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    -1L,
                    -1L,
                    false,
                    false,
                    reasons
            );
        }
    }

    private record Transition(State state, List<Reason> reasons) {
        private Transition {
            reasons = List.copyOf(reasons);
        }
    }
}
