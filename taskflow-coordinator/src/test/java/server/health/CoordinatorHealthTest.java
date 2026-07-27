package server.health;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorHealthTest {
    @Test
    void startupIsNotLiveOrReadyBeforeProcessLoopActivation() {
        CoordinatorHealth health = new CoordinatorHealth();

        CoordinatorHealth.LivenessSnapshot liveness = health.liveness();
        CoordinatorHealth.ReadinessSnapshot readiness = health.readiness();

        assertEquals(CoordinatorHealth.LivenessState.STARTING, liveness.state());
        assertFalse(liveness.live());
        assertEquals(CoordinatorHealth.State.STARTING, readiness.state());
        assertFalse(readiness.live());
        assertFalse(readiness.ready());
        assertFalse(readiness.degraded());
        assertEquals(
                java.util.List.of(CoordinatorHealth.Reason.STARTING),
                readiness.reasons()
        );
    }

    @Test
    void dependencyLossDegradesAndRecoveryRestoresReadinessWithoutRestart() {
        AtomicBoolean loopRunning = new AtomicBoolean(true);
        AtomicBoolean sqliteWritable = new AtomicBoolean(true);
        AtomicBoolean brokerUsable = new AtomicBoolean(true);
        AtomicBoolean outboxObserved = new AtomicBoolean(true);
        AtomicLong pendingOutbox = new AtomicLong(2L);
        AtomicBoolean admissionBlocked = new AtomicBoolean(false);
        AtomicBoolean terminalOverload = new AtomicBoolean(false);
        CoordinatorHealth health = new CoordinatorHealth();
        health.activate(
                loopRunning::get,
                () -> new CoordinatorHealth.ReadinessInputs(
                        sqliteWritable.get(),
                        brokerUsable.get(),
                        outboxObserved.get(),
                        pendingOutbox.get(),
                        3L,
                        admissionBlocked.get(),
                        terminalOverload.get()
                )
        );

        assertReady(health.readiness());

        brokerUsable.set(false);
        CoordinatorHealth.ReadinessSnapshot brokerDown = health.readiness();
        assertDegraded(brokerDown, CoordinatorHealth.Reason.BROKER_NOT_USABLE);
        assertTrue(health.liveness().live());

        brokerUsable.set(true);
        sqliteWritable.set(false);
        assertDegraded(
                health.readiness(),
                CoordinatorHealth.Reason.SQLITE_NOT_WRITABLE
        );

        sqliteWritable.set(true);
        outboxObserved.set(false);
        assertDegraded(
                health.readiness(),
                CoordinatorHealth.Reason.OUTBOX_OBSERVATION_UNAVAILABLE
        );

        outboxObserved.set(true);
        pendingOutbox.set(3L);
        assertDegraded(
                health.readiness(),
                CoordinatorHealth.Reason.OUTBOX_THRESHOLD_REACHED
        );

        pendingOutbox.set(0L);
        admissionBlocked.set(true);
        assertDegraded(
                health.readiness(),
                CoordinatorHealth.Reason.SCHEDULER_ADMISSION_BLOCKED
        );

        admissionBlocked.set(false);
        terminalOverload.set(true);
        assertDegraded(
                health.readiness(),
                CoordinatorHealth.Reason.SCHEDULER_TERMINAL_OVERLOAD
        );

        terminalOverload.set(false);
        assertReady(health.readiness());
    }

    @Test
    void deadProcessLoopIsDownWithoutCallingReadinessProbe() {
        AtomicLong probeCalls = new AtomicLong();
        CoordinatorHealth health = new CoordinatorHealth();
        health.activate(
                () -> false,
                () -> {
                    probeCalls.incrementAndGet();
                    throw new AssertionError("Readiness dependency must not be called");
                }
        );

        CoordinatorHealth.ReadinessSnapshot snapshot = health.readiness();

        assertEquals(CoordinatorHealth.State.DOWN, snapshot.state());
        assertFalse(snapshot.live());
        assertFalse(snapshot.ready());
        assertFalse(snapshot.degraded());
        assertEquals(
                java.util.List.of(CoordinatorHealth.Reason.PROCESS_LOOP_NOT_RUNNING),
                snapshot.reasons()
        );
        assertEquals(0L, probeCalls.get());
    }

    @Test
    void readinessProbeFailureIsDegradedAndDoesNotChangeLiveness() {
        CoordinatorHealth health = new CoordinatorHealth();
        health.activate(
                () -> true,
                () -> {
                    throw new IllegalStateException("injected probe failure");
                }
        );

        CoordinatorHealth.ReadinessSnapshot snapshot = health.readiness();

        assertDegraded(snapshot, CoordinatorHealth.Reason.READINESS_PROBE_FAILED);
        assertTrue(health.liveness().live());
    }

    private static void assertReady(CoordinatorHealth.ReadinessSnapshot snapshot) {
        assertEquals(CoordinatorHealth.State.READY, snapshot.state());
        assertTrue(snapshot.live());
        assertTrue(snapshot.ready());
        assertFalse(snapshot.degraded());
        assertTrue(snapshot.reasons().isEmpty());
    }

    private static void assertDegraded(
            CoordinatorHealth.ReadinessSnapshot snapshot,
            CoordinatorHealth.Reason reason
    ) {
        assertEquals(CoordinatorHealth.State.DEGRADED, snapshot.state());
        assertTrue(snapshot.live());
        assertFalse(snapshot.ready());
        assertTrue(snapshot.degraded());
        assertTrue(snapshot.reasons().contains(reason));
    }
}
