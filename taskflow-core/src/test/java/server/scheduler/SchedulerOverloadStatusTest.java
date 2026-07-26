package server.scheduler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import server.runtime.TaskFlowClock;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerOverloadStatusTest {

    @Test
    void reportsAllActiveReasonsInStablePriorityOrderAndRecoversExactlyBelowLimits() {
        MutableClock clock = new MutableClock(1_000L);
        SchedulerOverloadStatus status = status(clock);

        status.refreshActive(2L, 3L);
        status.refreshPendingOutbox(4L, true);
        status.refreshMailbox(new SchedulerMailbox.DepthSnapshot(1, 1, 1, 1, true));

        SchedulerOverloadSnapshot overloaded = status.snapshot();
        assertTrue(overloaded.overloaded());
        assertEquals(SchedulerOverloadSnapshot.Reason.TASK_RESULT_RESERVE_CAPACITY,
                overloaded.primaryReason());
        assertEquals(List.of(
                SchedulerOverloadSnapshot.Reason.TASK_RESULT_RESERVE_CAPACITY,
                SchedulerOverloadSnapshot.Reason.SUBMISSION_MAILBOX_CAPACITY,
                SchedulerOverloadSnapshot.Reason.MAX_PENDING_OUTBOX_ROWS,
                SchedulerOverloadSnapshot.Reason.MAX_ACTIVE_JOBS,
                SchedulerOverloadSnapshot.Reason.MAX_ACTIVE_TASKS
        ), overloaded.reasons().stream()
                .map(SchedulerOverloadSnapshot.Pressure::reason)
                .toList());
        assertEquals(1, overloaded.jobSubmitPrefetch());
        assertTrue(overloaded.pendingOutboxObservationHealthy());
        assertThrows(
                UnsupportedOperationException.class,
                () -> overloaded.reasons().clear()
        );

        clock.set(2_000L);
        status.refreshMailbox(new SchedulerMailbox.DepthSnapshot(0, 1, 0, 1, false));
        status.refreshPendingOutbox(3L, true);
        status.refreshActive(1L, 2L);

        SchedulerOverloadSnapshot recovered = status.snapshot();
        assertFalse(recovered.overloaded());
        assertEquals(List.of(), recovered.reasons());
        assertEquals(2_000L, recovered.changedAtEpochMillis());
    }

    @Test
    void failedOutboxObservationCannotClearKnownPressureOrInventZero() {
        MutableClock clock = new MutableClock(10L);
        SchedulerOverloadStatus status = status(clock);
        status.refreshPendingOutbox(4L, true);

        clock.set(20L);
        status.refreshPendingOutbox(0L, false);

        SchedulerOverloadSnapshot snapshot = status.snapshot();
        assertTrue(snapshot.overloaded());
        assertFalse(snapshot.pendingOutboxObservationHealthy());
        SchedulerOverloadSnapshot.Pressure pressure = snapshot.reasons().getFirst();
        assertEquals(SchedulerOverloadSnapshot.Reason.MAX_PENDING_OUTBOX_ROWS,
                pressure.reason());
        assertEquals(4L, pressure.observedValue());
        assertEquals(10L, pressure.activeSinceEpochMillis());
        assertEquals(20L, snapshot.changedAtEpochMillis());
    }

    @Test
    void resultReserveIsNotAnOverloadReasonUntilASecondResultObservesSaturation() {
        SchedulerOverloadStatus status = status(new MutableClock(1L));

        status.refreshMailbox(new SchedulerMailbox.DepthSnapshot(0, 1, 1, 1, false));
        assertFalse(status.snapshot().overloaded());

        status.refreshMailbox(new SchedulerMailbox.DepthSnapshot(0, 1, 1, 1, true));
        assertEquals(
                SchedulerOverloadSnapshot.Reason.TASK_RESULT_RESERVE_CAPACITY,
                status.snapshot().primaryReason()
        );
    }

    @Test
    void transitionEventsExposeStableReasonLimitAndRecoveryFields() {
        Logger schedulerLogger = (Logger) LoggerFactory.getLogger(TaskScheduler.class);
        Level previousLevel = schedulerLogger.getLevel();
        boolean previousAdditive = schedulerLogger.isAdditive();
        RecordingAppender appender = new RecordingAppender();
        schedulerLogger.setLevel(Level.INFO);
        schedulerLogger.setAdditive(false);
        appender.start();
        schedulerLogger.addAppender(appender);
        try {
            MutableClock clock = new MutableClock(100L);
            SchedulerOverloadStatus status = status(clock);

            status.refreshActive(2L, 0L);
            clock.set(200L);
            status.refreshMailbox(new SchedulerMailbox.DepthSnapshot(1, 1, 0, 1, false));
            clock.set(300L);
            status.refreshActive(1L, 0L);
            clock.set(400L);
            status.refreshMailbox(new SchedulerMailbox.DepthSnapshot(0, 1, 0, 1, false));

            List<String> events = appender.messages();
            assertEquals(4, events.size());
            assertTrue(events.get(0).contains("event=scheduler_overload_started"));
            assertTrue(events.get(0).contains("primary_reason=MAX_ACTIVE_JOBS"));
            assertTrue(events.get(0).contains("configured_maximum=2"));
            assertTrue(events.get(0).contains("observed_value=2"));
            assertTrue(events.get(0).contains("job_submit_prefetch=1"));
            assertTrue(events.get(1).contains("event=scheduler_overload_changed"));
            assertTrue(events.get(1).contains("primary_reason=SUBMISSION_MAILBOX_CAPACITY"));
            assertTrue(events.get(2).contains("event=scheduler_overload_changed"));
            assertTrue(events.get(3).contains("event=scheduler_overload_recovered"));
            assertTrue(events.get(3).contains("primary_reason=NONE"));
            assertTrue(events.get(3).contains("pending_outbox_observation_healthy=true"));
        } finally {
            schedulerLogger.detachAppender(appender);
            appender.stop();
            schedulerLogger.setLevel(previousLevel);
            schedulerLogger.setAdditive(previousAdditive);
        }
    }

    private static SchedulerOverloadStatus status(TaskFlowClock clock) {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "1",
                "TASKFLOW_MAX_ACTIVE_JOBS", "2",
                "TASKFLOW_MAX_ACTIVE_TASKS", "3",
                "TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "4"
        ));
        return new SchedulerOverloadStatus(config, clock, new SchedulerEventLog());
    }

    private static final class MutableClock implements TaskFlowClock {
        private long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        void set(long millis) {
            this.millis = millis;
        }

        @Override
        public Instant now() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long nowEpochMillis() {
            return millis;
        }
    }

    private static final class RecordingAppender extends AppenderBase<ILoggingEvent> {
        private final CopyOnWriteArrayList<ILoggingEvent> events =
                new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing();
            events.add(event);
        }

        private List<String> messages() {
            return events.stream().map(ILoggingEvent::getFormattedMessage).toList();
        }
    }
}
