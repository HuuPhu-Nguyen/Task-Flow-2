package server.scheduler;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import protocol.TaskFailureClassification;
import server.db.JobStateStore;
import server.runtime.TaskFlowClock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerEventLogTest {
    private static final Pattern EVENT_CALL = Pattern.compile(
            "events\\.(?:info|error)\\(\"([a-z][a-z0-9_]*)\""
    );
    private static final long EVENT_TIME = 1_735_689_600_000L;
    private static final String EVENT_TIMESTAMP = "2025-01-01T00:00:00Z";
    private static final String COORDINATOR_INSTANCE = "COORDINATOR_TEST_1";

    @Test
    void majorStateTransitionsCarryTheCommonSchemaAndApplicableCorrelation() {
        RecordingAppender appender = new RecordingAppender();
        Logger logger = (Logger) LoggerFactory.getLogger(TaskScheduler.class);
        Level previousLevel = logger.getLevel();
        boolean previousAdditive = logger.isAdditive();
        logger.setLevel(Level.ALL);
        logger.setAdditive(false);
        appender.setName("scheduler-event-schema-test");
        logger.addAppender(appender);
        appender.start();
        try {
            SchedulerEventLog events = eventLog();
            events.info("job_started", events.fields("job_id", "job-1"));
            events.info("job_resumed", events.fields("job_id", "job-1"));
            events.info("task_assignment_created", assignmentFields(events));
            events.info("task_result_committed", assignmentFields(
                    events,
                    "commit_outcome", JobStateStore.ResultCommitOutcome.COMMITTED
            ));
            events.info("task_result_stale_rejected", assignmentFields(
                    events,
                    "commit_outcome", JobStateStore.ResultCommitOutcome.STALE_ASSIGNMENT
            ));
            events.info("task_result_duplicate_ignored", assignmentFields(
                    events,
                    "commit_outcome",
                    JobStateStore.ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED
            ));
            events.error("task_failed", assignmentFields(
                    events,
                    "terminal_failure", false,
                    "failure_classification", TaskFailureClassification.RETRYABLE
            ));
            events.error("task_dispatch_failed", assignmentFields(events));
            events.error("task_timeout", assignmentFields(
                    events,
                    "terminal_failure", false
            ));
            events.error("task_lease_expired", assignmentFields(
                    events,
                    "terminal_failure", true
            ));
            events.error("task_peer_unavailable", assignmentFields(
                    events,
                    "terminal_failure", false,
                    "reason_code", "HEARTBEAT_TIMEOUT"
            ));
            events.info("job_completed", events.fields(
                    "job_id", "job-1",
                    "success", true
            ));
            events.error("job_failed", events.fields(
                    "job_id", "job-2",
                    "reason", "Task reached max retries."
            ));

            List<String> messages = appender.messages();
            assertEquals(13, messages.size());
            assertCommonSchema(event(messages, "job_started"), "ACCEPTED", "NONE");
            assertCommonSchema(event(messages, "job_resumed"), "RECOVERED", "NONE");
            assertCommonSchema(event(messages, "task_assignment_created"), "COMMITTED", "NONE");
            assertCommonSchema(event(messages, "task_result_committed"), "COMMITTED", "NONE");
            assertCommonSchema(
                    event(messages, "task_result_stale_rejected"),
                    "STALE_REJECTED",
                    "STALE_ASSIGNMENT"
            );
            assertCommonSchema(
                    event(messages, "task_result_duplicate_ignored"),
                    "DUPLICATE_IGNORED",
                    "DUPLICATE_RESULT"
            );
            assertCommonSchema(
                    event(messages, "task_failed"),
                    "RETRY_SCHEDULED",
                    "RETRYABLE"
            );
            assertCommonSchema(
                    event(messages, "task_dispatch_failed"),
                    "RETRY_SCHEDULED",
                    "DISPATCH_FAILED"
            );
            assertCommonSchema(
                    event(messages, "task_timeout"),
                    "RETRY_SCHEDULED",
                    "TASK_TIMEOUT"
            );
            assertCommonSchema(
                    event(messages, "task_lease_expired"),
                    "TERMINAL_FAILURE",
                    "LEASE_EXPIRED"
            );
            assertCommonSchema(
                    event(messages, "task_peer_unavailable"),
                    "RETRY_SCHEDULED",
                    "HEARTBEAT_TIMEOUT"
            );
            assertCommonSchema(event(messages, "job_completed"), "COMMITTED", "NONE");
            assertCommonSchema(event(messages, "job_failed"), "TERMINAL_FAILURE", "JOB_FAILED");

            for (String eventName : List.of(
                    "task_assignment_created",
                    "task_result_committed",
                    "task_result_stale_rejected",
                    "task_result_duplicate_ignored",
                    "task_failed",
                    "task_dispatch_failed",
                    "task_timeout",
                    "task_lease_expired",
                    "task_peer_unavailable"
            )) {
                assertAssignmentCorrelation(event(messages, eventName));
            }
            assertTrue(event(messages, "job_failed")
                    .contains("reason=\"Task reached max retries.\""));
        } finally {
            appender.stop();
            logger.detachAppender(appender.getName());
            logger.setLevel(previousLevel);
            logger.setAdditive(previousAdditive);
        }
    }

    @Test
    void rejectsSchemaReplacementSecretsFullPayloadsAndUnclassifiedEvents() {
        SchedulerEventLog events = eventLog();

        assertThrows(IllegalArgumentException.class,
                () -> events.fields("timestamp", EVENT_TIMESTAMP));
        assertThrows(IllegalArgumentException.class,
                () -> events.fields("outcome", "COMMITTED"));
        assertThrows(IllegalArgumentException.class,
                () -> events.fields("requester_token", "secret"));
        assertThrows(IllegalArgumentException.class,
                () -> events.fields("requester_public_key", "key-material"));
        assertThrows(IllegalArgumentException.class,
                () -> events.fields("private_key", "secret"));
        assertThrows(IllegalArgumentException.class,
                () -> events.fields("binary_payload", new byte[]{1, 2, 3}));
        assertThrows(IllegalArgumentException.class,
                () -> events.fields("job_id", "job-1", "job_id", "job-2"));
        assertThrows(IllegalArgumentException.class,
                () -> events.info("new_unclassified_event", events.fields()));
    }

    @Test
    void everySchedulerEventProducerUsesTheClassifiedSchemaBoundary() throws IOException {
        Path sourceRoot = schedulerSourceRoot();
        String schemaSource = Files.readString(sourceRoot.resolve("SchedulerEventLog.java"));
        Set<String> emittedEvents = new LinkedHashSet<>();

        try (var sources = Files.list(sourceRoot)) {
            for (Path source : sources
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.endsWith("SchedulerEventLog.java"))
                    .toList()) {
                String text = Files.readString(source);
                assertFalse(
                        text.contains("LoggerFactory") || text.contains("LOGGER."),
                        source.getFileName() + " bypasses SchedulerEventLog"
                );
                Matcher matcher = EVENT_CALL.matcher(text);
                while (matcher.find()) {
                    emittedEvents.add(matcher.group(1));
                }
            }
        }

        assertFalse(emittedEvents.isEmpty());
        for (String event : emittedEvents) {
            assertTrue(
                    schemaSource.contains('"' + event + '"'),
                    "Missing schema classification for " + event
            );
        }
        for (String overloadEvent : List.of(
                "scheduler_overload_started",
                "scheduler_overload_changed",
                "scheduler_overload_recovered"
        )) {
            assertTrue(schemaSource.contains('"' + overloadEvent + '"'));
        }
    }

    private static SchedulerEventLog eventLog() {
        return new SchedulerEventLog(
                new FixedClock(EVENT_TIME),
                COORDINATOR_INSTANCE
        );
    }

    private static java.util.Map<String, Object> assignmentFields(
            SchedulerEventLog events,
            Object... additionalFields
    ) {
        return events.assignmentTraceFields(
                "job-1",
                "task-1",
                2,
                "assignment-2",
                "worker-a",
                additionalFields
        );
    }

    private static String event(List<String> messages, String eventName) {
        return messages.stream()
                .filter(message -> message.startsWith("event=" + eventName + " "))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing event " + eventName));
    }

    private static Path schedulerSourceRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(
                    "taskflow-core/src/main/java/server/scheduler"
            );
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException(
                "Could not find scheduler sources from " + System.getProperty("user.dir")
        );
    }

    private static void assertCommonSchema(
            String message,
            String outcome,
            String failureReasonCode
    ) {
        assertTrue(message.startsWith("event="));
        assertTrue(message.contains(" timestamp=" + EVENT_TIMESTAMP));
        assertTrue(message.contains(" coordinator_instance_id=" + COORDINATOR_INSTANCE));
        assertTrue(message.contains(" outcome=" + outcome));
        assertTrue(message.contains(" failure_reason_code=" + failureReasonCode));
        assertFalse(message.contains("requester_token="));
        assertFalse(message.contains("private_key="));
        assertFalse(message.contains("binary_payload="));
    }

    private static void assertAssignmentCorrelation(String message) {
        assertTrue(message.contains(" job_id=job-1"));
        assertTrue(message.contains(" task_id=task-1"));
        assertTrue(message.contains(" attempt_number=2"));
        assertTrue(message.contains(" assignment_id=assignment-2"));
        assertTrue(message.contains(" worker_id=worker-a"));
    }

    private record FixedClock(long nowEpochMillis) implements TaskFlowClock {
        @Override
        public Instant now() {
            return Instant.ofEpochMilli(nowEpochMillis);
        }
    }

    private static final class RecordingAppender extends AppenderBase<ILoggingEvent> {
        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent eventObject) {
            messages.add(eventObject.getFormattedMessage());
        }

        private List<String> messages() {
            return List.copyOf(messages);
        }
    }
}
