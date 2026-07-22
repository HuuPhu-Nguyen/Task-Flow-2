package protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageValidatorTest {
    @Test
    void acceptsValidJobSubmissions() {
        JobSubmitMessage message = new JobSubmitMessage(
                "requester-1",
                "2026-07-04T00:00:00Z",
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                "token-job-1"
        );

        assertDoesNotThrow(() -> MessageValidator.validate(message));
    }

    @Test
    void rejectsPeerIdsThatWouldBeSanitizedForRouting() {
        PingMessage message = new PingMessage("peer/unsafe", "2026-07-04T00:00:00Z");

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(message)
        );

        assertTrue(error.getMessage().contains("nodeId contains unsupported characters"));
    }

    @Test
    void rejectsUnsafeJobIds() {
        JobSubmitMessage message = new JobSubmitMessage(
                "requester-1",
                "2026-07-04T00:00:00Z",
                "../job",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                "token-bad-job"
        );

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(message)
        );

        assertTrue(error.getMessage().contains("Job id may contain only"));
    }

    @Test
    void rejectsOversizeJobPayloads() {
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, "48");
        try {
            JobSubmitMessage message = new JobSubmitMessage(
                    "requester-1",
                    "2026-07-04T00:00:00Z",
                    "job-oversize",
                    "TEXT_ANALYSIS",
                    List.of("this-payload-is-longer-than-the-test-limit"),
                    "summary",
                    "token-job-oversize"
            );

            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(message)
            );

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void rejectsOversizeResultPayloads() {
        System.setProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY, "48");
        try {
            TaskResultMessage message = new TaskResultMessage(
                    "peer-1",
                    "2026-07-04T00:00:00Z",
                    "task-1",
                    "job-1",
                    1,
                    "550e8400-e29b-41d4-a716-446655440000",
                    "this-result-is-longer-than-the-test-limit",
                    true,
                    ""
            );

            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(message)
            );

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_RESULT_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY);
        }
    }
}
