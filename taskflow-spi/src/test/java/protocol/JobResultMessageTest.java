package protocol;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobResultMessageTest {
    private static final Gson GSON = new Gson();

    @Test
    void legacyResultListIsAlsoTheSemanticPayload() {
        List<Object> results = List.of("first", "second");

        JobResultMessage message = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                results);

        assertEquals(results, message.getResultsByTaskId());
        assertEquals(results, message.getResultPayload());
        assertEquals(results, message.getResultPayloadList());
    }

    @Test
    void semanticPayloadCanDifferFromCompatibilityList() {
        Map<String, Object> report = Map.of(
                "documentCount", 2,
                "totalWords", 42);
        List<Object> taskResults = List.of("task-a", "task-b");

        JobResultMessage message = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                report,
                taskResults,
                null);

        assertEquals(report, message.getResultPayload());
        assertEquals(taskResults, message.getResultsByTaskId());
        assertEquals(taskResults, message.getResultPayloadList());
    }

    @Test
    void semanticPayloadFallsBackToSingletonCompatibilityList() {
        Map<String, Object> report = Map.of("totalWords", 42);

        JobResultMessage message = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                report,
                null,
                null);

        assertEquals(report, message.getResultPayload());
        assertEquals(List.of(report), message.getResultPayloadList());
    }

    @Test
    void admissionRejectionRoundTripsWithoutChangingProtocolVersion() {
        AdmissionRejection rejection = new AdmissionRejection(
                AdmissionRejection.Limit.MAX_ACTIVE_TASKS,
                100_000L,
                100_001L
        );
        JobResultMessage message = JobResultMessage.admissionRejected(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                "Active task limit exceeded.",
                rejection
        );

        JobResultMessage decoded = GSON.fromJson(
                GSON.toJson(message),
                JobResultMessage.class
        );

        assertEquals(ProtocolVersions.ASSIGNMENT_IDENTITY, decoded.getProtocolVersion());
        assertEquals(rejection, decoded.getAdmissionRejection());
        assertEquals("Active task limit exceeded.", decoded.getErrorMessage());
        MessageValidator.validate(decoded);
    }

    @Test
    void legacyJobResultWithoutAdmissionDetailRemainsReadable() {
        JobResultMessage decoded = GSON.fromJson("""
                {
                  "protocolVersion": 2,
                  "type": "JOB_RESULT",
                  "nodeId": "coordinator",
                  "time": "1970-01-01T00:00:00Z",
                  "jobId": "job-1",
                  "taskType": "TEXT_ANALYSIS",
                  "successful": false,
                  "resultsByTaskId": [],
                  "errorMessage": "failed"
                }
                """, JobResultMessage.class);

        assertNull(decoded.getAdmissionRejection());
        MessageValidator.validate(decoded);
    }

    @Test
    void ordinaryTerminalResultsDoNotCarryAdmissionDetail() {
        JobResultMessage failed = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                false,
                List.of(),
                "Plugin failed."
        );
        JobResultMessage successful = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                List.of("done")
        );

        assertNull(failed.getAdmissionRejection());
        assertNull(successful.getAdmissionRejection());

        String invalidJson = GSON.toJson(successful).replace(
                "\"successful\":true",
                "\"successful\":true,\"admissionRejection\":{"
                        + "\"limit\":\"MAX_ACTIVE_JOBS\","
                        + "\"configuredMaximum\":1,"
                        + "\"observedValue\":2}"
        );
        JobResultMessage invalid = GSON.fromJson(invalidJson, JobResultMessage.class);
        assertThrows(MessageValidationException.class, () -> MessageValidator.validate(invalid));
    }
}
