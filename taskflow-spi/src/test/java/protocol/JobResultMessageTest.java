package protocol;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JobResultMessageTest {
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
}
