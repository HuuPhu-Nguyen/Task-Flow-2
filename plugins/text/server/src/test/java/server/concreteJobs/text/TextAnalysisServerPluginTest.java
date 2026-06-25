package server.concreteJobs.text;

import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;
import server.job.TaskUnit;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;
import text.model.TextAnalysisTaskTypes;

import java.time.Instant;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextAnalysisServerPluginTest {
    @Test
    void discoversTaskPlugin() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(TaskPlugin.class).spliterator(), false)
                .map(TaskPlugin::taskType)
                .collect(Collectors.toSet());

        assertTrue(taskTypes.contains(TextAnalysisTaskTypes.TEXT_ANALYSIS));
    }

    @Test
    void processesAndAggregatesCustomPayloadsWithoutFilePayload() throws Exception {
        TextAnalysisPayload payload = new TextAnalysisPayload("notes.txt", "Hello hello\nTaskFlow");
        JobSubmitMessage submit = new JobSubmitMessage(
                "CLIENT",
                Instant.now().toString(),
                "job-1",
                TextAnalysisTaskTypes.TEXT_ANALYSIS,
                List.<Object>of(payload),
                "csv"
        );

        EmbarrassinglyParallelJob<?, ?> rawJob = new TextAnalysisTaskPlugin().createJob(submit, "client-1");
        rawJob.initializeTasks(submit);
        TaskUnit<?> task = rawJob.getPendingTasks().getFirst();
        assertTrue(task.markAssigned("peer-1", System.currentTimeMillis()));

        TextAnalysisResult result = new TextAnalysisResult("notes.txt", 2, 3, 20, 2);
        rawJob.recordResult(task.getTaskId(), "peer-1", result);

        assertTrue(rawJob.isJobComplete());
        TextAnalysisResult aggregated = (TextAnalysisResult) rawJob.aggregateAndSendResult().getFirst();
        assertEquals("notes.txt", aggregated.documentName());
        assertEquals(2, aggregated.lineCount());
        assertEquals(3, aggregated.wordCount());
        assertEquals(2, aggregated.uniqueWordCount());
    }

    @Test
    void acceptsValidSubmission() {
        JobSubmitMessage submit = submit(
                List.<Object>of(new TextAnalysisPayload("notes.txt", "Hello TaskFlow")),
                "csv"
        );

        assertDoesNotThrow(() -> new TextAnalysisTaskPlugin().validateSubmission(submit));
    }

    @Test
    void rejectsUnsupportedResultFormat() {
        JobSubmitMessage submit = submit(
                List.<Object>of(new TextAnalysisPayload("notes.txt", "Hello TaskFlow")),
                "json"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new TextAnalysisTaskPlugin().validateSubmission(submit));

        assertTrue(error.getMessage().contains("Unsupported text analysis result format"));
    }

    @Test
    void rejectsMissingTextPayload() {
        JobSubmitMessage submit = submit(
                List.<Object>of(new TextAnalysisPayload("notes.txt", null)),
                "csv"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new TextAnalysisTaskPlugin().validateSubmission(submit));

        assertTrue(error.getMessage().contains("requires text"));
    }

    private static JobSubmitMessage submit(List<Object> payloads, String parameter) {
        return new JobSubmitMessage(
                "CLIENT",
                Instant.EPOCH.toString(),
                "job-validation",
                TextAnalysisTaskTypes.TEXT_ANALYSIS,
                payloads,
                parameter
        );
    }
}
