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

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
