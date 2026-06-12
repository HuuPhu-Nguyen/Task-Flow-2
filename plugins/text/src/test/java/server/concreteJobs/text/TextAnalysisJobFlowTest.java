package server.concreteJobs.text;

import org.junit.jupiter.api.Test;
import peer.processors.TextAnalysisProcessor;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextAnalysisJobFlowTest {
    @Test
    void processesAndAggregatesCustomPayloadsWithoutFilePayload() throws Exception {
        TextAnalysisPayload payload = new TextAnalysisPayload("notes.txt", "Hello hello\nTaskFlow");
        JobSubmitMessage submit = new JobSubmitMessage(
                "CLIENT",
                Instant.now().toString(),
                "job-1",
                TextAnalysisTaskPlugin.TYPE,
                List.<Object>of(payload),
                "csv"
        );

        EmbarrassinglyParallelJob<?, ?> rawJob = new TextAnalysisTaskPlugin().createJob(submit, "client-1");
        rawJob.initializeTasks(submit);
        TaskUnit<?> task = rawJob.getPendingTasks().getFirst();
        assertTrue(task.markAssigned("peer-1", System.currentTimeMillis()));

        TaskAssignMessage assignment = rawJob.createTaskAssignMessage(task);
        TextAnalysisResult result = new TextAnalysisProcessor().process(assignment);
        rawJob.recordResult(task.getTaskId(), "peer-1", result);

        assertTrue(rawJob.isJobComplete());
        TextAnalysisResult aggregated = (TextAnalysisResult) rawJob.aggregateAndSendResult().getFirst();
        assertEquals("notes.txt", aggregated.documentName());
        assertEquals(2, aggregated.lineCount());
        assertEquals(3, aggregated.wordCount());
        assertEquals(2, aggregated.uniqueWordCount());
    }
}
