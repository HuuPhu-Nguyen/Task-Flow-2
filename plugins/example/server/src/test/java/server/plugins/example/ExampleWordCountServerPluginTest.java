package server.plugins.example;

import example.model.ExampleJobSummary;
import example.model.ExamplePayload;
import example.model.ExampleTaskResult;
import example.model.ExampleTaskTypes;
import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;
import server.job.TaskUnit;

import java.time.Instant;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExampleWordCountServerPluginTest {
    @Test
    void discoversTaskPlugin() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(TaskPlugin.class).spliterator(), false)
                .map(TaskPlugin::taskType)
                .collect(Collectors.toSet());

        assertTrue(taskTypes.contains(ExampleTaskTypes.WORD_COUNT));
    }

    @Test
    void validatesAndAggregatesSemanticSummary() {
        JobSubmitMessage submit = submit(List.<Object>of(
                new ExamplePayload("one.txt", "one two"),
                new ExamplePayload("two.txt", "three four five")
        ), "summary");

        ExampleWordCountTaskPlugin plugin = new ExampleWordCountTaskPlugin();
        assertDoesNotThrow(() -> plugin.validateSubmission(submit));

        EmbarrassinglyParallelJob<?, ?> rawJob = plugin.createJob(submit, "client-1");
        rawJob.initializeTasks(submit);
        assertEquals(2, rawJob.getPendingTasks().size());

        TaskUnit<?> firstTask = rawJob.getPendingTasks().getFirst();
        TaskAssignMessage assignment = rawJob.createTaskAssignMessage(firstTask);
        assertEquals(ExampleTaskTypes.WORD_COUNT, assignment.getTaskType());
        assertEquals("summary", assignment.getParam());

        for (TaskUnit<?> task : rawJob.getPendingTasks()) {
            assertTrue(task.markAssigned("peer-1", System.currentTimeMillis()));
            ExamplePayload payload = (ExamplePayload) task.getPayload();
            rawJob.recordResult(task.getTaskId(), "peer-1",
                    new ExampleTaskResult(payload.documentName(), payload.text().split(" ").length));
        }

        assertTrue(rawJob.isJobComplete());
        assertEquals(2, rawJob.aggregateAndSendResult().size());
        ExampleJobSummary summary = assertInstanceOf(ExampleJobSummary.class, rawJob.aggregateResultPayload());
        assertEquals(2, summary.documentCount());
        assertEquals(5, summary.totalWordCount());
        assertEquals("one.txt", summary.documents().getFirst().documentName());
    }

    @Test
    void rejectsUnsupportedResultFormat() {
        JobSubmitMessage submit = submit(
                List.<Object>of(new ExamplePayload("notes.txt", "hello")),
                "json"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new ExampleWordCountTaskPlugin().validateSubmission(submit));

        assertTrue(error.getMessage().contains("Unsupported example word count result format"));
    }

    @Test
    void rejectsMissingTextPayload() {
        JobSubmitMessage submit = submit(
                List.<Object>of(new ExamplePayload("notes.txt", null)),
                "summary"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new ExampleWordCountTaskPlugin().validateSubmission(submit));

        assertTrue(error.getMessage().contains("requires text"));
    }

    private static JobSubmitMessage submit(List<Object> payloads, String parameter) {
        return new JobSubmitMessage(
                "CLIENT",
                Instant.EPOCH.toString(),
                "job-example",
                ExampleTaskTypes.WORD_COUNT,
                payloads,
                parameter
        );
    }
}
