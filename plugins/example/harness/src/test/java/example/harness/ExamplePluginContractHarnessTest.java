package example.harness;

import client.ClientJobPlugin;
import example.model.ExampleJobSummary;
import example.model.ExampleTaskTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import plugin.RetrySafety;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;
import server.job.TaskUnit;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExamplePluginContractHarnessTest {
    @TempDir
    Path tempDir;

    @Test
    void examplePluginRunsAcrossClientServerPeerAndResultHandlerContracts() throws Exception {
        ClientJobPlugin clientPlugin = loadClientPlugin();
        TaskPlugin taskPlugin = loadTaskPlugin();
        PeerProcessorPlugin peerPlugin = loadPeerPlugin();

        assertEquals(RetrySafety.PURE, taskPlugin.retrySafety());
        assertEquals(taskPlugin.retrySafety(), peerPlugin.retrySafety());
        assertEquals(taskPlugin.resourceProfile(), peerPlugin.resourceProfile());

        Path first = tempDir.resolve("first.txt");
        Path second = tempDir.resolve("second.txt");
        Files.writeString(first, "one two", StandardCharsets.UTF_8);
        Files.writeString(second, "three four five", StandardCharsets.UTF_8);

        List<Object> payloads = clientPlugin.buildPayloads(List.of(first, second), "summary");
        JobSubmitMessage submit = new JobSubmitMessage(
                "example-client",
                Instant.EPOCH.toString(),
                "job-example-harness",
                ExampleTaskTypes.WORD_COUNT,
                payloads,
                "summary"
        );
        taskPlugin.validateSubmission(submit);

        EmbarrassinglyParallelJob<?, ?> job = taskPlugin.createJob(submit, "example-client");
        job.initializeTasks(submit);
        TaskProcessor<?> processor = peerPlugin.createProcessor();

        for (TaskUnit<?> task : job.getPendingTasks()) {
            assertTrue(task.markAssigned("example-peer", System.currentTimeMillis()));
            TaskAssignMessage assignment = job.createTaskAssignMessage(task);
            Object taskResult = processor.process(assignment);
            job.recordResult(task.getTaskId(), "example-peer", taskResult);
        }

        assertTrue(job.isJobComplete());
        ExampleJobSummary summary = assertInstanceOf(ExampleJobSummary.class, job.aggregateResultPayload());
        assertEquals(2, summary.documentCount());
        assertEquals(5, summary.totalWordCount());

        JobResultMessage finalResult = new JobResultMessage(
                "COORDINATOR",
                Instant.EPOCH.toString(),
                submit.getJobId(),
                ExampleTaskTypes.WORD_COUNT,
                true,
                summary,
                job.aggregateAndSendResult()
        );
        Path outputDir = tempDir.resolve("out");
        clientPlugin.handleResult(finalResult, outputDir);

        String report = Files.readString(outputDir.resolve("example-word-count-summary.txt"));
        assertTrue(report.contains("documents=2"));
        assertTrue(report.contains("total_words=5"));
        assertTrue(report.contains("first.txt,2"));
        assertTrue(report.contains("second.txt,3"));
    }

    @Test
    void examplePluginIsNotWiredIntoCoreOrRuntimePoms() throws Exception {
        Path root = repoRoot();
        for (String pom : List.of(
                "taskflow-core/pom.xml",
                "taskflow-coordinator/pom.xml",
                "taskflow-peer/pom.xml",
                "taskflow-gui/pom.xml"
        )) {
            String xml = Files.readString(root.resolve(pom));
            assertFalse(xml.contains("taskflow-plugin-example-"), pom + " should not depend on the example plugin");
        }
    }

    private static ClientJobPlugin loadClientPlugin() {
        return StreamSupport.stream(ServiceLoader.load(ClientJobPlugin.class).spliterator(), false)
                .filter(plugin -> ExampleTaskTypes.WORD_COUNT.equals(plugin.taskType()))
                .findFirst()
                .orElseThrow();
    }

    private static TaskPlugin loadTaskPlugin() {
        return StreamSupport.stream(ServiceLoader.load(TaskPlugin.class).spliterator(), false)
                .filter(plugin -> ExampleTaskTypes.WORD_COUNT.equals(plugin.taskType()))
                .findFirst()
                .orElseThrow();
    }

    private static PeerProcessorPlugin loadPeerPlugin() {
        return StreamSupport.stream(ServiceLoader.load(PeerProcessorPlugin.class).spliterator(), false)
                .filter(plugin -> ExampleTaskTypes.WORD_COUNT.equals(plugin.taskType()))
                .findFirst()
                .orElseThrow();
    }

    private static Path repoRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            if (Files.exists(current.resolve("mvnw.cmd")) && Files.exists(current.resolve("pom.xml"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not find repository root from " + System.getProperty("user.dir"));
    }
}
