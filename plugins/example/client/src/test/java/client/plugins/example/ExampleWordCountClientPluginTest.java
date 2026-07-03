package client.plugins.example;

import client.ClientJobPlugin;
import example.model.ExampleJobSummary;
import example.model.ExamplePayload;
import example.model.ExampleTaskResult;
import example.model.ExampleTaskTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;

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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExampleWordCountClientPluginTest {
    @TempDir
    Path tempDir;

    @Test
    void discoversClientPlugin() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(ClientJobPlugin.class).spliterator(), false)
                .map(ClientJobPlugin::taskType)
                .collect(Collectors.toSet());

        assertTrue(taskTypes.contains(ExampleTaskTypes.WORD_COUNT));
    }

    @Test
    void buildsPayloadsFromTextFiles() throws Exception {
        Path input = tempDir.resolve("notes.txt");
        Files.writeString(input, "hello taskflow", StandardCharsets.UTF_8);

        List<Object> payloads = new ExampleWordCountClientPlugin().buildPayloads(List.of(input), "SUMMARY");

        assertEquals(1, payloads.size());
        ExamplePayload payload = assertInstanceOf(ExamplePayload.class, payloads.getFirst());
        assertEquals("notes.txt", payload.documentName());
        assertEquals("hello taskflow", payload.text());
    }

    @Test
    void rejectsUnsupportedInputExtension() throws Exception {
        Path input = tempDir.resolve("notes.md");
        Files.writeString(input, "hello", StandardCharsets.UTF_8);

        Exception error = assertThrows(Exception.class, () ->
                new ExampleWordCountClientPlugin().buildPayloads(List.of(input), "summary"));

        assertTrue(error.getMessage().contains("Unsupported example input file type"));
    }

    @Test
    void handlesSemanticSummaryResult() throws Exception {
        ExampleJobSummary summary = new ExampleJobSummary(
                2,
                5,
                List.of(
                        new ExampleTaskResult("one.txt", 2),
                        new ExampleTaskResult("two.txt", 3)
                )
        );
        JobResultMessage result = new JobResultMessage(
                "COORDINATOR",
                Instant.EPOCH.toString(),
                "job-example",
                ExampleTaskTypes.WORD_COUNT,
                true,
                summary,
                List.of(new ExampleTaskResult("one.txt", 2), new ExampleTaskResult("two.txt", 3))
        );

        new ExampleWordCountClientPlugin().handleResult(result, tempDir);

        String report = Files.readString(tempDir.resolve("example-word-count-summary.txt"));
        assertTrue(report.contains("documents=2"));
        assertTrue(report.contains("total_words=5"));
        assertTrue(report.contains("one.txt,2"));
    }
}
