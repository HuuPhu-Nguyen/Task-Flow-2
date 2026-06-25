package client.plugins.text;

import client.ClientJobPlugin;
import protocol.PayloadLimits;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;
import text.model.TextAnalysisTaskTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextAnalysisClientPluginTest {
    private static final Gson GSON = new Gson();

    @TempDir
    Path tempDir;

    @Test
    void discoversClientJobPlugin() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(ClientJobPlugin.class).spliterator(), false)
                .map(ClientJobPlugin::taskType)
                .collect(Collectors.toSet());

        assertTrue(taskTypes.contains(TextAnalysisTaskTypes.TEXT_ANALYSIS));
    }

    @Test
    void buildsCustomPayloadsFromTextInputs() throws Exception {
        Path input = tempDir.resolve("notes.txt");
        Files.writeString(input, "hello taskflow");

        List<Object> payloads = new TextAnalysisClientPlugin().buildPayloads(List.of(input), "csv");

        assertEquals(1, payloads.size());
        TextAnalysisPayload payload = GSON.fromJson(GSON.toJson(payloads.getFirst()), TextAnalysisPayload.class);
        assertEquals("notes.txt", payload.documentName());
        assertEquals("hello taskflow", payload.text());
    }

    @Test
    void rejectsUnsupportedInputExtension() throws Exception {
        Path input = tempDir.resolve("image.png");
        Files.writeString(input, "not text");

        assertThrows(java.io.IOException.class,
                () -> new TextAnalysisClientPlugin().buildPayloads(List.of(input), "csv"));
    }

    @Test
    void rejectsInputLargerThanConfiguredLimit() throws Exception {
        Path input = tempDir.resolve("large.txt");
        Files.writeString(input, "abcd");
        System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "3");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new TextAnalysisClientPlugin().buildPayloads(List.of(input), "csv"));
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_INPUT_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY);
        }
    }

    @Test
    void rejectsMoreInputsThanConfiguredTaskLimit() throws Exception {
        Path first = tempDir.resolve("first.txt");
        Path second = tempDir.resolve("second.txt");
        Files.writeString(first, "one");
        Files.writeString(second, "two");
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "1");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new TextAnalysisClientPlugin().buildPayloads(List.of(first, second), "csv"));
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_TASKS_PER_JOB_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
        }
    }

    @Test
    void rejectsPayloadLargerThanConfiguredJobLimit() throws Exception {
        Path first = tempDir.resolve("first.txt");
        Path second = tempDir.resolve("second.txt");
        Files.writeString(first, "abc");
        Files.writeString(second, "def");
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, "5");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new TextAnalysisClientPlugin().buildPayloads(List.of(first, second), "csv"));
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void writesCsvResultsWithEscapedDocumentNames() throws Exception {
        Path outputDir = tempDir.resolve("out");
        TextAnalysisResult result = new TextAnalysisResult("a,b.txt", 2, 4, 20, 3);

        new TextAnalysisClientPlugin().saveResults(List.<Object>of(result), outputDir);

        Path csv = outputDir.resolve("text-analysis-results.csv");
        assertTrue(Files.isRegularFile(csv));
        assertEquals("""
                document,line_count,word_count,character_count,unique_word_count
                "a,b.txt",2,4,20,3
                """.stripTrailing(), Files.readString(csv).replace("\r\n", "\n").stripTrailing());
    }
}
