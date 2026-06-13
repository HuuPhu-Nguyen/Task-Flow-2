package server.concreteJobs.text;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextAnalysisClientJobPluginTest {
    private static final Gson GSON = new Gson();

    @TempDir
    Path tempDir;

    @Test
    void buildsCustomPayloadsFromTextInputs() throws Exception {
        Path input = tempDir.resolve("notes.txt");
        Files.writeString(input, "hello taskflow");

        List<Object> payloads = new TextAnalysisTaskPlugin().buildPayloads(List.of(input), "csv");

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
                () -> new TextAnalysisTaskPlugin().buildPayloads(List.of(input), "csv"));
    }

    @Test
    void rejectsInputLargerThanConfiguredLimit() throws Exception {
        Path input = tempDir.resolve("large.txt");
        Files.writeString(input, "abcd");
        System.setProperty("taskflow.maxInputBytes", "3");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new TextAnalysisTaskPlugin().buildPayloads(List.of(input), "csv"));
            assertTrue(error.getMessage().contains("TASKFLOW_MAX_INPUT_BYTES"));
        } finally {
            System.clearProperty("taskflow.maxInputBytes");
        }
    }

    @Test
    void writesCsvResultsWithEscapedDocumentNames() throws Exception {
        Path outputDir = tempDir.resolve("out");
        TextAnalysisResult result = new TextAnalysisResult("a,b.txt", 2, 4, 20, 3);

        new TextAnalysisTaskPlugin().saveResults(List.<Object>of(result), outputDir);

        Path csv = outputDir.resolve("text-analysis-results.csv");
        assertTrue(Files.isRegularFile(csv));
        assertEquals("""
                document,line_count,word_count,character_count,unique_word_count
                "a,b.txt",2,4,20,3
                """.stripTrailing(), Files.readString(csv).replace("\r\n", "\n").stripTrailing());
    }
}
