package server.concreteJobs.conversion;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.FilePayload;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConversionClientJobPluginTest {
    private static final Gson GSON = new Gson();

    @TempDir
    Path tempDir;

    @Test
    void buildsFilePayloadsFromInputPaths() throws Exception {
        byte[] inputBytes = new byte[]{1, 2, 3, 4};
        Path input = tempDir.resolve("sample.png");
        Files.write(input, inputBytes);

        List<Object> payloads = new ImageConversionTaskPlugin().buildPayloads(List.of(input), "png");

        assertEquals(1, payloads.size());
        FilePayload payload = GSON.fromJson(GSON.toJson(payloads.getFirst()), FilePayload.class);
        assertEquals("sample.png", payload.fileName());
        assertArrayEquals(inputBytes, Base64.getDecoder().decode(payload.base64Data()));
    }

    @Test
    void normalizesTargetParameterCase() {
        assertEquals("png", new ImageConversionTaskPlugin().normalizeParameter("PNG"));
    }

    @Test
    void rejectsUnsupportedTargetParameter() {
        assertThrows(IllegalArgumentException.class,
                () -> new ImageConversionTaskPlugin().normalizeParameter("tiff"));
    }

    @Test
    void rejectsUnsupportedInputExtension() throws Exception {
        Path input = tempDir.resolve("notes.txt");
        Files.writeString(input, "not an image");

        assertThrows(java.io.IOException.class,
                () -> new ImageConversionTaskPlugin().buildPayloads(List.of(input), "png"));
    }

    @Test
    void savesResultsInsideSelectedFolderWithSafeNames() throws Exception {
        byte[] outputBytes = new byte[]{9, 8, 7};
        Path outputDir = tempDir.resolve("out");
        Path outsideTarget = tempDir.resolve("escape.txt");
        FilePayload result = new FilePayload("../escape.txt", Base64.getEncoder().encodeToString(outputBytes));

        new ImageConversionTaskPlugin().saveResults(List.<Object>of(result), outputDir);

        assertFalse(Files.exists(outsideTarget));
        assertArrayEquals(outputBytes, Files.readAllBytes(outputDir.resolve("escape.txt")));
    }

    @Test
    void savesDuplicateResultNamesWithoutOverwriting() throws Exception {
        byte[] first = new byte[]{1};
        byte[] second = new byte[]{2};
        Path outputDir = tempDir.resolve("out");
        FilePayload firstResult = new FilePayload("same.png", Base64.getEncoder().encodeToString(first));
        FilePayload secondResult = new FilePayload("same.png", Base64.getEncoder().encodeToString(second));

        new ImageConversionTaskPlugin().saveResults(List.<Object>of(firstResult, secondResult), outputDir);

        assertArrayEquals(first, Files.readAllBytes(outputDir.resolve("same.png")));
        assertArrayEquals(second, Files.readAllBytes(outputDir.resolve("same-1.png")));
        assertTrue(Files.isRegularFile(outputDir.resolve("same.png")));
        assertTrue(Files.isRegularFile(outputDir.resolve("same-1.png")));
    }
}
