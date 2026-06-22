package client.concreteJobs.conversion;

import client.ClientJobPlugin;
import client.PayloadLimits;
import com.google.gson.Gson;
import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConversionClientPluginTest {
    private static final Gson GSON = new Gson();

    @TempDir
    Path tempDir;

    @Test
    void discoversClientJobPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(ClientJobPlugin.class).spliterator(), false)
                .map(ClientJobPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of(
                ConversionTaskTypes.IMAGE_CONVERSION,
                ConversionTaskTypes.VIDEO_TRANSCODING
        ), taskTypes);
    }

    @Test
    void buildsFilePayloadsFromInputPaths() throws Exception {
        byte[] inputBytes = new byte[]{1, 2, 3, 4};
        Path input = tempDir.resolve("sample.png");
        Files.write(input, inputBytes);

        List<Object> payloads = new ImageConversionClientPlugin().buildPayloads(List.of(input), "png");

        assertEquals(1, payloads.size());
        FilePayload payload = GSON.fromJson(GSON.toJson(payloads.getFirst()), FilePayload.class);
        assertEquals("sample.png", payload.fileName());
        assertArrayEquals(inputBytes, Base64.getDecoder().decode(payload.base64Data()));
    }

    @Test
    void normalizesTargetParameterCase() {
        assertEquals("png", new ImageConversionClientPlugin().normalizeParameter("PNG"));
    }

    @Test
    void rejectsUnsupportedTargetParameter() {
        assertThrows(IllegalArgumentException.class,
                () -> new ImageConversionClientPlugin().normalizeParameter("tiff"));
    }

    @Test
    void rejectsUnsupportedInputExtension() throws Exception {
        Path input = tempDir.resolve("notes.txt");
        Files.writeString(input, "not an image");

        assertThrows(java.io.IOException.class,
                () -> new ImageConversionClientPlugin().buildPayloads(List.of(input), "png"));
    }

    @Test
    void rejectsInputLargerThanConfiguredLimit() throws Exception {
        Path input = tempDir.resolve("large.png");
        Files.write(input, new byte[]{1, 2, 3, 4});
        System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "3");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new ImageConversionClientPlugin().buildPayloads(List.of(input), "png"));
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_INPUT_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY);
        }
    }

    @Test
    void rejectsMoreInputsThanConfiguredTaskLimit() throws Exception {
        Path first = tempDir.resolve("first.png");
        Path second = tempDir.resolve("second.png");
        Files.write(first, new byte[]{1});
        Files.write(second, new byte[]{2});
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "1");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new ImageConversionClientPlugin().buildPayloads(List.of(first, second), "png"));
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_TASKS_PER_JOB_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
        }
    }

    @Test
    void rejectsEncodedPayloadLargerThanConfiguredJobLimit() throws Exception {
        Path first = tempDir.resolve("first.png");
        Path second = tempDir.resolve("second.png");
        Files.write(first, new byte[]{1, 2, 3});
        Files.write(second, new byte[]{4, 5, 6});
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, "7");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new ImageConversionClientPlugin().buildPayloads(List.of(first, second), "png"));
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void savesResultsInsideSelectedFolderWithSafeNames() throws Exception {
        byte[] outputBytes = new byte[]{9, 8, 7};
        Path outputDir = tempDir.resolve("out");
        Path outsideTarget = tempDir.resolve("escape.txt");
        FilePayload result = new FilePayload("../escape.txt", Base64.getEncoder().encodeToString(outputBytes));

        new ImageConversionClientPlugin().saveResults(List.<Object>of(result), outputDir);

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

        new ImageConversionClientPlugin().saveResults(List.<Object>of(firstResult, secondResult), outputDir);

        assertArrayEquals(first, Files.readAllBytes(outputDir.resolve("same.png")));
        assertArrayEquals(second, Files.readAllBytes(outputDir.resolve("same-1.png")));
        assertTrue(Files.isRegularFile(outputDir.resolve("same.png")));
        assertTrue(Files.isRegularFile(outputDir.resolve("same-1.png")));
    }

    @Test
    void rejectsResultPayloadLargerThanConfiguredLimit() {
        byte[] outputBytes = new byte[]{1, 2, 3, 4};
        Path outputDir = tempDir.resolve("out");
        FilePayload result = new FilePayload("large.png", Base64.getEncoder().encodeToString(outputBytes));
        System.setProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY, "3");
        try {
            java.io.IOException error = assertThrows(java.io.IOException.class,
                    () -> new ImageConversionClientPlugin().saveResults(List.<Object>of(result), outputDir));
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_RESULT_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY);
        }
    }
}
