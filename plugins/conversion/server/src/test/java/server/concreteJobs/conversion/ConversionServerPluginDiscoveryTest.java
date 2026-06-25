package server.concreteJobs.conversion;

import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import server.job.TaskPlugin;

import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConversionServerPluginDiscoveryTest {
    @Test
    void discoversCoordinatorJobPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(TaskPlugin.class).spliterator(), false)
                .map(TaskPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of(
                ConversionTaskTypes.IMAGE_CONVERSION,
                ConversionTaskTypes.VIDEO_TRANSCODING
        ), taskTypes);
    }

    @Test
    void imagePluginAcceptsValidSubmission() {
        JobSubmitMessage submit = submit(
                ConversionTaskTypes.IMAGE_CONVERSION,
                List.<Object>of(filePayload("sample.png")),
                "png"
        );

        assertDoesNotThrow(() -> new ImageConversionTaskPlugin().validateSubmission(submit));
    }

    @Test
    void imagePluginRejectsUnsupportedTargetFormat() {
        JobSubmitMessage submit = submit(
                ConversionTaskTypes.IMAGE_CONVERSION,
                List.<Object>of(filePayload("sample.png")),
                "exe"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new ImageConversionTaskPlugin().validateSubmission(submit));

        assertTrue(error.getMessage().contains("Unsupported conversion target format"));
    }

    @Test
    void videoPluginRejectsMalformedBase64Payload() {
        JobSubmitMessage submit = submit(
                ConversionTaskTypes.VIDEO_TRANSCODING,
                List.<Object>of(new FilePayload("clip.mp4", "not base64")),
                "mp4"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new VideoTranscodingTaskPlugin().validateSubmission(submit));

        assertTrue(error.getMessage().contains("invalid Base64"));
    }

    @Test
    void videoPluginRejectsUnsupportedInputExtension() {
        JobSubmitMessage submit = submit(
                ConversionTaskTypes.VIDEO_TRANSCODING,
                List.<Object>of(filePayload("notes.txt")),
                "mp4"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new VideoTranscodingTaskPlugin().validateSubmission(submit));

        assertTrue(error.getMessage().contains("Unsupported conversion input file type"));
    }

    private static JobSubmitMessage submit(String taskType, List<Object> payloads, String parameter) {
        return new JobSubmitMessage(
                "client-1",
                Instant.EPOCH.toString(),
                "job-1",
                taskType,
                payloads,
                parameter
        );
    }

    private static FilePayload filePayload(String fileName) {
        return new FilePayload(
                fileName,
                Base64.getEncoder().encodeToString("payload".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
    }
}
