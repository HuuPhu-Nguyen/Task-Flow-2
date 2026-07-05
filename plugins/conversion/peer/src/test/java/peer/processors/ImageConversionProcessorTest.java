package peer.processors;

import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.LocalPayloadStorage;
import protocol.TaskAssignMessage;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImageConversionProcessorTest {
    @TempDir
    Path tempDir;

    @Test
    void readsReferencedInputPayload() throws Exception {
        configureExternalPayloadStorage("1000");
        try {
            byte[] inputBytes = pngBytes();
            FilePayload input = new FilePayload(
                    "sample.png",
                    null,
                    LocalPayloadStorage.storeBytes("sample.png", inputBytes)
            );

            FilePayload result = new ImageConversionProcessor().process(task(input));

            assertEquals("sample.png", result.fileName());
            assertTrue(result.hasInlineData());
            assertNotNull(ImageIO.read(new ByteArrayInputStream(Base64.getDecoder().decode(result.base64Data()))));
        } finally {
            clearExternalPayloadStorage();
        }
    }

    @Test
    void externalizesResultPayloadWhenConfigured() throws Exception {
        configureExternalPayloadStorage("0");
        try {
            FilePayload input = new FilePayload(
                    "sample.png",
                    Base64.getEncoder().encodeToString(pngBytes())
            );

            FilePayload result = new ImageConversionProcessor().process(task(input));

            assertEquals("sample.png", result.fileName());
            assertTrue(result.hasPayloadReference());
            byte[] outputBytes = LocalPayloadStorage.read(result.payloadReference(), 1024);
            assertNotNull(ImageIO.read(new ByteArrayInputStream(outputBytes)));
        } finally {
            clearExternalPayloadStorage();
        }
    }

    @Test
    void normalizesTargetFormatBeforeWritingImage() throws Exception {
        FilePayload input = new FilePayload(
                "sample.jpg",
                Base64.getEncoder().encodeToString(jpegBytes())
        );

        FilePayload result = new ImageConversionProcessor().process(task(input, " PNG "));

        assertEquals("sample.png", result.fileName());
        byte[] outputBytes = Base64.getDecoder().decode(result.base64Data());
        assertNotNull(ImageIO.read(new ByteArrayInputStream(outputBytes)));
    }

    private TaskAssignMessage task(FilePayload input) {
        return task(input, "png");
    }

    private TaskAssignMessage task(FilePayload input, String format) {
        return new TaskAssignMessage(
                "COORDINATOR",
                Instant.now().toString(),
                "task-1",
                "job-1",
                ConversionTaskTypes.IMAGE_CONVERSION,
                input,
                format
        );
    }

    private byte[] pngBytes() throws Exception {
        BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, Color.RED.getRGB());
        image.setRGB(1, 0, Color.GREEN.getRGB());
        image.setRGB(0, 1, Color.BLUE.getRGB());
        image.setRGB(1, 1, Color.WHITE.getRGB());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ImageIO.write(image, "png", output);
        return output.toByteArray();
    }

    private byte[] jpegBytes() throws Exception {
        BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, Color.RED.getRGB());
        image.setRGB(1, 0, Color.GREEN.getRGB());
        image.setRGB(0, 1, Color.BLUE.getRGB());
        image.setRGB(1, 1, Color.WHITE.getRGB());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ImageIO.write(image, "jpg", output);
        return output.toByteArray();
    }

    private void configureExternalPayloadStorage(String threshold) {
        System.setProperty(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_PROPERTY,
                tempDir.resolve("payloads").toString());
        System.setProperty(LocalPayloadStorage.EXTERNAL_PAYLOAD_THRESHOLD_BYTES_PROPERTY, threshold);
    }

    private void clearExternalPayloadStorage() {
        System.clearProperty(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_PROPERTY);
        System.clearProperty(LocalPayloadStorage.EXTERNAL_PAYLOAD_THRESHOLD_BYTES_PROPERTY);
    }
}
