package peer.processors;

import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import objectstore.ObjectListing;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.PayloadIntegrityException;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.Test;
import protocol.PayloadLimits;
import protocol.TaskAssignMessage;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImageConversionProcessorTest {
    @Test
    void downloadsReferencedInputByObjectKey() throws Exception {
        byte[] inputBytes = pngBytes();
        MemoryObjectStore store = new MemoryObjectStore();
        ObjectReference reference = reference(inputBytes);
        store.put(reference, new ByteArrayInputStream(inputBytes));
        FilePayload input = new FilePayload("sample.png", null, reference);

        FilePayload result = new ImageConversionProcessor(() -> store).process(task(input));

        assertEquals("sample.png", result.fileName());
        assertTrue(result.hasInlineData());
        assertNotNull(ImageIO.read(
                new ByteArrayInputStream(Base64.getDecoder().decode(result.base64Data()))
        ));
        assertEquals(reference.key(), store.lastGetKey);
    }

    @Test
    void rejectsOutputAtExclusiveInlineBoundaryUntilObjectResultsExist() throws Exception {
        byte[] inputBytes = pngBytes();
        MemoryObjectStore store = new MemoryObjectStore();
        ObjectReference reference = reference(inputBytes);
        store.put(reference, new ByteArrayInputStream(inputBytes));
        FilePayload input = new FilePayload("sample.png", null, reference);
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "1");
        try {
            IOException error = assertThrows(
                    IOException.class,
                    () -> new ImageConversionProcessor(() -> store).process(task(input))
            );

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void rejectsCorruptReferencedInputBeforeImageProcessing() throws Exception {
        byte[] expected = pngBytes();
        byte[] corrupt = expected.clone();
        corrupt[corrupt.length - 1] ^= 1;
        MemoryObjectStore store = new MemoryObjectStore();
        ObjectReference reference = reference(expected);
        store.put(reference, new ByteArrayInputStream(corrupt));
        FilePayload input = new FilePayload("sample.png", null, reference);
        AtomicInteger decodeInvocations = new AtomicInteger();
        ImageConversionProcessor processor = new ImageConversionProcessor(() -> store) {
            @Override
            BufferedImage decode(byte[] rawBytes, String inputFileName) throws IOException {
                decodeInvocations.incrementAndGet();
                return super.decode(rawBytes, inputFileName);
            }
        };

        PayloadIntegrityException failure = assertThrows(
                PayloadIntegrityException.class,
                () -> processor.process(task(input))
        );

        assertEquals(PayloadIntegrityException.Mismatch.SHA256, failure.mismatch());
        assertEquals(reference.key(), store.lastGetKey);
        assertEquals(0, decodeInvocations.get());
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
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
                input,
                format
        );
    }

    private byte[] pngBytes() throws Exception {
        BufferedImage image = image();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ImageIO.write(image, "png", output);
        return output.toByteArray();
    }

    private byte[] jpegBytes() throws Exception {
        BufferedImage image = image();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ImageIO.write(image, "jpg", output);
        return output.toByteArray();
    }

    private BufferedImage image() {
        BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, Color.RED.getRGB());
        image.setRGB(1, 0, Color.GREEN.getRGB());
        image.setRGB(0, 1, Color.BLUE.getRGB());
        image.setRGB(1, 1, Color.WHITE.getRGB());
        return image;
    }

    private static ObjectReference reference(byte[] content) {
        return new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", "image-test"),
                content.length,
                sha256(content),
                "image/png"
        );
    }

    private static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(content)
            );
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must be available.", e);
        }
    }

    private static final class MemoryObjectStore implements ObjectStore {
        private final Map<String, StoredObject> objects = new ConcurrentHashMap<>();
        private String lastGetKey;

        @Override
        public ObjectReference put(ObjectReference reference, InputStream content)
                throws ObjectStoreException {
            try {
                objects.put(reference.key(), new StoredObject(reference, content.readAllBytes()));
                return reference;
            } catch (IOException e) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.STORAGE_FAILURE,
                        "Test upload failed.",
                        e
                );
            }
        }

        @Override
        public InputStream get(String key) throws ObjectStoreException {
            lastGetKey = TaskFlowObjectKeys.requireObjectKey(key);
            StoredObject object = objects.get(lastGetKey);
            if (object == null) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.NOT_FOUND,
                        "Missing test object."
                );
            }
            return new ByteArrayInputStream(object.bytes());
        }

        @Override
        public ObjectReference stat(String key) throws ObjectStoreException {
            StoredObject object = objects.get(TaskFlowObjectKeys.requireObjectKey(key));
            if (object == null) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.NOT_FOUND,
                        "Missing test object."
                );
            }
            return object.reference();
        }

        @Override
        public void delete(String key) {
            objects.remove(TaskFlowObjectKeys.requireObjectKey(key));
        }

        @Override
        public ObjectReference copy(String sourceKey, String destinationKey)
                throws ObjectStoreException {
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.STORAGE_FAILURE,
                    "Copy is not used by this test."
            );
        }

        @Override
        public ObjectListing list(String prefix, String startAfter, int limit) {
            return new ObjectListing(List.of(), null);
        }

        @Override
        public void close() {
            // Shared test instance deliberately survives separate provider opens.
        }

        private record StoredObject(ObjectReference reference, byte[] bytes) {
        }
    }
}
