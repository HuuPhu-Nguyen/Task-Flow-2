package client.plugins.conversion;

import client.ClientJobPlugin;
import com.google.gson.Gson;
import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import objectstore.ObjectListing;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.PayloadLimits;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
        Set<String> taskTypes = StreamSupport.stream(
                        ServiceLoader.load(ClientJobPlugin.class).spliterator(),
                        false
                )
                .map(ClientJobPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of(
                ConversionTaskTypes.IMAGE_CONVERSION,
                ConversionTaskTypes.VIDEO_TRANSCODING
        ), taskTypes);
    }

    @Test
    void buildsSmallFilePayloadsInline() throws Exception {
        byte[] inputBytes = new byte[]{1, 2, 3, 4};
        Path input = tempDir.resolve("sample.png");
        Files.write(input, inputBytes);

        List<Object> payloads = new ImageConversionClientPlugin()
                .buildPayloads(List.of(input), "png");

        assertEquals(1, payloads.size());
        FilePayload payload = GSON.fromJson(GSON.toJson(payloads.getFirst()), FilePayload.class);
        assertEquals("sample.png", payload.fileName());
        assertArrayEquals(inputBytes, Base64.getDecoder().decode(payload.base64Data()));
        assertFalse(payload.hasObjectReference());
    }

    @Test
    void uploadsInputAtExclusiveInlineBoundary() throws Exception {
        byte[] inputBytes = new byte[]{1, 2, 3, 4};
        Path input = tempDir.resolve("sample.png");
        Files.write(input, inputBytes);
        MemoryObjectStore store = new MemoryObjectStore();
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "4");
        try {
            List<Object> payloads = new ImageConversionClientPlugin(() -> store)
                    .buildPayloads(List.of(input), "png");

            FilePayload payload = GSON.fromJson(
                    GSON.toJson(payloads.getFirst()),
                    FilePayload.class
            );
            assertEquals("sample.png", payload.fileName());
            assertFalse(payload.hasInlineData());
            assertTrue(payload.hasObjectReference());
            assertTrue(payload.objectReference().key().startsWith("taskflow/inputs/"));
            assertEquals(inputBytes.length, payload.objectReference().contentLength());
            assertArrayEquals(inputBytes, store.bytes(payload.objectReference().key()));
        } finally {
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void deletesUploadedInputsWhenBuildFailsAfterStorage() throws Exception {
        Path input = tempDir.resolve("sample.png");
        Files.write(input, new byte[]{1, 2, 3, 4});
        MemoryObjectStore store = new MemoryObjectStore();
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "0");
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, "1");
        try {
            IOException error = assertThrows(
                    IOException.class,
                    () -> new ImageConversionClientPlugin(() -> store)
                            .buildPayloads(List.of(input), "png")
            );

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV));
            assertTrue(store.isEmpty());
        } finally {
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
        }
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

        assertThrows(IOException.class,
                () -> new ImageConversionClientPlugin().buildPayloads(List.of(input), "png"));
    }

    @Test
    void rejectsInputLargerThanConfiguredLimit() throws Exception {
        Path input = tempDir.resolve("large.png");
        Files.write(input, new byte[]{1, 2, 3, 4});
        System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "3");
        try {
            IOException error = assertThrows(
                    IOException.class,
                    () -> new ImageConversionClientPlugin().buildPayloads(List.of(input), "png")
            );
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
            IOException error = assertThrows(
                    IOException.class,
                    () -> new ImageConversionClientPlugin()
                            .buildPayloads(List.of(first, second), "png")
            );
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
            IOException error = assertThrows(
                    IOException.class,
                    () -> new ImageConversionClientPlugin()
                            .buildPayloads(List.of(first, second), "png")
            );
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
        FilePayload result = new FilePayload(
                "../escape.txt",
                Base64.getEncoder().encodeToString(outputBytes)
        );

        new ImageConversionClientPlugin().saveResults(List.<Object>of(result), outputDir);

        assertFalse(Files.exists(outsideTarget));
        assertArrayEquals(outputBytes, Files.readAllBytes(outputDir.resolve("escape.txt")));
    }

    @Test
    void downloadsObjectReferencedResultsByKey() throws Exception {
        byte[] outputBytes = new byte[]{9, 8, 7};
        Path outputDir = tempDir.resolve("out");
        MemoryObjectStore store = new MemoryObjectStore();
        ObjectReference reference = reference(
                TaskFlowObjectKeys.objectKey("results", "test"),
                outputBytes.length
        );
        store.put(reference, new ByteArrayInputStream(outputBytes));
        FilePayload result = new FilePayload("../escape.txt", null, reference);

        new ImageConversionClientPlugin(() -> store)
                .saveResults(List.<Object>of(result), outputDir);

        assertArrayEquals(outputBytes, Files.readAllBytes(outputDir.resolve("escape.txt")));
    }

    @Test
    void savesDuplicateResultNamesWithoutOverwriting() throws Exception {
        byte[] first = new byte[]{1};
        byte[] second = new byte[]{2};
        Path outputDir = tempDir.resolve("out");
        FilePayload firstResult = new FilePayload(
                "same.png",
                Base64.getEncoder().encodeToString(first)
        );
        FilePayload secondResult = new FilePayload(
                "same.png",
                Base64.getEncoder().encodeToString(second)
        );

        new ImageConversionClientPlugin()
                .saveResults(List.<Object>of(firstResult, secondResult), outputDir);

        assertArrayEquals(first, Files.readAllBytes(outputDir.resolve("same.png")));
        assertArrayEquals(second, Files.readAllBytes(outputDir.resolve("same-1.png")));
    }

    @Test
    void rejectsResultPayloadLargerThanConfiguredLimit() {
        byte[] outputBytes = new byte[]{1, 2, 3, 4};
        Path outputDir = tempDir.resolve("out");
        FilePayload result = new FilePayload(
                "large.png",
                Base64.getEncoder().encodeToString(outputBytes)
        );
        System.setProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY, "3");
        try {
            IOException error = assertThrows(
                    IOException.class,
                    () -> new ImageConversionClientPlugin()
                            .saveResults(List.<Object>of(result), outputDir)
            );
            assertTrue(error.getMessage().contains(PayloadLimits.MAX_RESULT_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY);
        }
    }

    private static ObjectReference reference(String key, long size) {
        return new ObjectReference(
                key,
                size,
                "0".repeat(64),
                "application/octet-stream"
        );
    }

    private static final class MemoryObjectStore implements ObjectStore {
        private final Map<String, StoredObject> objects = new ConcurrentHashMap<>();

        @Override
        public ObjectReference put(ObjectReference reference, InputStream content)
                throws ObjectStoreException {
            try {
                objects.put(reference.key(), new StoredObject(reference, content.readAllBytes()));
                return reference;
            } catch (IOException e) {
                throw failure(e);
            }
        }

        @Override
        public InputStream get(String key) throws ObjectStoreException {
            StoredObject object = objects.get(TaskFlowObjectKeys.requireObjectKey(key));
            if (object == null) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.NOT_FOUND,
                        "Missing test object: " + key
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
                        "Missing test object: " + key
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
            StoredObject source = objects.get(TaskFlowObjectKeys.requireObjectKey(sourceKey));
            if (source == null) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.NOT_FOUND,
                        "Missing test object: " + sourceKey
                );
            }
            ObjectReference copied = new ObjectReference(
                    destinationKey,
                    source.reference().contentLength(),
                    source.reference().sha256(),
                    source.reference().contentType()
            );
            objects.put(copied.key(), new StoredObject(copied, source.bytes().clone()));
            return copied;
        }

        @Override
        public ObjectListing list(String prefix, String startAfter, int limit) {
            return new ObjectListing(List.of(), null);
        }

        @Override
        public void close() {
            // Shared test instance deliberately survives separate provider opens.
        }

        byte[] bytes(String key) {
            return objects.get(key).bytes().clone();
        }

        boolean isEmpty() {
            return objects.isEmpty();
        }

        private static ObjectStoreException failure(Exception cause) {
            return new ObjectStoreException(
                    ObjectStoreException.Reason.STORAGE_FAILURE,
                    "Test object-store failure.",
                    cause
            );
        }

        private record StoredObject(ObjectReference reference, byte[] bytes) {
        }
    }
}
