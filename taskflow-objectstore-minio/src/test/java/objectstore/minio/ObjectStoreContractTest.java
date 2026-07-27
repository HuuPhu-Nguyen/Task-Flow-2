package objectstore.minio;

import objectstore.ObjectListing;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.PayloadIntegrityException;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class ObjectStoreContractTest {
    private ObjectStore store;
    private String testPrefix;

    protected abstract ObjectStore createStore();

    protected abstract ObjectStore createUnavailableStore();

    @BeforeEach
    void openStore() {
        store = createStore();
        testPrefix = TaskFlowObjectKeys.prefix("contracts", UUID.randomUUID().toString());
    }

    @AfterEach
    void closeStore() throws ObjectStoreException {
        store.close();
    }

    @Test
    void putStreamsContentAndRoundTripsRequiredMetadata() throws Exception {
        byte[] content = "streamed TaskFlow object".getBytes(StandardCharsets.UTF_8);
        ObjectReference expected = referenceForKey(key("input"), content, "text/plain");
        CloseTrackingInputStream upload = new CloseTrackingInputStream(content);

        ObjectReference written = store.put(expected, upload);

        assertFalse(upload.closed, "ObjectStore must not close its caller-owned upload stream");
        assertEquals(expected, written);
        assertEquals(expected, store.stat(expected.key()));
        try (InputStream download = store.get(expected.key())) {
            assertArrayEquals(content, download.readAllBytes());
        }
        upload.close();
    }

    @Test
    void putIfAbsentNeverReplacesAnExistingObject() throws Exception {
        byte[] firstContent = new byte[6 * 1024 * 1024];
        byte[] replacementContent = new byte[firstContent.length];
        Arrays.fill(firstContent, (byte) 17);
        Arrays.fill(replacementContent, (byte) 23);
        String objectKey = key("immutable-output");
        ObjectReference first = referenceForKey(objectKey, firstContent, "text/plain");
        ObjectReference replacement =
                referenceForKey(objectKey, replacementContent, "text/plain");

        assertEquals(
                first,
                store.putIfAbsent(first, new ByteArrayInputStream(firstContent))
        );
        ObjectStoreException duplicate = assertThrows(
                ObjectStoreException.class,
                () -> store.putIfAbsent(
                        replacement,
                        new ByteArrayInputStream(replacementContent)
                )
        );

        assertEquals(ObjectStoreException.Reason.ALREADY_EXISTS, duplicate.reason());
        assertEquals(first, store.stat(objectKey));
        try (InputStream download = store.get(objectKey)) {
            assertArrayEquals(firstContent, download.readAllBytes());
        }
    }

    @Test
    void putRejectsBytesThatDoNotMatchTheReference() throws Exception {
        byte[] expectedContent = "expected".getBytes(StandardCharsets.UTF_8);
        byte[] corruptContent = "corrupt!".getBytes(StandardCharsets.UTF_8);
        ObjectReference reference = referenceForKey(
                key("corrupt-upload"),
                expectedContent,
                "application/octet-stream"
        );

        PayloadIntegrityException failure = assertThrows(
                PayloadIntegrityException.class,
                () -> store.put(reference, new ByteArrayInputStream(corruptContent))
        );

        assertEquals(PayloadIntegrityException.Mismatch.SHA256, failure.mismatch());
        ObjectStoreException missing = assertThrows(
                ObjectStoreException.class,
                () -> store.stat(reference.key())
        );
        assertEquals(ObjectStoreException.Reason.NOT_FOUND, missing.reason());
    }

    @Test
    void putRejectsContentExtendedBeyondTheReferenceLength() throws Exception {
        byte[] expectedContent = "expected".getBytes(StandardCharsets.UTF_8);
        byte[] extendedContent = "expected!".getBytes(StandardCharsets.UTF_8);
        ObjectReference reference = referenceForKey(
                key("extended-upload"),
                expectedContent,
                "application/octet-stream"
        );

        PayloadIntegrityException failure = assertThrows(
                PayloadIntegrityException.class,
                () -> store.put(reference, new ByteArrayInputStream(extendedContent))
        );

        assertEquals(PayloadIntegrityException.Mismatch.LENGTH, failure.mismatch());
        assertEquals(expectedContent.length + 1L, failure.actualLength());
        ObjectStoreException missing = assertThrows(
                ObjectStoreException.class,
                () -> store.stat(reference.key())
        );
        assertEquals(ObjectStoreException.Reason.NOT_FOUND, missing.reason());
    }

    @Test
    void putRejectsContentTruncatedBeforeTheReferenceLength() throws Exception {
        byte[] expectedContent = "expected".getBytes(StandardCharsets.UTF_8);
        byte[] truncatedContent = "expecte".getBytes(StandardCharsets.UTF_8);
        ObjectReference reference = referenceForKey(
                key("truncated-upload"),
                expectedContent,
                "application/octet-stream"
        );

        PayloadIntegrityException failure = assertThrows(
                PayloadIntegrityException.class,
                () -> store.put(reference, new ByteArrayInputStream(truncatedContent))
        );

        assertEquals(PayloadIntegrityException.Mismatch.LENGTH, failure.mismatch());
        assertEquals(truncatedContent.length, failure.actualLength());
        ObjectStoreException missing = assertThrows(
                ObjectStoreException.class,
                () -> store.stat(reference.key())
        );
        assertEquals(ObjectStoreException.Reason.NOT_FOUND, missing.reason());
    }

    @Test
    void copyPreservesMetadataAndKeepsSourceIndependent() throws Exception {
        byte[] content = "copy source".getBytes(StandardCharsets.UTF_8);
        ObjectReference source = referenceForKey(key("source"), content, "application/octet-stream");
        store.put(source, new ByteArrayInputStream(content));

        ObjectReference copied = store.copy(source.key(), key("promoted"));

        assertEquals(key("promoted"), copied.key());
        assertEquals(source.contentLength(), copied.contentLength());
        assertEquals(source.sha256(), copied.sha256());
        assertEquals(source.contentType(), copied.contentType());
        store.delete(source.key());
        try (InputStream download = store.get(copied.key())) {
            assertArrayEquals(content, download.readAllBytes());
        }
    }

    @Test
    void boundedListingPaginatesOnlyInsideRequestedPrefix() throws Exception {
        byte[] content = {1};
        List<String> names = List.of("a", "b", "c");
        for (String name : names) {
            ObjectReference reference = referenceForKey(key("selected", name), content, "application/octet-stream");
            store.put(reference, new ByteArrayInputStream(content));
        }
        ObjectReference outside = referenceForKey(key("outside"), content, "application/octet-stream");
        store.put(outside, new ByteArrayInputStream(content));

        String selectedPrefix = testPrefix + "selected/";
        ObjectListing first = store.list(selectedPrefix, 2);
        ObjectListing second = store.list(selectedPrefix, first.nextStartAfter(), 2);
        List<String> returned = new ArrayList<>();
        first.objects().forEach(reference -> returned.add(reference.key()));
        second.objects().forEach(reference -> returned.add(reference.key()));

        assertEquals(names.stream().map(name -> key("selected", name)).toList(), returned);
        assertEquals(2, first.objects().size());
        assertEquals(first.objects().getLast().key(), first.nextStartAfter());
        assertTrue(first.objects().stream()
                .allMatch(metadata -> metadata.lastModifiedAtEpochMillis() > 0L));
        assertEquals(1, second.objects().size());
        assertEquals(null, second.nextStartAfter());
        assertTrue(returned.stream().allMatch(key -> key.startsWith(selectedPrefix)));
        assertThrows(IllegalArgumentException.class, () -> store.list("other/", 1));
        assertThrows(IllegalArgumentException.class, () ->
                store.list(selectedPrefix, key("outside"), 1));
        assertThrows(IllegalArgumentException.class, () -> store.list(selectedPrefix, 0));
    }

    @Test
    void deleteIsIdempotentAndMissingObjectsAreClassified() throws Exception {
        byte[] content = {3};
        ObjectReference reference = referenceForKey(key("delete"), content, "application/octet-stream");
        store.put(reference, new ByteArrayInputStream(content));

        store.delete(reference.key());
        store.delete(reference.key());

        ObjectStoreException missingGet = assertThrows(
                ObjectStoreException.class,
                () -> store.get(reference.key())
        );
        ObjectStoreException missingStat = assertThrows(
                ObjectStoreException.class,
                () -> store.stat(reference.key())
        );
        assertEquals(ObjectStoreException.Reason.NOT_FOUND, missingGet.reason());
        assertEquals(ObjectStoreException.Reason.NOT_FOUND, missingStat.reason());
    }

    @Test
    void separateAttemptKeysHaveIndependentOwnership() throws Exception {
        byte[] firstContent = "attempt-one".getBytes(StandardCharsets.UTF_8);
        byte[] secondContent = "attempt-two".getBytes(StandardCharsets.UTF_8);
        String firstKey = key("jobs", "job-1", "tasks", "task-1", "attempts", "1", UUID.randomUUID().toString(),
                "output");
        String secondKey = key("jobs", "job-1", "tasks", "task-1", "attempts", "2", UUID.randomUUID().toString(),
                "output");
        store.put(referenceForKey(firstKey, firstContent, "application/octet-stream"),
                new ByteArrayInputStream(firstContent));
        store.put(referenceForKey(secondKey, secondContent, "application/octet-stream"),
                new ByteArrayInputStream(secondContent));

        store.delete(firstKey);

        ObjectStoreException missing = assertThrows(ObjectStoreException.class, () -> store.stat(firstKey));
        assertEquals(ObjectStoreException.Reason.NOT_FOUND, missing.reason());
        try (InputStream second = store.get(secondKey)) {
            assertArrayEquals(secondContent, second.readAllBytes());
        }
    }

    @Test
    void invalidKeysAreRejectedBeforeStorageMutation() {
        byte[] content = {7};
        ObjectReference valid = referenceForKey(key("valid"), content, "application/octet-stream");

        assertThrows(IllegalArgumentException.class, () -> store.get("../escape"));
        assertThrows(IllegalArgumentException.class, () -> store.delete("taskflow/../../escape"));
        assertThrows(IllegalArgumentException.class, () ->
                store.copy(valid.key(), "file:///tmp/output"));
    }

    @Test
    void unavailableStoreIsClassifiedAsStorageFailure() throws Exception {
        try (ObjectStore unavailable = createUnavailableStore()) {
            ObjectStoreException failure = assertThrows(
                    ObjectStoreException.class,
                    () -> unavailable.stat(TaskFlowObjectKeys.objectKey("contracts", "unavailable"))
            );
            assertEquals(ObjectStoreException.Reason.STORAGE_FAILURE, failure.reason());
            assertTrue(failure.getCause() != null);
        }
    }

    private String key(String... relativeSegments) {
        String relative = String.join("/", relativeSegments);
        return TaskFlowObjectKeys.requireObjectKey(testPrefix + relative);
    }

    private static ObjectReference referenceForKey(String key, byte[] content, String contentType) {
        return new ObjectReference(key, content.length, sha256(content), contentType);
    }

    private static String sha256(byte[] content) {
        try {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(content)
            );
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must be available.", e);
        }
    }

    private static final class CloseTrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        private CloseTrackingInputStream(byte[] content) {
            super(content);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
