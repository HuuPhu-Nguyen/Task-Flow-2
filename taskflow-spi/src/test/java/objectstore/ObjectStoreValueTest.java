package objectstore;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObjectStoreValueTest {
    private static final String DIGEST = "A".repeat(64);

    @Test
    void objectReferenceCanonicalizesAndValidatesRequiredMetadata() {
        ObjectReference reference = new ObjectReference(
                TaskFlowObjectKeys.objectKey("jobs", "job-1", "input"),
                7L,
                DIGEST,
                " application/octet-stream "
        );

        assertEquals("a".repeat(64), reference.sha256());
        assertEquals("application/octet-stream", reference.contentType());
        assertThrows(IllegalArgumentException.class, () ->
                new ObjectReference("outside/file", 7L, DIGEST, "text/plain"));
        assertThrows(IllegalArgumentException.class, () ->
                new ObjectReference(reference.key(), -1L, DIGEST, "text/plain"));
        assertThrows(IllegalArgumentException.class, () ->
                new ObjectReference(reference.key(), 1L, "abc", "text/plain"));
        assertThrows(IllegalArgumentException.class, () ->
                new ObjectReference(reference.key(), 1L, DIGEST, "\r\n"));
    }

    @Test
    void generatedKeysNeverInterpretFilesystemOrUriSegments() {
        assertEquals(
                "taskflow/jobs/job-1/tasks/task_1/",
                TaskFlowObjectKeys.prefix("jobs", "job-1", "tasks", "task_1")
        );
        assertThrows(IllegalArgumentException.class, () ->
                TaskFlowObjectKeys.objectKey("jobs", "../../secret"));
        assertThrows(IllegalArgumentException.class, () ->
                TaskFlowObjectKeys.objectKey("jobs", "C:\\temp\\file"));
        assertThrows(IllegalArgumentException.class, () ->
                TaskFlowObjectKeys.requireObjectKey("taskflow/jobs//file"));
        assertThrows(IllegalArgumentException.class, () ->
                TaskFlowObjectKeys.requireObjectKey(" taskflow/jobs/file"));
        assertThrows(IllegalArgumentException.class, () ->
                TaskFlowObjectKeys.requirePrefix("taskflow/jobs"));
    }

    @Test
    void listingIsImmutableAndContinuationMustMatchLastObject() {
        ObjectReference first = reference("first");
        ObjectReference second = reference("second");
        ObjectListing listing = new ObjectListing(List.of(first, second), second.key());

        assertEquals(List.of(first, second), listing.objects());
        assertThrows(UnsupportedOperationException.class, () -> listing.objects().clear());
        assertThrows(IllegalArgumentException.class, () ->
                new ObjectListing(List.of(first, second), first.key()));
        assertThrows(IllegalArgumentException.class, () -> ObjectStore.requireListLimit(0));
        assertThrows(
                IllegalArgumentException.class,
                () -> ObjectStore.requireListLimit(ObjectStore.MAX_LIST_PAGE_SIZE + 1)
        );
    }

    private static ObjectReference reference(String name) {
        return new ObjectReference(
                TaskFlowObjectKeys.objectKey("contracts", name),
                1L,
                "0".repeat(64),
                "application/octet-stream"
        );
    }
}
