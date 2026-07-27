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
    void attemptOutputKeyIncludesTheCompleteAssignmentIdentity() {
        String assignmentId = "550e8400-e29b-41d4-a716-446655440000";

        assertEquals(
                "taskflow/jobs/job-1/tasks/task-1/attempts/2/"
                        + assignmentId + "/output",
                TaskFlowObjectKeys.attemptOutputKey(
                        "job-1",
                        "task-1",
                        2,
                        assignmentId
                )
        );
        assertThrows(IllegalArgumentException.class, () ->
                TaskFlowObjectKeys.attemptOutputKey(
                        "job-1",
                        "task-1",
                        0,
                        assignmentId
                ));
        assertThrows(IllegalArgumentException.class, () ->
                TaskFlowObjectKeys.attemptOutputKey(
                        "job-1",
                        "task-1",
                        1,
                        "not-a-uuid"
                ));

        TaskFlowObjectKeys.AttemptOutputIdentity parsed =
                TaskFlowObjectKeys.parseAttemptOutputKey(
                        TaskFlowObjectKeys.attemptOutputKey(
                                "job-1",
                                "task-1",
                                2,
                                assignmentId
                        )
                ).orElseThrow();
        assertEquals("job-1", parsed.jobId());
        assertEquals("task-1", parsed.taskId());
        assertEquals(2, parsed.attemptNumber());
        assertEquals(assignmentId, parsed.assignmentId());
        assertEquals(
                TaskFlowObjectKeys.attemptOutputKey("job-1", "task-1", 2, assignmentId),
                parsed.key()
        );
        assertEquals(
                java.util.Optional.empty(),
                TaskFlowObjectKeys.parseAttemptOutputKey(
                        TaskFlowObjectKeys.objectKey("jobs", "job-1", "input")
                )
        );
        assertEquals(
                java.util.Optional.empty(),
                TaskFlowObjectKeys.parseAttemptOutputKey(
                        TaskFlowObjectKeys.objectKey(
                                "jobs",
                                "job-1",
                                "tasks",
                                "task-1",
                                "attempts",
                                "not-a-number",
                                assignmentId,
                                "output"
                        )
                )
        );
    }

    @Test
    void listingIsImmutableAndContinuationMustMatchLastObject() {
        ObjectReference first = reference("first");
        ObjectReference second = reference("second");
        ObjectMetadata firstMetadata = new ObjectMetadata(first, 10L);
        ObjectMetadata secondMetadata = new ObjectMetadata(second, 20L);
        ObjectListing listing =
                new ObjectListing(List.of(firstMetadata, secondMetadata), second.key());

        assertEquals(List.of(firstMetadata, secondMetadata), listing.objects());
        assertThrows(UnsupportedOperationException.class, () -> listing.objects().clear());
        assertThrows(IllegalArgumentException.class, () ->
                new ObjectListing(List.of(firstMetadata, secondMetadata), first.key()));
        assertThrows(IllegalArgumentException.class, () -> new ObjectMetadata(first, -1L));
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
