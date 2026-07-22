package server.job;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssignmentIdentityTest {

    @Test
    void isImmutableFiveFieldRecordWithCanonicalUuid() {
        AssignmentIdentity identity = new AssignmentIdentity(
                " task-1 ",
                3,
                "550E8400-E29B-41D4-A716-446655440000",
                " peer-1 ",
                1_780_000_000_000L
        );

        assertTrue(AssignmentIdentity.class.isRecord());
        assertEquals(5, AssignmentIdentity.class.getRecordComponents().length);
        assertEquals(
                List.of("taskId", "attemptNumber", "assignmentId", "workerId", "leaseExpiresAtEpochMillis"),
                Arrays.stream(AssignmentIdentity.class.getRecordComponents())
                        .map(component -> component.getName())
                        .toList()
        );
        assertTrue(Modifier.isFinal(AssignmentIdentity.class.getModifiers()));
        assertEquals("task-1", identity.taskId());
        assertEquals(3, identity.attemptNumber());
        assertEquals("550e8400-e29b-41d4-a716-446655440000", identity.assignmentId());
        assertEquals("peer-1", identity.workerId());
        assertEquals(1_780_000_000_000L, identity.leaseExpiresAtEpochMillis());
    }

    @Test
    void factoryCreatesDifferentUuidForEachGeneration() {
        AssignmentIdentity first = AssignmentIdentity.create("task-1", 1, "peer-1", 100L);
        AssignmentIdentity second = AssignmentIdentity.create("task-1", 2, "peer-1", 200L);

        UUID.fromString(first.assignmentId());
        UUID.fromString(second.assignmentId());
        assertNotEquals(first.assignmentId(), second.assignmentId());
    }

    @Test
    void rejectsInvalidIdentityFields() {
        assertThrows(IllegalArgumentException.class,
                () -> AssignmentIdentity.create("", 1, "peer-1", 100L));
        assertThrows(IllegalArgumentException.class,
                () -> AssignmentIdentity.create("task-1", 0, "peer-1", 100L));
        assertThrows(IllegalArgumentException.class,
                () -> new AssignmentIdentity("task-1", 1, "not-a-uuid", "peer-1", 100L));
        assertThrows(IllegalArgumentException.class,
                () -> new AssignmentIdentity("task-1", 1, "1-1-1-1-1", "peer-1", 100L));
        assertThrows(IllegalArgumentException.class,
                () -> AssignmentIdentity.create("task-1", 1, " ", 100L));
        assertThrows(IllegalArgumentException.class,
                () -> AssignmentIdentity.create("task-1", 1, "peer-1", -1L));
    }
}
