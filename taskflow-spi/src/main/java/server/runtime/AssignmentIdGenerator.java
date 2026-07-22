package server.runtime;

/**
 * Creates the opaque UUID candidate for one task-assignment generation.
 */
public interface AssignmentIdGenerator {
    String nextAssignmentId();
}
