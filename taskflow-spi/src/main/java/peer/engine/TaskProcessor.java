package peer.engine;

import protocol.TaskAssignMessage;

public interface TaskProcessor<R> {
    /**
     * Executes a validated assignment and returns a JSON-serializable result.
     *
     * <p>The assignment is also the execution context. Its {@code taskId} is
     * the logical identity retained across coordinator retry generations; its
     * {@code assignmentId} identifies one generation and remains stable across
     * broker redelivery. A plugin declaring {@code REQUIRES_IDEMPOTENCY_KEY}
     * must pass the appropriate identity to its external system and document
     * which identity it uses.</p>
     */
    R process(TaskAssignMessage task) throws Exception;
}
