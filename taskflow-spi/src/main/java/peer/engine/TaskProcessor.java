package peer.engine;

import protocol.TaskAssignMessage;
import java.util.List;

public interface TaskProcessor<R> {
    /**
     * Executes the task and returns a List of JSON-encoded results.
     * Returning a List allows one task (like PDF split) to return multiple files.
     */
    R process(TaskAssignMessage task) throws Exception;
}