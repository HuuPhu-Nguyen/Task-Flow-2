package peer.engine;

import plugin.RetrySafety;
import plugin.TaskResourceProfile;

public interface PeerProcessorPlugin {
    String taskType();

    /**
     * Declares the retry behavior of the processor returned by this plugin.
     * The paired coordinator-side task plugin must return the same value.
     */
    RetrySafety retrySafety();

    /**
     * Executor-visible mirror of the paired coordinator task profile.
     */
    TaskResourceProfile resourceProfile();

    TaskProcessor<?> createProcessor();
}
