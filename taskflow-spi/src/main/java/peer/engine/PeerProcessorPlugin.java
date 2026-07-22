package peer.engine;

import plugin.RetrySafety;

public interface PeerProcessorPlugin {
    String taskType();

    /**
     * Declares the retry behavior of the processor returned by this plugin.
     * The paired coordinator-side task plugin must return the same value.
     */
    RetrySafety retrySafety();

    TaskProcessor<?> createProcessor();
}
