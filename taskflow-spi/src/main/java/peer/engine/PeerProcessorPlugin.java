package peer.engine;

public interface PeerProcessorPlugin {
    String taskType();

    TaskProcessor<?> createProcessor();
}
