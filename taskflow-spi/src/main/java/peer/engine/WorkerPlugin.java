package peer.engine;

public interface WorkerPlugin {
    String taskType();

    TaskProcessor<?> createProcessor();
}
