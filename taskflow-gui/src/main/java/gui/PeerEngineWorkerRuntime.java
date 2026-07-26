package gui;

import peer.engine.AssignmentCacheSnapshot;
import peer.engine.AssignmentExecution;
import peer.engine.PeerExecutionEngine;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

final class PeerEngineWorkerRuntime implements GuiWorkerRuntime {
    private final PeerExecutionEngine engine;

    PeerEngineWorkerRuntime(String nodeId) {
        this(new PeerExecutionEngine(nodeId));
    }

    PeerEngineWorkerRuntime(PeerExecutionEngine engine) {
        this.engine = engine;
    }

    @Override
    public Set<String> supportedTaskTypes() {
        return engine.getRegisteredTaskTypes();
    }

    @Override
    public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
        return engine.executeTask(task);
    }

    @Override
    public AssignmentExecution executeAssignment(TaskAssignMessage task) {
        return engine.executeAssignment(task);
    }

    @Override
    public AssignmentCacheSnapshot assignmentCacheSnapshot() {
        return engine.assignmentCacheSnapshot();
    }

    @Override
    public PongMessage capacityHeartbeat(String peerId, String timestamp) {
        return engine.capacityHeartbeat(timestamp);
    }

    @Override
    public void onCapacityChanged(Runnable listener) {
        engine.onCapacityChanged(listener);
    }

    @Override
    public void shutdown() {
        engine.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return engine.awaitTermination(timeout, unit);
    }
}
