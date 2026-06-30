package gui;

import peer.engine.PeerExecutionEngine;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.io.PrintWriter;
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
    public CompletableFuture<Boolean> submitTask(TaskAssignMessage task, PrintWriter out) {
        return engine.submitTask(task, out);
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
