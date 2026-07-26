package gui;

import peer.engine.AssignmentCacheSnapshot;
import peer.engine.AssignmentExecution;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

interface GuiWorkerRuntime extends AutoCloseable {
    Set<String> supportedTaskTypes();

    CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task);

    default AssignmentExecution executeAssignment(TaskAssignMessage task) {
        return AssignmentExecution.started(executeTask(task));
    }

    default AssignmentCacheSnapshot assignmentCacheSnapshot() {
        return AssignmentCacheSnapshot.empty();
    }

    default PongMessage capacityHeartbeat(String peerId, String timestamp) {
        return new PongMessage(peerId, timestamp, supportedTaskTypes());
    }

    default void onCapacityChanged(Runnable listener) {
    }

    void shutdown();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    @Override
    default void close() {
        shutdown();
    }
}
