package gui;

import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

interface GuiWorkerRuntime extends AutoCloseable {
    Set<String> supportedTaskTypes();

    CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task);

    CompletableFuture<Boolean> submitTask(TaskAssignMessage task, PrintWriter out);

    void shutdown();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    @Override
    default void close() {
        shutdown();
    }
}
