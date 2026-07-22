package peer.engine;

import protocol.TaskResultMessage;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public record AssignmentExecution(
        Disposition disposition,
        CompletableFuture<TaskResultMessage> resultFuture
) {
    public AssignmentExecution {
        Objects.requireNonNull(disposition, "disposition");
        Objects.requireNonNull(resultFuture, "resultFuture");
    }

    public static AssignmentExecution started(CompletableFuture<TaskResultMessage> resultFuture) {
        return new AssignmentExecution(Disposition.STARTED, resultFuture);
    }

    public enum Disposition {
        STARTED,
        DUPLICATE_RUNNING,
        DUPLICATE_COMPLETED
    }
}
