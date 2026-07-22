package gui;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GuiCoordinatorConnectionServiceTest {
    @Test
    void startCreatesTracksAndStartsConnection() {
        FakeWorkerRuntime worker = new FakeWorkerRuntime();
        CapturingFactory factory = new CapturingFactory();
        GuiCoordinatorConnectionService service = new GuiCoordinatorConnectionService(worker, factory);

        service.start("example.test", 6789, new RecordingListener());

        FakeConnection connection = factory.connection.get();
        assertEquals("example.test", factory.host.get());
        assertEquals(6789, factory.port.get());
        assertSame(worker, factory.workerRuntime.get());
        assertTrue(connection.started);
        assertSame(connection, service.currentConnection());
        assertTrue(service.isCurrent(connection));
    }

    @Test
    void connectionFailureClearsCurrentConnectionAndReportsError() {
        CapturingFactory factory = new CapturingFactory();
        GuiCoordinatorConnectionService service =
                new GuiCoordinatorConnectionService(new FakeWorkerRuntime(), factory);
        RecordingListener listener = new RecordingListener();
        service.start("localhost", 6789, listener);
        FakeConnection connection = factory.connection.get();

        connection.listener.onConnectionFailed(connection, "connection refused");

        assertNull(service.currentConnection());
        assertEquals("connection refused", listener.connectionFailure.get());
        assertFalse(connection.closed);
    }

    @Test
    void clearWithCloseClosesConnectionAndRemovesCurrentReference() {
        CapturingFactory factory = new CapturingFactory();
        GuiCoordinatorConnectionService service =
                new GuiCoordinatorConnectionService(new FakeWorkerRuntime(), factory);
        service.start("localhost", 6789, new RecordingListener());
        FakeConnection connection = factory.connection.get();

        service.clear(connection, true);

        assertNull(service.currentConnection());
        assertTrue(connection.closed);
        assertFalse(service.isCurrent(connection));
    }

    @Test
    void stopClosesCurrentConnectionAndSuppressesLaterDisconnectCallback() {
        CapturingFactory factory = new CapturingFactory();
        GuiCoordinatorConnectionService service =
                new GuiCoordinatorConnectionService(new FakeWorkerRuntime(), factory);
        RecordingListener listener = new RecordingListener();
        service.start("localhost", 6789, listener);
        FakeConnection connection = factory.connection.get();

        service.stop();
        connection.listener.onDisconnected(connection, "lost");

        assertNull(service.currentConnection());
        assertTrue(connection.closed);
        assertNull(listener.disconnectedMessage.get());
    }

    @Test
    void jobResultsRouteToListener() {
        CapturingFactory factory = new CapturingFactory();
        GuiCoordinatorConnectionService service =
                new GuiCoordinatorConnectionService(new FakeWorkerRuntime(), factory);
        RecordingListener listener = new RecordingListener();
        service.start("localhost", 6789, listener);
        JobResultMessage result = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                List.of("done"));

        factory.connection.get().listener.onJobResult(factory.connection.get(), result);

        assertSame(result, listener.jobResult.get());
    }

    private static final class CapturingFactory implements GuiCoordinatorConnectionService.ConnectionFactory {
        private final AtomicReference<String> host = new AtomicReference<>();
        private final AtomicInteger port = new AtomicInteger();
        private final AtomicReference<GuiWorkerRuntime> workerRuntime = new AtomicReference<>();
        private final AtomicReference<FakeConnection> connection = new AtomicReference<>();

        @Override
        public StartableCoordinatorConnection create(
                String host,
                int port,
                GuiWorkerRuntime workerRuntime,
                CoordinatorConnectionListener listener) {
            this.host.set(host);
            this.port.set(port);
            this.workerRuntime.set(workerRuntime);
            FakeConnection fakeConnection = new FakeConnection(listener);
            connection.set(fakeConnection);
            return fakeConnection;
        }
    }

    private static final class FakeConnection implements StartableCoordinatorConnection {
        private final CoordinatorConnectionListener listener;
        private final PrintWriter writer = new PrintWriter(new StringWriter(), true);
        private boolean started;
        private boolean closed;

        private FakeConnection(CoordinatorConnectionListener listener) {
            this.listener = listener;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public PrintWriter writer() {
            return writer;
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class RecordingListener implements GuiCoordinatorConnectionService.Listener {
        private final AtomicReference<String> connectionFailure = new AtomicReference<>();
        private final AtomicReference<String> disconnectedMessage = new AtomicReference<>();
        private final AtomicReference<JobResultMessage> jobResult = new AtomicReference<>();

        @Override
        public void onConnected() {
        }

        @Override
        public void onConnectionFailed(String error) {
            connectionFailure.set(error);
        }

        @Override
        public void onDisconnected(String message) {
            disconnectedMessage.set(message);
        }

        @Override
        public void onJobResult(JobResultMessage result) {
            jobResult.set(result);
        }
    }

    private static final class FakeWorkerRuntime implements GuiWorkerRuntime {
        @Override
        public Set<String> supportedTaskTypes() {
            return Set.of("TEXT_ANALYSIS");
        }

        @Override
        public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
            return CompletableFuture.completedFuture(new TaskResultMessage(
                    "peer-1",
                    Instant.EPOCH.toString(),
                    task.getTaskId(),
                    task.getJobId(),
                    task.getAttemptNumber(),
                    task.getAssignmentId(),
                    "done",
                    true,
                    null
            ));
        }

        @Override
        public CompletableFuture<Boolean> submitTask(TaskAssignMessage task, PrintWriter out) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }
}
