package gui;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.PingMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TcpCoordinatorConnectionTest {
    private final Gson gson = new Gson();

    @Test
    void respondsToPingWithWorkerCapabilities() throws Exception {
        FakeWorkerRuntime worker = new FakeWorkerRuntime(Set.of("TEXT_ANALYSIS"));
        RecordingListener listener = new RecordingListener();

        try (ServerSocket server = new ServerSocket(0)) {
            TcpCoordinatorConnection connection =
                    new TcpCoordinatorConnection("gui-peer-1", "localhost", server.getLocalPort(), worker, listener);
            connection.start();

            try (Socket socket = server.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                socket.setSoTimeout(2_000);
                assertTrue(listener.awaitConnected());

                out.println(gson.toJson(new PingMessage("coordinator", Instant.EPOCH.toString())));

                String response = in.readLine();
                assertNotNull(response);
                PongMessage pong = gson.fromJson(response, PongMessage.class);
                assertEquals("gui-peer-1", pong.getNodeId());
                assertEquals(List.of("TEXT_ANALYSIS"), pong.getSupportedTaskTypes());
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void routesJobResultsToListener() throws Exception {
        FakeWorkerRuntime worker = new FakeWorkerRuntime(Set.of("TEXT_ANALYSIS"));
        RecordingListener listener = new RecordingListener();
        JobResultMessage result = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                List.of("done")
        );

        try (ServerSocket server = new ServerSocket(0)) {
            TcpCoordinatorConnection connection =
                    new TcpCoordinatorConnection("localhost", server.getLocalPort(), worker, listener);
            connection.start();

            try (Socket socket = server.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                assertTrue(listener.awaitConnected());

                out.println(gson.toJson(result));

                assertTrue(listener.awaitJobResult());
                assertEquals("job-1", listener.jobResult.get().getJobId());
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void reportsInitialConnectionFailure() throws Exception {
        FakeWorkerRuntime worker = new FakeWorkerRuntime(Set.of("TEXT_ANALYSIS"));
        RecordingListener listener = new RecordingListener();
        int closedPort;
        try (ServerSocket server = new ServerSocket(0)) {
            closedPort = server.getLocalPort();
        }

        TcpCoordinatorConnection connection =
                new TcpCoordinatorConnection("localhost", closedPort, worker, listener);
        connection.start();
        try {
            assertTrue(listener.awaitConnectionFailed());
            assertNotNull(listener.connectionFailure.get());
            assertFalse(listener.connected());
            assertFalse(listener.disconnected());
        } finally {
            connection.close();
        }
    }

    @Test
    void reportsDisconnectAfterSuccessfulConnection() throws Exception {
        FakeWorkerRuntime worker = new FakeWorkerRuntime(Set.of("TEXT_ANALYSIS"));
        RecordingListener listener = new RecordingListener();

        try (ServerSocket server = new ServerSocket(0)) {
            TcpCoordinatorConnection connection =
                    new TcpCoordinatorConnection("localhost", server.getLocalPort(), worker, listener);
            connection.start();

            try (Socket ignored = server.accept()) {
                assertTrue(listener.awaitConnected());
            }

            assertTrue(listener.awaitDisconnected());
            assertEquals("Coordinator connection closed.", listener.disconnectedMessage.get());
            assertFalse(listener.connectionFailed());
            connection.close();
        }
    }

    @Test
    void malformedInboundMessageDoesNotPreventLaterJobResult() throws Exception {
        FakeWorkerRuntime worker = new FakeWorkerRuntime(Set.of("TEXT_ANALYSIS"));
        RecordingListener listener = new RecordingListener();
        JobResultMessage result = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                List.of("done")
        );

        try (ServerSocket server = new ServerSocket(0)) {
            TcpCoordinatorConnection connection =
                    new TcpCoordinatorConnection("localhost", server.getLocalPort(), worker, listener);
            connection.start();

            try (Socket socket = server.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                assertTrue(listener.awaitConnected());

                out.println("{not-json");
                out.println(gson.toJson(result));

                assertTrue(listener.awaitJobResult());
                assertEquals("job-1", listener.jobResult.get().getJobId());
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void delegatesTaskAssignmentsToWorkerRuntime() throws Exception {
        FakeWorkerRuntime worker = new FakeWorkerRuntime(Set.of("TEXT_ANALYSIS"));
        RecordingListener listener = new RecordingListener();
        TaskAssignMessage task = new TaskAssignMessage(
                "peer-1",
                Instant.EPOCH.toString(),
                "task-1",
                "job-1",
                "TEXT_ANALYSIS",
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
                "payload",
                "summary"
        );

        try (ServerSocket server = new ServerSocket(0)) {
            TcpCoordinatorConnection connection =
                    new TcpCoordinatorConnection("localhost", server.getLocalPort(), worker, listener);
            connection.start();

            try (Socket socket = server.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                assertTrue(listener.awaitConnected());

                out.println(gson.toJson(task));

                assertTrue(worker.awaitAssignment());
                assertEquals("task-1", worker.assignedTask.get().getTaskId());
                assertEquals(1, worker.assignedTask.get().getAttemptNumber());
                assertEquals("550e8400-e29b-41d4-a716-446655440000",
                        worker.assignedTask.get().getAssignmentId());
            } finally {
                connection.close();
            }
        }
    }

    private static final class FakeWorkerRuntime implements GuiWorkerRuntime {
        private final Set<String> taskTypes;
        private final CountDownLatch assigned = new CountDownLatch(1);
        private final AtomicReference<TaskAssignMessage> assignedTask = new AtomicReference<>();

        private FakeWorkerRuntime(Set<String> taskTypes) {
            this.taskTypes = taskTypes;
        }

        @Override
        public Set<String> supportedTaskTypes() {
            return taskTypes;
        }

        @Override
        public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
            assignedTask.set(task);
            assigned.countDown();
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
            assignedTask.set(task);
            assigned.countDown();
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        private boolean awaitAssignment() throws InterruptedException {
            return assigned.await(2, TimeUnit.SECONDS);
        }
    }

    private static final class RecordingListener implements CoordinatorConnectionListener {
        private final CountDownLatch connected = new CountDownLatch(1);
        private final CountDownLatch connectionFailed = new CountDownLatch(1);
        private final CountDownLatch disconnected = new CountDownLatch(1);
        private final CountDownLatch jobResultReceived = new CountDownLatch(1);
        private final AtomicReference<String> connectionFailure = new AtomicReference<>();
        private final AtomicReference<String> disconnectedMessage = new AtomicReference<>();
        private final AtomicReference<JobResultMessage> jobResult = new AtomicReference<>();

        @Override
        public void onConnected(CoordinatorConnection connection) {
            connected.countDown();
        }

        @Override
        public void onConnectionFailed(CoordinatorConnection connection, String error) {
            connectionFailure.set(error);
            connectionFailed.countDown();
        }

        @Override
        public void onDisconnected(CoordinatorConnection connection, String message) {
            disconnectedMessage.set(message);
            disconnected.countDown();
        }

        @Override
        public void onJobResult(CoordinatorConnection connection, JobResultMessage result) {
            jobResult.set(result);
            jobResultReceived.countDown();
        }

        private boolean awaitConnected() throws InterruptedException {
            return connected.await(2, TimeUnit.SECONDS);
        }

        private boolean awaitConnectionFailed() throws InterruptedException {
            return connectionFailed.await(2, TimeUnit.SECONDS);
        }

        private boolean awaitDisconnected() throws InterruptedException {
            return disconnected.await(2, TimeUnit.SECONDS);
        }

        private boolean awaitJobResult() throws InterruptedException {
            return jobResultReceived.await(2, TimeUnit.SECONDS);
        }

        private boolean connected() {
            return connected.getCount() == 0;
        }

        private boolean connectionFailed() {
            return connectionFailed.getCount() == 0;
        }

        private boolean disconnected() {
            return disconnected.getCount() == 0;
        }
    }
}
