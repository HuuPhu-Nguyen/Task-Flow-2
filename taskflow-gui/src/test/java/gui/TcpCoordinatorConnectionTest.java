package gui;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.PingMessage;
import protocol.TaskAssignMessage;

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
                    new TcpCoordinatorConnection("localhost", server.getLocalPort(), worker, listener);
            connection.start();

            try (Socket socket = server.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                socket.setSoTimeout(2_000);
                assertTrue(listener.awaitConnected());

                out.println(gson.toJson(new PingMessage("coordinator", Instant.EPOCH.toString())));

                String response = in.readLine();
                assertNotNull(response);
                assertTrue(response.contains("\"type\":\"PONG\""));
                assertTrue(response.contains("TEXT_ANALYSIS"));
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
    void delegatesTaskAssignmentsToWorkerRuntime() throws Exception {
        FakeWorkerRuntime worker = new FakeWorkerRuntime(Set.of("TEXT_ANALYSIS"));
        RecordingListener listener = new RecordingListener();
        TaskAssignMessage task = new TaskAssignMessage(
                "peer-1",
                Instant.EPOCH.toString(),
                "task-1",
                "job-1",
                "TEXT_ANALYSIS",
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

    private static final class RecordingListener implements TcpCoordinatorConnection.Listener {
        private final CountDownLatch connected = new CountDownLatch(1);
        private final CountDownLatch jobResultReceived = new CountDownLatch(1);
        private final AtomicReference<JobResultMessage> jobResult = new AtomicReference<>();

        @Override
        public void onConnected(CoordinatorConnection connection) {
            connected.countDown();
        }

        @Override
        public void onConnectionFailed(CoordinatorConnection connection, String error) {
        }

        @Override
        public void onDisconnected(CoordinatorConnection connection, String message) {
        }

        @Override
        public void onJobResult(CoordinatorConnection connection, JobResultMessage result) {
            jobResult.set(result);
            jobResultReceived.countDown();
        }

        private boolean awaitConnected() throws InterruptedException {
            return connected.await(2, TimeUnit.SECONDS);
        }

        private boolean awaitJobResult() throws InterruptedException {
            return jobResultReceived.await(2, TimeUnit.SECONDS);
        }
    }
}
