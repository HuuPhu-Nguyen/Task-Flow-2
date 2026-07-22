package peer.engine;

import org.junit.jupiter.api.Test;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerExecutionEngineTest {

    @Test
    void submitTaskReportsSuccessfulResultSend() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> "done");
            StringWriter buffer = new StringWriter();
            PrintWriter out = new PrintWriter(buffer);

            boolean sent = engine.submitTask(testTask(), out).get(2, TimeUnit.SECONDS);

            assertTrue(sent);
            assertTrue(buffer.toString().contains("\"protocolVersion\":2"));
            assertTrue(buffer.toString().contains("\"type\":\"TASK_RESULT\""));
            assertTrue(buffer.toString().contains("\"taskId\":\"task-1\""));
            assertTrue(buffer.toString().contains("\"attemptNumber\":7"));
            assertTrue(buffer.toString().contains(
                    "\"assignmentId\":\"550e8400-e29b-41d4-a716-446655440000\""));
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void submitTaskReportsFailedResultSend() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> "done");
            PrintWriter out = new PrintWriter(new FailingWriter());

            boolean sent = engine.submitTask(testTask(), out).get(2, TimeUnit.SECONDS);

            assertFalse(sent);
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void failedExecutionEchoesAssignmentIdentity() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> {
                throw new IllegalStateException("processor failed");
            });

            TaskResultMessage result = engine.executeTask(testTask()).get(2, TimeUnit.SECONDS);

            assertFalse(result.isSuccessful());
            assertEquals(7, result.getAttemptNumber());
            assertEquals("550e8400-e29b-41d4-a716-446655440000", result.getAssignmentId());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void shutdownTerminatesExecutionPool() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");

        engine.shutdown();

        assertTrue(engine.isShutdown());
        assertTrue(engine.awaitTermination(2, TimeUnit.SECONDS));
    }

    @Test
    void duplicateProcessorTaskTypeIsRejected() {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("test", task -> "first");

            IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> engine.registerProcessor(" TEST ", task -> "second"));

            assertTrue(error.getMessage().contains("TEST"));
        } finally {
            engine.shutdown();
        }
    }

    private static TaskAssignMessage testTask() {
        return new TaskAssignMessage(
                "coordinator",
                "2026-06-13T00:00:00Z",
                "task-1",
                "job-1",
                "TEST",
                7,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
                "payload",
                "param"
        );
    }

    private static final class FailingWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            throw new IOException("write failed");
        }

        @Override
        public void flush() throws IOException {
            throw new IOException("flush failed");
        }

        @Override
        public void close() {
        }
    }
}
