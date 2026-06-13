package peer.engine;

import org.junit.jupiter.api.Test;
import protocol.TaskAssignMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
            assertTrue(buffer.toString().contains("\"type\":\"TASK_RESULT\""));
            assertTrue(buffer.toString().contains("\"taskId\":\"task-1\""));
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

    private static TaskAssignMessage testTask() {
        return new TaskAssignMessage(
                "coordinator",
                "2026-06-13T00:00:00Z",
                "task-1",
                "job-1",
                "TEST",
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
