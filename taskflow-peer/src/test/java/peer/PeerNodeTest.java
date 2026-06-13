package peer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerNodeTest {

    @Test
    void submitJobWritesJobSubmitMessage() {
        PeerNode node = new PeerNode();
        StringWriter buffer = new StringWriter();
        PrintWriter out = new PrintWriter(buffer);

        String jobId = node.submitJob("TEXT_ANALYSIS", List.of("payload"), "summary", out);

        String json = buffer.toString();
        assertFalse(jobId.isBlank());
        assertTrue(json.contains("\"type\":\"JOB_SUBMIT\""));
        assertTrue(json.contains("\"jobId\":\"" + jobId + "\""));
        assertTrue(json.contains("\"taskType\":\"TEXT_ANALYSIS\""));
    }

    @Test
    void submitJobFailsFastWhenMessageCannotBeWritten() {
        PeerNode node = new PeerNode();
        PrintWriter out = new PrintWriter(new FailingWriter());

        assertThrows(IllegalStateException.class,
                () -> node.submitJob("TEXT_ANALYSIS", List.of("payload"), "summary", out));
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
