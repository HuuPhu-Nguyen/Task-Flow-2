package messaging;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import protocol.PingMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SafeJsonWriterTest {

    private final Gson gson = new Gson();

    @Test
    void returnsTrueAndWritesJsonWhenWriterSucceeds() {
        StringWriter buffer = new StringWriter();
        PrintWriter out = new PrintWriter(buffer);

        assertTrue(SafeJsonWriter.send(out, gson, new PingMessage("peer-1", "2026-06-13T00:00:00Z")));

        String json = buffer.toString();
        assertTrue(json.contains("\"type\":\"PING\""));
        assertTrue(json.contains("\"nodeId\":\"peer-1\""));
    }

    @Test
    void returnsFalseWhenPrintWriterRecordsIoFailure() {
        PrintWriter out = new PrintWriter(new FailingWriter());

        assertFalse(SafeJsonWriter.send(out, gson, new PingMessage("peer-1", "2026-06-13T00:00:00Z")));
        assertTrue(out.checkError());
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
