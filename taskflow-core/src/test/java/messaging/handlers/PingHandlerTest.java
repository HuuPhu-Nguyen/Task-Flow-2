package messaging.handlers;

import org.junit.jupiter.api.Test;
import protocol.PingMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PingHandlerTest {

    @Test
    void throwsWhenPongResponseCannotBeWritten() {
        PingHandler handler = new PingHandler();
        PrintWriter out = new PrintWriter(new FailingWriter());

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> handler.handle(new PingMessage("peer-1", "2026-06-13T00:00:00Z"), out)
        );

        assertEquals("Could not send PONG response.", error.getMessage());
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
