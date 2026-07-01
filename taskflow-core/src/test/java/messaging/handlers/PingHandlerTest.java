package messaging.handlers;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import protocol.PingMessage;
import protocol.PongMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PingHandlerTest {
    private final Gson gson = new Gson();

    @Test
    void usesExplicitPeerIdAndAdvertisesCapabilities() {
        PingHandler handler = new PingHandler("peer-explicit", () -> List.of("text_analysis"));
        StringWriter buffer = new StringWriter();

        handler.handle(new PingMessage("coordinator-temp", "2026-06-13T00:00:00Z"), new PrintWriter(buffer, true));

        PongMessage response = gson.fromJson(buffer.toString(), PongMessage.class);
        assertEquals("peer-explicit", response.getNodeId());
        assertEquals(List.of("TEXT_ANALYSIS"), response.getSupportedTaskTypes());
    }

    @Test
    void fallsBackToPingNodeIdWhenNoExplicitPeerIdIsConfigured() {
        PingHandler handler = new PingHandler();
        StringWriter buffer = new StringWriter();

        handler.handle(new PingMessage("coordinator-temp", "2026-06-13T00:00:00Z"), new PrintWriter(buffer, true));

        PongMessage response = gson.fromJson(buffer.toString(), PongMessage.class);
        assertEquals("coordinator-temp", response.getNodeId());
    }

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
