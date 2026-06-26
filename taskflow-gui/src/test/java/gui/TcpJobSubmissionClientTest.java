package gui;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TcpJobSubmissionClientTest {
    @TempDir
    Path tempDir;

    @Test
    void requestJobResultUsesTokenLoadedFromPersistentStore() {
        Path storePath = tempDir.resolve("requester-tokens.properties");
        TcpJobSubmissionClient submitClient = new TcpJobSubmissionClient(
                "CLIENT",
                new FileGuiRequesterTokenStore(storePath)
        );
        StringWriter submitWriter = new StringWriter();
        submitClient.submitJob(
                "job-restart",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                new PrintWriter(submitWriter, true)
        );
        String submittedToken = requesterToken(submitWriter.toString());

        TcpJobSubmissionClient restartedClient = new TcpJobSubmissionClient(
                "CLIENT",
                new FileGuiRequesterTokenStore(storePath)
        );
        StringWriter requestWriter = new StringWriter();
        restartedClient.requestJobResult("job-restart", new PrintWriter(requestWriter, true));

        assertEquals(submittedToken, requesterToken(requestWriter.toString()));
    }

    @Test
    void submitFailureRemovesPersistedToken() {
        Path storePath = tempDir.resolve("requester-tokens.properties");
        TcpJobSubmissionClient client = new TcpJobSubmissionClient(
                "CLIENT",
                new FileGuiRequesterTokenStore(storePath)
        );

        assertThrows(IllegalStateException.class, () -> client.submitJob(
                "job-send-failure",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                new PrintWriter(new FailingWriter(), true)
        ));

        FileGuiRequesterTokenStore reloaded = new FileGuiRequesterTokenStore(storePath);
        assertTrue(reloaded.tokenForJob("job-send-failure").isEmpty());
    }

    private static String requesterToken(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("requesterToken")
                .getAsString();
    }

    private static final class FailingWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            throw new IOException("closed");
        }

        @Override
        public void flush() throws IOException {
            throw new IOException("closed");
        }

        @Override
        public void close() {
        }
    }
}
