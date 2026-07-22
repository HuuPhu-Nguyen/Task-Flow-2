package gui;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.RequesterIdentity;

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
    private static final Gson GSON = new Gson();

    @TempDir
    Path tempDir;

    @Test
    void newJobIdIncludesSanitizedPeerIdAndFullUuid() {
        TcpJobSubmissionClient client = new TcpJobSubmissionClient(
                "gui peer/1",
                new FileGuiRequesterTokenStore(tempDir.resolve("requester-tokens.properties"))
        );

        String jobId = client.newJobId();

        assertTrue(jobId.matches(
                "JOB_gui_peer_1_\\d+_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
    }

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
                new TestCoordinatorConnection(new PrintWriter(submitWriter, true))
        );
        String submittedToken = requesterToken(submitWriter.toString());

        TcpJobSubmissionClient restartedClient = new TcpJobSubmissionClient(
                "CLIENT",
                new FileGuiRequesterTokenStore(storePath)
        );
        StringWriter requestWriter = new StringWriter();
        restartedClient.requestJobResult(
                "job-restart",
                new TestCoordinatorConnection(new PrintWriter(requestWriter, true)));

        assertEquals(submittedToken, requesterToken(requestWriter.toString()));
        JobSubmitMessage submit = jobSubmit(submitWriter.toString());
        JobResultRequestMessage request = jobResultRequest(requestWriter.toString());
        assertEquals(submit.getRequesterPublicKey(), request.getRequesterPublicKey());
        assertTrue(RequesterIdentity.verifyJobSubmit(submit));
        assertTrue(RequesterIdentity.verifyJobResultRequest(request));
    }

    @Test
    void uncertainSubmitFailureRetainsPersistedTokenForIdempotentReplay() {
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
                new TestCoordinatorConnection(new PrintWriter(new FailingWriter(), true))
        ));

        FileGuiRequesterTokenStore reloaded = new FileGuiRequesterTokenStore(storePath);
        assertTrue(reloaded.tokenForJob("job-send-failure").isPresent());
    }

    private static String requesterToken(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("requesterToken")
                .getAsString();
    }

    private static JobSubmitMessage jobSubmit(String json) {
        return GSON.fromJson(json, JobSubmitMessage.class);
    }

    private static JobResultRequestMessage jobResultRequest(String json) {
        return GSON.fromJson(json, JobResultRequestMessage.class);
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
