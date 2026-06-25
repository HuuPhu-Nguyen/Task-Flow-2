package gui;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GuiJobSubmitterTest {
    private final ClientJobPlugin plugin = new FakeClientJobPlugin();
    private final JobSubmissionClient jobSubmissionClient = new TcpJobSubmissionClient("CLIENT");

    @Test
    void successfulSubmitTracksActiveJobImmediately() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer, true);

        GuiJobSubmitter.SubmittedJob submittedJob = GuiJobSubmitter.submitPreparedPayloads(
                jobSubmissionClient,
                plugin,
                List.of("payload"),
                "summary",
                out,
                () -> true,
                () -> {
                    throw new AssertionError("send failure callback should not run");
                },
                activeJobs);

        assertNotNull(submittedJob.jobId());
        assertSame(plugin, submittedJob.plugin());
        assertTrue(submittedJob.activeAfterSend());
        assertTrue(activeJobs.contains(submittedJob.jobId()));
        assertTrue(writer.toString().contains("TEXT_ANALYSIS"));
    }

    @Test
    void resultArrivingDuringSendRoutesBecauseJobIsAlreadyTracked() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        AtomicReference<GuiJobResultRouter.Action> routedAction = new AtomicReference<>();
        JobSubmissionClient immediateResultClient = new JobSubmissionClient() {
            @Override
            public String newJobId() {
                return "job-fast-failure";
            }

            @Override
            public void submitJob(String jobId,
                                  String taskType,
                                  List<?> payloads,
                                  String parameter,
                                  PrintWriter out) {
                routedAction.set(GuiJobResultRouter.route(
                        result(jobId, false, "failed immediately"),
                        activeJobs).action());
            }

            @Override
            public void requestJobResult(String jobId, PrintWriter out) {
                throw new AssertionError("result request should not be used during submit");
            }
        };

        GuiJobSubmitter.SubmittedJob submittedJob = GuiJobSubmitter.submitPreparedPayloads(
                immediateResultClient,
                plugin,
                List.of("payload"),
                "summary",
                new PrintWriter(new StringWriter(), true),
                () -> true,
                () -> {
                    throw new AssertionError("send failure callback should not run");
                },
                activeJobs);

        assertEquals("job-fast-failure", submittedJob.jobId());
        assertFalse(submittedJob.activeAfterSend());
        assertEquals(GuiJobResultRouter.Action.SHOW_FAILURE, routedAction.get());
        assertFalse(activeJobs.contains("job-fast-failure"));
    }

    @Test
    void changedConnectionPreventsSubmitWithoutTrackingJob() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        StringWriter writer = new StringWriter();
        AtomicBoolean sendFailureCallback = new AtomicBoolean(false);

        assertThrows(IllegalStateException.class, () -> GuiJobSubmitter.submitPreparedPayloads(
                jobSubmissionClient,
                plugin,
                List.of("payload"),
                "summary",
                new PrintWriter(writer, true),
                () -> false,
                () -> sendFailureCallback.set(true),
                activeJobs));

        assertTrue(writer.toString().isEmpty());
        assertTrue(activeJobs.isEmpty());
        assertFalse(sendFailureCallback.get());
    }

    @Test
    void connectionChangeAfterTrackingRemovesReservedJobWithoutSending() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        StringWriter writer = new StringWriter();
        AtomicBoolean sendFailureCallback = new AtomicBoolean(false);
        AtomicInteger connectionChecks = new AtomicInteger(0);

        assertThrows(IllegalStateException.class, () -> GuiJobSubmitter.submitPreparedPayloads(
                jobSubmissionClient,
                plugin,
                List.of("payload"),
                "summary",
                new PrintWriter(writer, true),
                () -> connectionChecks.incrementAndGet() == 1,
                () -> sendFailureCallback.set(true),
                activeJobs));

        assertTrue(writer.toString().isEmpty());
        assertTrue(activeJobs.isEmpty());
        assertFalse(sendFailureCallback.get());
    }

    @Test
    void sendFailureRunsCleanupCallbackWithoutTrackingJob() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        AtomicBoolean sendFailureCallback = new AtomicBoolean(false);

        assertThrows(IllegalStateException.class, () -> GuiJobSubmitter.submitPreparedPayloads(
                jobSubmissionClient,
                plugin,
                List.of("payload"),
                "summary",
                new PrintWriter(new FailingWriter(), true),
                () -> true,
                () -> sendFailureCallback.set(true),
                activeJobs));

        assertTrue(activeJobs.isEmpty());
        assertTrue(sendFailureCallback.get());
    }

    @Test
    void tcpClientWritesJobResultRequest() {
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer, true);

        jobSubmissionClient.requestJobResult("job-completed", out);

        String json = writer.toString();
        assertTrue(json.contains("\"type\":\"JOB_RESULT_REQUEST\""));
        assertTrue(json.contains("\"jobId\":\"job-completed\""));
    }

    private static JobResultMessage result(String jobId, boolean successful, String errorMessage) {
        return new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                jobId,
                "TEXT_ANALYSIS",
                successful,
                List.of(),
                errorMessage);
    }

    private static final class FakeClientJobPlugin implements ClientJobPlugin {
        @Override
        public String taskType() {
            return "TEXT_ANALYSIS";
        }

        @Override
        public String displayName() {
            return "Text Analysis";
        }

        @Override
        public List<String> supportedInputExtensions() {
            return List.of("txt");
        }

        @Override
        public List<String> parameterOptions() {
            return List.of("summary");
        }

        @Override
        public String defaultParameter() {
            return "summary";
        }

        @Override
        public List<Object> buildPayloads(List<Path> inputPaths, String parameter) {
            return List.copyOf(inputPaths);
        }

        @Override
        public void saveResults(List<Object> results, Path outputDir) {
        }
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
