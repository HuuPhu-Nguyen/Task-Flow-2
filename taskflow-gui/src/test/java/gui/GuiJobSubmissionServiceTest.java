package gui;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GuiJobSubmissionServiceTest {
    @Test
    void buildsPayloadsAndSubmitsPreparedJob() throws Exception {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        RecordingSubmissionClient submissionClient = new RecordingSubmissionClient();
        RecordingPlugin plugin = new RecordingPlugin();
        AtomicInteger beforeSubmitCalls = new AtomicInteger();
        GuiJobSubmissionService service = new GuiJobSubmissionService(submissionClient, activeJobs);

        GuiJobSubmitter.SubmittedJob submittedJob = service.submit(
                plugin,
                List.of(Path.of("note.txt")),
                "summary",
                new TestCoordinatorConnection(),
                () -> true,
                () -> {
                    throw new AssertionError("send failure callback should not run");
                },
                () -> false,
                beforeSubmitCalls::incrementAndGet);

        assertEquals("job-1", submittedJob.jobId());
        assertSame(plugin, submittedJob.plugin());
        assertTrue(submittedJob.activeAfterSend());
        assertTrue(activeJobs.contains("job-1"));
        assertEquals(List.of(Path.of("note.txt")), plugin.inputPaths);
        assertEquals("summary", plugin.parameter);
        assertEquals(List.of("payload:note.txt:summary"), submissionClient.payloads);
        assertEquals("TEXT_ANALYSIS", submissionClient.taskType);
        assertEquals("summary", submissionClient.parameter);
        assertEquals(1, beforeSubmitCalls.get());
    }

    @Test
    void cancelledAfterPayloadBuildSkipsSubmit() throws Exception {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        RecordingSubmissionClient submissionClient = new RecordingSubmissionClient();
        RecordingPlugin plugin = new RecordingPlugin();
        AtomicInteger beforeSubmitCalls = new AtomicInteger();
        GuiJobSubmissionService service = new GuiJobSubmissionService(submissionClient, activeJobs);

        GuiJobSubmitter.SubmittedJob submittedJob = service.submit(
                plugin,
                List.of(Path.of("note.txt")),
                "summary",
                new TestCoordinatorConnection(),
                () -> true,
                () -> {
                    throw new AssertionError("send failure callback should not run");
                },
                () -> true,
                beforeSubmitCalls::incrementAndGet);

        assertNull(submittedJob);
        assertEquals(1, plugin.buildPayloadCalls);
        assertEquals(0, submissionClient.submitCalls);
        assertTrue(activeJobs.isEmpty());
        assertEquals(0, beforeSubmitCalls.get());
    }

    @Test
    void payloadBuildFailureDoesNotReserveJob() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        RecordingSubmissionClient submissionClient = new RecordingSubmissionClient();
        RecordingPlugin plugin = new RecordingPlugin();
        plugin.buildFailure = new IllegalStateException("bad input");
        AtomicInteger beforeSubmitCalls = new AtomicInteger();
        GuiJobSubmissionService service = new GuiJobSubmissionService(submissionClient, activeJobs);

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> service.submit(
                plugin,
                List.of(Path.of("note.txt")),
                "summary",
                new TestCoordinatorConnection(),
                () -> true,
                () -> {
                    throw new AssertionError("send failure callback should not run");
                },
                () -> false,
                beforeSubmitCalls::incrementAndGet));

        assertEquals("bad input", error.getMessage());
        assertEquals(0, submissionClient.submitCalls);
        assertTrue(activeJobs.isEmpty());
        assertEquals(0, beforeSubmitCalls.get());
    }

    private static final class RecordingSubmissionClient implements JobSubmissionClient {
        private String taskType;
        private String parameter;
        private List<?> payloads = List.of();
        private int submitCalls;

        @Override
        public String newJobId() {
            return "job-1";
        }

        @Override
        public void submitJob(String jobId,
                              String taskType,
                              List<?> payloads,
                              String parameter,
                              CoordinatorConnection connection) {
            this.submitCalls++;
            this.taskType = taskType;
            this.payloads = List.copyOf(payloads);
            this.parameter = parameter;
        }

    }

    private static final class RecordingPlugin implements ClientJobPlugin {
        private List<Path> inputPaths = List.of();
        private String parameter;
        private int buildPayloadCalls;
        private RuntimeException buildFailure;

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
            this.buildPayloadCalls++;
            if (buildFailure != null) {
                throw buildFailure;
            }
            this.inputPaths = List.copyOf(inputPaths);
            this.parameter = parameter;
            return inputPaths.stream()
                    .map(path -> "payload:" + path.getFileName() + ":" + parameter)
                    .map(Object.class::cast)
                    .toList();
        }

        @Override
        public void saveResults(List<Object> results, Path outputDir) {
        }
    }
}
