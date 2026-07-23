package gui;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;

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

    @Test
    void successfulSubmitTracksActiveJobImmediately() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        RecordingSubmissionClient client = new RecordingSubmissionClient("job-success");

        GuiJobSubmitter.SubmittedJob submittedJob = GuiJobSubmitter.submitPreparedPayloads(
                client,
                plugin,
                List.of("payload"),
                "summary",
                new TestCoordinatorConnection(),
                () -> true,
                () -> {
                    throw new AssertionError("send failure callback should not run");
                },
                activeJobs);

        assertNotNull(submittedJob.jobId());
        assertSame(plugin, submittedJob.plugin());
        assertTrue(submittedJob.activeAfterSend());
        assertTrue(activeJobs.contains(submittedJob.jobId()));
        assertEquals(1, client.submitCalls);
        assertEquals("TEXT_ANALYSIS", client.taskType);
        assertEquals(List.of("payload"), client.payloads);
        assertEquals("summary", client.parameter);
    }

    @Test
    void resultArrivingDuringSendRoutesBecauseJobIsAlreadyTracked() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        AtomicReference<GuiJobResultRouter.Action> routedAction = new AtomicReference<>();
        RecordingSubmissionClient immediateResultClient =
                new RecordingSubmissionClient("job-fast-failure");
        immediateResultClient.onSubmit = () -> routedAction.set(GuiJobResultRouter.route(
                result("job-fast-failure", false, "failed immediately"),
                activeJobs).action());

        GuiJobSubmitter.SubmittedJob submittedJob = GuiJobSubmitter.submitPreparedPayloads(
                immediateResultClient,
                plugin,
                List.of("payload"),
                "summary",
                new TestCoordinatorConnection(),
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
        RecordingSubmissionClient client = new RecordingSubmissionClient("job-not-sent");
        AtomicBoolean sendFailureCallback = new AtomicBoolean(false);

        assertThrows(IllegalStateException.class, () -> GuiJobSubmitter.submitPreparedPayloads(
                client,
                plugin,
                List.of("payload"),
                "summary",
                new TestCoordinatorConnection(),
                () -> false,
                () -> sendFailureCallback.set(true),
                activeJobs));

        assertEquals(0, client.submitCalls);
        assertTrue(activeJobs.isEmpty());
        assertFalse(sendFailureCallback.get());
    }

    @Test
    void connectionChangeAfterTrackingRemovesReservedJobWithoutSending() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        RecordingSubmissionClient client = new RecordingSubmissionClient("job-connection-changed");
        AtomicBoolean sendFailureCallback = new AtomicBoolean(false);
        AtomicInteger connectionChecks = new AtomicInteger(0);

        assertThrows(IllegalStateException.class, () -> GuiJobSubmitter.submitPreparedPayloads(
                client,
                plugin,
                List.of("payload"),
                "summary",
                new TestCoordinatorConnection(),
                () -> connectionChecks.incrementAndGet() == 1,
                () -> sendFailureCallback.set(true),
                activeJobs));

        assertEquals(0, client.submitCalls);
        assertTrue(activeJobs.isEmpty());
        assertFalse(sendFailureCallback.get());
    }

    @Test
    void sendFailureRunsCleanupCallbackWithoutTrackingJob() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        AtomicBoolean sendFailureCallback = new AtomicBoolean(false);
        RecordingSubmissionClient client = new RecordingSubmissionClient("job-publish-failed");
        client.submitFailure = new IllegalStateException("broker publish failed");

        assertThrows(IllegalStateException.class, () -> GuiJobSubmitter.submitPreparedPayloads(
                client,
                plugin,
                List.of("payload"),
                "summary",
                new TestCoordinatorConnection(),
                () -> true,
                () -> sendFailureCallback.set(true),
                activeJobs));

        assertEquals(1, client.submitCalls);
        assertTrue(activeJobs.isEmpty());
        assertTrue(sendFailureCallback.get());
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

    private static final class RecordingSubmissionClient implements JobSubmissionClient {
        private final String jobId;
        private int submitCalls;
        private String taskType;
        private List<?> payloads = List.of();
        private String parameter;
        private Runnable onSubmit = () -> {
        };
        private RuntimeException submitFailure;

        private RecordingSubmissionClient(String jobId) {
            this.jobId = jobId;
        }

        @Override
        public String newJobId() {
            return jobId;
        }

        @Override
        public void submitJob(String jobId,
                              String taskType,
                              List<?> payloads,
                              String parameter,
                              CoordinatorConnection connection) {
            submitCalls++;
            this.taskType = taskType;
            this.payloads = List.copyOf(payloads);
            this.parameter = parameter;
            onSubmit.run();
            if (submitFailure != null) {
                throw submitFailure;
            }
        }
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
}
