package gui;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class GuiJobResultRouterTest {
    @Test
    void activeFailedResultRoutesToFailureAndClearsActiveJob() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        activeJobs.add("job-1");
        JobResultMessage result = result("job-1", false, "processor failed");

        GuiJobResultRouter.RoutedJobResult routed = GuiJobResultRouter.route(result, activeJobs);

        assertEquals(GuiJobResultRouter.Action.SHOW_FAILURE, routed.action());
        assertEquals(result, routed.result());
        assertFalse(activeJobs.contains("job-1"));
    }

    @Test
    void activeSuccessfulResultRoutesToDownloadAndClearsActiveJob() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        activeJobs.add("job-1");
        JobResultMessage result = result("job-1", true, null);

        GuiJobResultRouter.RoutedJobResult routed = GuiJobResultRouter.route(result, activeJobs);

        assertEquals(GuiJobResultRouter.Action.SHOW_DOWNLOAD, routed.action());
        assertEquals(result, routed.result());
        assertFalse(activeJobs.contains("job-1"));
    }

    @Test
    void resultForForeignJobIsIgnored() {
        Set<String> activeJobs = ConcurrentHashMap.newKeySet();
        activeJobs.add("job-1");

        GuiJobResultRouter.RoutedJobResult routed = GuiJobResultRouter.route(
                result("other-job", false, "failed"),
                activeJobs);

        assertEquals(GuiJobResultRouter.Action.IGNORE, routed.action());
        assertEquals(Set.of("job-1"), activeJobs);
    }

    @Test
    void blankFailureMessageUsesDefaultDisplayText() {
        assertEquals(
                GuiJobResultRouter.DEFAULT_FAILURE_MESSAGE,
                GuiJobResultRouter.failureMessage(result("job-1", false, " ")));
    }

    @Test
    void explicitFailureMessageIsDisplayed() {
        assertEquals(
                "unsupported task type",
                GuiJobResultRouter.failureMessage(result("job-1", false, "unsupported task type")));
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
}
