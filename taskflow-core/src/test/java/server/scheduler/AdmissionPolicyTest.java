package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.AdmissionRejection;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdmissionPolicyTest {

    @Test
    void activeJobBoundaryAllowsExactMaximumAndRejectsNextJob() {
        SchedulerConfig config = boundedConfig();

        assertEquals(
                AdmissionPolicy.Outcome.ALLOWED,
                AdmissionPolicy.evaluate(1L, 0L, 1L, 0L, true, config).outcome()
        );
        AdmissionPolicy.Decision rejected = AdmissionPolicy.evaluate(
                2L,
                0L,
                1L,
                0L,
                true,
                config
        );

        assertEquals(AdmissionPolicy.Outcome.LIMIT_EXCEEDED, rejected.outcome());
        assertEquals(AdmissionRejection.Limit.MAX_ACTIVE_JOBS, rejected.rejection().limit());
        assertEquals(2L, rejected.rejection().configuredMaximum());
        assertEquals(3L, rejected.rejection().observedValue());
    }

    @Test
    void activeTaskBoundaryAllowsExactMaximumAndRejectsNextTask() {
        SchedulerConfig config = boundedConfig();

        assertEquals(
                AdmissionPolicy.Outcome.ALLOWED,
                AdmissionPolicy.evaluate(0L, 2L, 1L, 0L, true, config).outcome()
        );
        AdmissionPolicy.Decision rejected = AdmissionPolicy.evaluate(
                0L,
                2L,
                2L,
                0L,
                true,
                config
        );

        assertEquals(AdmissionPolicy.Outcome.LIMIT_EXCEEDED, rejected.outcome());
        assertEquals(AdmissionRejection.Limit.MAX_ACTIVE_TASKS, rejected.rejection().limit());
        assertEquals(3L, rejected.rejection().configuredMaximum());
        assertEquals(4L, rejected.rejection().observedValue());
    }

    @Test
    void pendingOutboxBoundaryRejectsAtConfiguredThreshold() {
        SchedulerConfig config = boundedConfig();

        assertEquals(
                AdmissionPolicy.Outcome.ALLOWED,
                AdmissionPolicy.evaluate(0L, 0L, 1L, 4L, true, config).outcome()
        );
        AdmissionPolicy.Decision rejected = AdmissionPolicy.evaluate(
                0L,
                0L,
                1L,
                5L,
                true,
                config
        );

        assertEquals(AdmissionPolicy.Outcome.LIMIT_EXCEEDED, rejected.outcome());
        assertEquals(
                AdmissionRejection.Limit.MAX_PENDING_OUTBOX_ROWS,
                rejected.rejection().limit()
        );
        assertEquals(5L, rejected.rejection().configuredMaximum());
        assertEquals(5L, rejected.rejection().observedValue());
    }

    @Test
    void pendingOutboxReadFailureFailsClosed() {
        AdmissionPolicy.Decision decision = AdmissionPolicy.evaluate(
                0L,
                0L,
                1L,
                0L,
                false,
                boundedConfig()
        );

        assertEquals(AdmissionPolicy.Outcome.STORAGE_FAILURE, decision.outcome());
    }

    @Test
    void admissionResumesAfterAcceptedStateCleanupReturnsProjectionBelowLimit() {
        SchedulerConfig lowered = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "1",
                "TASKFLOW_MAX_ACTIVE_TASKS", "1"
        ));
        SchedulerState state = new SchedulerState(lowered);
        state.addActiveJob(testJob("job-recovered-first", List.of("a")), "", "");
        state.addActiveJob(testJob("job-recovered-second", List.of("b")), "", "");

        assertEquals(
                AdmissionPolicy.Outcome.LIMIT_EXCEEDED,
                evaluateState(state, lowered).outcome()
        );
        state.removeJob("job-recovered-first");
        assertEquals(
                AdmissionPolicy.Outcome.LIMIT_EXCEEDED,
                evaluateState(state, lowered).outcome()
        );
        state.removeJob("job-recovered-second");
        assertEquals(
                AdmissionPolicy.Outcome.ALLOWED,
                evaluateState(state, lowered).outcome()
        );
    }

    @Test
    void activeTaskProjectionCountsWholeJobsExactly() {
        SchedulerState state = new SchedulerState(SchedulerConfig.defaults());
        EmbarrassinglyParallelJob<?, ?> first = testJob("job-first", List.of("a", "b"));
        EmbarrassinglyParallelJob<?, ?> replacement = testJob(
                "job-first",
                List.of("a", "b", "c")
        );
        EmbarrassinglyParallelJob<?, ?> second = testJob("job-second", List.of("d"));

        state.addActiveJob(first, "", "");
        assertEquals(1, state.activeJobCount());
        assertEquals(2L, state.activeTaskCount());

        state.addActiveJob(replacement, "", "");
        assertEquals(1, state.activeJobCount());
        assertEquals(3L, state.activeTaskCount());

        state.addActiveJob(second, "", "");
        assertEquals(2, state.activeJobCount());
        assertEquals(4L, state.activeTaskCount());

        state.removeJob("job-first");
        assertEquals(1, state.activeJobCount());
        assertEquals(1L, state.activeTaskCount());
        state.removeJob("job-first");
        assertEquals(1L, state.activeTaskCount());
    }

    private static SchedulerConfig boundedConfig() {
        return SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "2",
                "TASKFLOW_MAX_ACTIVE_TASKS", "3",
                "TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "5"
        ));
    }

    private static AdmissionPolicy.Decision evaluateState(
            SchedulerState state,
            SchedulerConfig config) {
        return AdmissionPolicy.evaluate(
                state.activeJobCount(),
                state.activeTaskCount(),
                1L,
                0L,
                true,
                config
        );
    }

    private static EmbarrassinglyParallelJob<?, ?> testJob(
            String jobId,
            List<Object> payloads) {
        JobSubmitMessage submit = new JobSubmitMessage(
                "requester-1",
                "2026-07-26T00:00:00Z",
                jobId,
                TestTaskPlugin.TASK_TYPE,
                payloads,
                "",
                "token-" + jobId
        );
        EmbarrassinglyParallelJob<?, ?> job = JobFactory.create(submit, "requester-1");
        job.initializeTasks(submit);
        return job;
    }
}
