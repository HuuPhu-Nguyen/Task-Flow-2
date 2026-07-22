package server.job;

import org.junit.jupiter.api.Test;
import plugin.RetrySafety;
import protocol.JobSubmitMessage;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobFactoryTest {

    @Test
    void loadPluginsNormalizesTaskTypes() {
        StubPlugin plugin = new StubPlugin(" test_task ");

        Map<String, TaskPlugin> plugins = JobFactory.loadPlugins(List.of(plugin));

        assertSame(plugin, plugins.get("TEST_TASK"));
    }

    @Test
    void loadPluginsRejectsDuplicateNormalizedTaskTypes() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                JobFactory.loadPlugins(List.of(
                        new StubPlugin("test_task"),
                        new StubPlugin(" TEST_TASK ")
                )));

        assertTrue(error.getMessage().contains("TEST_TASK"));
    }

    @Test
    void loadPluginsRejectsMissingRetrySafetyDeclaration() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                JobFactory.loadPlugins(List.of(new MissingRetrySafetyPlugin())));

        assertTrue(error.getMessage().contains("must declare retry safety"));
    }

    @Test
    void retrySafetyLookupNormalizesTaskType() {
        StubPlugin plugin = new StubPlugin("TEST_TASK");

        assertSame(
                RetrySafety.PURE,
                JobFactory.retrySafety(" test_task ", Map.of("TEST_TASK", plugin))
        );
    }

    @Test
    void createRunsPluginValidationBeforeCreatingJob() {
        ValidatingPlugin plugin = new ValidatingPlugin();
        JobSubmitMessage submit = new JobSubmitMessage(
                "client-1",
                "2026-06-25T00:00:00Z",
                "job-1",
                "VALIDATED_TASK",
                List.of("payload"),
                "bad"
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                JobFactory.create(submit, "client-1", Map.of("VALIDATED_TASK", plugin)));

        assertTrue(error.getMessage().contains("Rejected by validating plugin"));
    }

    private record StubPlugin(String taskType) implements TaskPlugin {
        @Override
        public RetrySafety retrySafety() {
            return RetrySafety.PURE;
        }

        @Override
        public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
            throw new UnsupportedOperationException("Test plugin does not create jobs.");
        }
    }

    private static final class ValidatingPlugin implements TaskPlugin {
        @Override
        public String taskType() {
            return "VALIDATED_TASK";
        }

        @Override
        public RetrySafety retrySafety() {
            return RetrySafety.IDEMPOTENT;
        }

        @Override
        public void validateSubmission(JobSubmitMessage message) {
            throw new IllegalArgumentException("Rejected by validating plugin.");
        }

        @Override
        public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
            throw new AssertionError("Invalid submissions should not create jobs.");
        }
    }

    private static final class MissingRetrySafetyPlugin implements TaskPlugin {
        @Override
        public String taskType() {
            return "MISSING_RETRY_SAFETY";
        }

        @Override
        public RetrySafety retrySafety() {
            return null;
        }

        @Override
        public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
            throw new UnsupportedOperationException();
        }
    }
}
