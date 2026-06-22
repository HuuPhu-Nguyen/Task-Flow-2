package server.job;

import org.junit.jupiter.api.Test;
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

    private record StubPlugin(String taskType) implements TaskPlugin {
        @Override
        public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
            throw new UnsupportedOperationException("Test plugin does not create jobs.");
        }
    }
}
