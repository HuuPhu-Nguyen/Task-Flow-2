package server.concreteJobs.conversion;

import org.junit.jupiter.api.Test;
import peer.engine.WorkerPlugin;
import server.job.TaskPlugin;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConversionPluginDiscoveryTest {
    @Test
    void discoversCoordinatorJobPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(TaskPlugin.class).spliterator(), false)
                .map(TaskPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of("IMAGE_CONVERSION", "VIDEO_TRANSCODING"), taskTypes);
    }

    @Test
    void discoversWorkerProcessorPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(WorkerPlugin.class).spliterator(), false)
                .map(WorkerPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of("IMAGE_CONVERSION", "VIDEO_TRANSCODING"), taskTypes);
    }
}
