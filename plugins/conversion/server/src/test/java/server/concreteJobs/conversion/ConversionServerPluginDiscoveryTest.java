package server.concreteJobs.conversion;

import conversion.model.ConversionTaskTypes;
import org.junit.jupiter.api.Test;
import server.job.TaskPlugin;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConversionServerPluginDiscoveryTest {
    @Test
    void discoversCoordinatorJobPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(TaskPlugin.class).spliterator(), false)
                .map(TaskPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of(
                ConversionTaskTypes.IMAGE_CONVERSION,
                ConversionTaskTypes.VIDEO_TRANSCODING
        ), taskTypes);
    }
}
