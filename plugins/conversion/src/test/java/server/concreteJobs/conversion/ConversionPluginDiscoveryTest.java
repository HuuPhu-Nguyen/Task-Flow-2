package server.concreteJobs.conversion;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;
import peer.engine.PeerProcessorPlugin;
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
    void discoversPeerProcessorPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(PeerProcessorPlugin.class).spliterator(), false)
                .map(PeerProcessorPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of("IMAGE_CONVERSION", "VIDEO_TRANSCODING"), taskTypes);
    }

    @Test
    void discoversClientJobPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(ClientJobPlugin.class).spliterator(), false)
                .map(ClientJobPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of("IMAGE_CONVERSION", "VIDEO_TRANSCODING"), taskTypes);
    }
}
