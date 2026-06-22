package peer.concreteJobs.conversion;

import conversion.model.ConversionTaskTypes;
import org.junit.jupiter.api.Test;
import peer.engine.PeerProcessorPlugin;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConversionPeerPluginDiscoveryTest {
    @Test
    void discoversPeerProcessorPlugins() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(PeerProcessorPlugin.class).spliterator(), false)
                .map(PeerProcessorPlugin::taskType)
                .collect(Collectors.toSet());

        assertEquals(Set.of(
                ConversionTaskTypes.IMAGE_CONVERSION,
                ConversionTaskTypes.VIDEO_TRANSCODING
        ), taskTypes);
    }
}
