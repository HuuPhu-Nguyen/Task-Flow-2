package peer.plugins.conversion;

import conversion.model.ConversionTaskTypes;
import org.junit.jupiter.api.Test;
import peer.engine.PeerProcessorPlugin;
import plugin.RetrySafety;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConversionPeerPluginDiscoveryTest {
    @Test
    void discoversPeerProcessorPlugins() {
        Map<String, RetrySafety> retrySafetyByTaskType = StreamSupport.stream(
                        ServiceLoader.load(PeerProcessorPlugin.class).spliterator(),
                        false
                )
                .collect(Collectors.toMap(PeerProcessorPlugin::taskType, PeerProcessorPlugin::retrySafety));

        assertEquals(Set.of(
                ConversionTaskTypes.IMAGE_CONVERSION,
                ConversionTaskTypes.VIDEO_TRANSCODING
        ), retrySafetyByTaskType.keySet());
        assertEquals(RetrySafety.PURE, retrySafetyByTaskType.get(ConversionTaskTypes.IMAGE_CONVERSION));
        assertEquals(RetrySafety.PURE, retrySafetyByTaskType.get(ConversionTaskTypes.VIDEO_TRANSCODING));
    }
}
