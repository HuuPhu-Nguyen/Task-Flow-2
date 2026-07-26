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
        Map<String, PeerProcessorPlugin> pluginsByTaskType = StreamSupport.stream(
                        ServiceLoader.load(PeerProcessorPlugin.class).spliterator(),
                        false
                )
                .collect(Collectors.toMap(
                        PeerProcessorPlugin::taskType,
                        plugin -> plugin
                ));

        assertEquals(Set.of(
                ConversionTaskTypes.IMAGE_CONVERSION,
                ConversionTaskTypes.VIDEO_TRANSCODING
        ), pluginsByTaskType.keySet());
        assertEquals(
                RetrySafety.PURE,
                pluginsByTaskType.get(ConversionTaskTypes.IMAGE_CONVERSION).retrySafety()
        );
        assertEquals(
                RetrySafety.PURE,
                pluginsByTaskType.get(ConversionTaskTypes.VIDEO_TRANSCODING).retrySafety()
        );
        assertEquals(
                2,
                pluginsByTaskType.get(ConversionTaskTypes.IMAGE_CONVERSION)
                        .resourceProfile()
                        .capacityUnitCost()
        );
        assertEquals(
                8,
                pluginsByTaskType.get(ConversionTaskTypes.VIDEO_TRANSCODING)
                        .resourceProfile()
                        .capacityUnitCost()
        );
    }
}
