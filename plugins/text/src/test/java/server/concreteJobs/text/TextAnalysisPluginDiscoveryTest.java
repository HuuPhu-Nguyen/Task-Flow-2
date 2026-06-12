package server.concreteJobs.text;

import client.ClientJobPlugin;
import org.junit.jupiter.api.Test;
import peer.engine.PeerProcessorPlugin;
import server.job.TaskPlugin;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TextAnalysisPluginDiscoveryTest {
    @Test
    void discoversTaskPlugin() {
        assertTrue(discoverTaskTypes(TaskPlugin.class).contains(TextAnalysisTaskPlugin.TYPE));
    }

    @Test
    void discoversPeerProcessorPlugin() {
        assertTrue(discoverTaskTypes(PeerProcessorPlugin.class).contains(TextAnalysisTaskPlugin.TYPE));
    }

    @Test
    void discoversClientJobPlugin() {
        assertTrue(discoverTaskTypes(ClientJobPlugin.class).contains(TextAnalysisTaskPlugin.TYPE));
    }

    private static <T> Set<String> discoverTaskTypes(Class<T> type) {
        return StreamSupport.stream(ServiceLoader.load(type).spliterator(), false)
                .map(plugin -> {
                    if (plugin instanceof TaskPlugin taskPlugin) {
                        return taskPlugin.taskType();
                    }
                    if (plugin instanceof PeerProcessorPlugin peerPlugin) {
                        return peerPlugin.taskType();
                    }
                    return ((ClientJobPlugin) plugin).taskType();
                })
                .collect(Collectors.toSet());
    }
}
