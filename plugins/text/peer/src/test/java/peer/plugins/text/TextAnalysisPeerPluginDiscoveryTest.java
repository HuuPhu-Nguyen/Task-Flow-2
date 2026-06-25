package peer.plugins.text;

import org.junit.jupiter.api.Test;
import peer.engine.PeerProcessorPlugin;
import peer.processors.TextAnalysisProcessor;
import protocol.TaskAssignMessage;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;
import text.model.TextAnalysisTaskTypes;

import java.time.Instant;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextAnalysisPeerPluginDiscoveryTest {
    @Test
    void discoversPeerProcessorPlugin() {
        Set<String> taskTypes = StreamSupport.stream(ServiceLoader.load(PeerProcessorPlugin.class).spliterator(), false)
                .map(PeerProcessorPlugin::taskType)
                .collect(Collectors.toSet());

        assertTrue(taskTypes.contains(TextAnalysisTaskTypes.TEXT_ANALYSIS));
    }

    @Test
    void processorCountsLinesWordsAndUniqueWords() {
        TaskAssignMessage assignment = new TaskAssignMessage(
                "peer-1",
                Instant.now().toString(),
                "task-1",
                "job-1",
                TextAnalysisTaskTypes.TEXT_ANALYSIS,
                new TextAnalysisPayload("notes.txt", "Hello hello\nTaskFlow"),
                "csv"
        );

        TextAnalysisResult result = new TextAnalysisProcessor().process(assignment);

        assertEquals(2, result.lineCount());
        assertEquals(3, result.wordCount());
        assertEquals(2, result.uniqueWordCount());
    }
}
