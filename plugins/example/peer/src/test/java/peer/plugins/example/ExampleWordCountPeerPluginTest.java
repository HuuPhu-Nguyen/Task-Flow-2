package peer.plugins.example;

import example.model.ExamplePayload;
import example.model.ExampleTaskResult;
import example.model.ExampleTaskTypes;
import org.junit.jupiter.api.Test;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.ExampleWordCountProcessor;
import plugin.RetrySafety;
import protocol.TaskAssignMessage;

import java.time.Instant;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExampleWordCountPeerPluginTest {
    @Test
    void discoversPeerProcessorPlugin() {
        Set<PeerProcessorPlugin> plugins = StreamSupport.stream(
                        ServiceLoader.load(PeerProcessorPlugin.class).spliterator(),
                        false
                )
                .collect(Collectors.toSet());
        Set<String> taskTypes = plugins.stream()
                .map(PeerProcessorPlugin::taskType)
                .collect(Collectors.toSet());

        assertTrue(taskTypes.contains(ExampleTaskTypes.WORD_COUNT));
        PeerProcessorPlugin plugin = plugins.stream()
                .filter(candidate -> ExampleTaskTypes.WORD_COUNT.equals(candidate.taskType()))
                .findFirst()
                .orElseThrow();
        assertEquals(RetrySafety.PURE, plugin.retrySafety());
    }

    @Test
    void processorCountsWords() throws Exception {
        TaskProcessor<ExampleTaskResult> processor = new ExampleWordCountProcessor();
        TaskAssignMessage assignment = assignment(new ExamplePayload("notes.txt", "Hello, TaskFlow hello."));

        ExampleTaskResult result = processor.process(assignment);

        assertEquals("notes.txt", result.documentName());
        assertEquals(3, result.wordCount());
    }

    @Test
    void processorRejectsMissingText() {
        TaskAssignMessage assignment = assignment(new ExamplePayload("notes.txt", null));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new ExampleWordCountProcessor().process(assignment));

        assertTrue(error.getMessage().contains("requires text"));
    }

    private static TaskAssignMessage assignment(ExamplePayload payload) {
        return new TaskAssignMessage(
                "COORDINATOR",
                Instant.EPOCH.toString(),
                "task-job-example-0",
                "job-example",
                ExampleTaskTypes.WORD_COUNT,
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
                payload,
                "summary"
        );
    }
}
