package peer.plugins.example;

import example.model.ExampleTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.ExampleWordCountProcessor;
import plugin.RetrySafety;

public class ExampleWordCountProcessorPlugin implements PeerProcessorPlugin {
    @Override
    public String taskType() {
        return ExampleTaskTypes.WORD_COUNT;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new ExampleWordCountProcessor();
    }
}
