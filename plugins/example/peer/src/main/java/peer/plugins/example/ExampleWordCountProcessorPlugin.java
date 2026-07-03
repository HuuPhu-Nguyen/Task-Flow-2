package peer.plugins.example;

import example.model.ExampleTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.ExampleWordCountProcessor;

public class ExampleWordCountProcessorPlugin implements PeerProcessorPlugin {
    @Override
    public String taskType() {
        return ExampleTaskTypes.WORD_COUNT;
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new ExampleWordCountProcessor();
    }
}
