package peer.concreteJobs.text;

import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.TextAnalysisProcessor;
import text.model.TextAnalysisTaskTypes;

public class TextAnalysisProcessorPlugin implements PeerProcessorPlugin {
    @Override
    public String taskType() {
        return TextAnalysisTaskTypes.TEXT_ANALYSIS;
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new TextAnalysisProcessor();
    }
}
