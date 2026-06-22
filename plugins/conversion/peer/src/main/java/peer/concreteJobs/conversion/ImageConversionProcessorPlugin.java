package peer.concreteJobs.conversion;

import conversion.model.ConversionTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.ImageConversionProcessor;

public class ImageConversionProcessorPlugin implements PeerProcessorPlugin {
    @Override
    public String taskType() {
        return ConversionTaskTypes.IMAGE_CONVERSION;
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new ImageConversionProcessor();
    }
}
