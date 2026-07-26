package peer.plugins.conversion;

import conversion.model.ConversionTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.ImageConversionProcessor;
import plugin.RetrySafety;
import plugin.TaskResourceProfile;

public class ImageConversionProcessorPlugin implements PeerProcessorPlugin {
    @Override
    public String taskType() {
        return ConversionTaskTypes.IMAGE_CONVERSION;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    public TaskResourceProfile resourceProfile() {
        return TaskResourceProfile.ofCapacityUnits(2);
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new ImageConversionProcessor();
    }
}
