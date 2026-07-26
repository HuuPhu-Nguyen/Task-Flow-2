package peer.plugins.conversion;

import conversion.model.ConversionTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.VideoTranscodingProcessor;
import plugin.RetrySafety;
import plugin.TaskResourceProfile;

public class VideoTranscodingProcessorPlugin implements PeerProcessorPlugin {
    @Override
    public String taskType() {
        return ConversionTaskTypes.VIDEO_TRANSCODING;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    public TaskResourceProfile resourceProfile() {
        return TaskResourceProfile.ofCapacityUnits(8);
    }

    @Override
    public TaskProcessor<?> createProcessor() {
        return new VideoTranscodingProcessor();
    }
}
