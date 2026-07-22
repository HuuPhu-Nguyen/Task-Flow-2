package peer.plugins.conversion;

import conversion.model.ConversionTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.engine.TaskProcessor;
import peer.processors.VideoTranscodingProcessor;
import plugin.RetrySafety;

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
    public TaskProcessor<?> createProcessor() {
        return new VideoTranscodingProcessor();
    }
}
