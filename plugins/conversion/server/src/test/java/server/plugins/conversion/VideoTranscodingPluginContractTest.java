package server.plugins.conversion;

import conversion.model.ConversionTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.plugins.conversion.VideoTranscodingProcessorPlugin;
import server.job.TaskPlugin;

class VideoTranscodingPluginContractTest extends ConversionPluginContractBinding {
    @Override
    protected TaskPlugin taskPlugin() {
        return new VideoTranscodingTaskPlugin();
    }

    @Override
    protected PeerProcessorPlugin peerPlugin() {
        return new VideoTranscodingProcessorPlugin();
    }

    @Override
    protected String taskType() {
        return ConversionTaskTypes.VIDEO_TRANSCODING;
    }

    @Override
    protected String targetFormat() {
        return "mp4";
    }

    @Override
    protected String inputExtension() {
        return "mp4";
    }

    @Override
    protected String inputContentType() {
        return "video/mp4";
    }

    @Override
    protected String resultContentType() {
        return "video/mp4";
    }

    @Override
    protected String submissionParameter() {
        return " MP4 ";
    }
}
