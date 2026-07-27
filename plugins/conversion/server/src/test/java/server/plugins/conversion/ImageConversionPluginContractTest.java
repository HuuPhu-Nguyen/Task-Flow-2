package server.plugins.conversion;

import conversion.model.ConversionTaskTypes;
import peer.engine.PeerProcessorPlugin;
import peer.plugins.conversion.ImageConversionProcessorPlugin;
import server.job.TaskPlugin;

class ImageConversionPluginContractTest extends ConversionPluginContractBinding {
    @Override
    protected TaskPlugin taskPlugin() {
        return new ImageConversionTaskPlugin();
    }

    @Override
    protected PeerProcessorPlugin peerPlugin() {
        return new ImageConversionProcessorPlugin();
    }

    @Override
    protected String taskType() {
        return ConversionTaskTypes.IMAGE_CONVERSION;
    }

    @Override
    protected String targetFormat() {
        return "png";
    }

    @Override
    protected String inputExtension() {
        return "png";
    }

    @Override
    protected String inputContentType() {
        return "image/png";
    }

    @Override
    protected String resultContentType() {
        return "image/png";
    }
}
