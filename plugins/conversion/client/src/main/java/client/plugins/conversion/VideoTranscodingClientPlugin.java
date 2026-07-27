package client.plugins.conversion;

import client.ClientJobPlugin;
import conversion.model.ConversionTaskTypes;
import objectstore.ObjectStoreProvider;
import objectstore.ObjectStores;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class VideoTranscodingClientPlugin implements ClientJobPlugin {
    private static final List<String> INPUT_EXTENSIONS = List.of(
            ".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv"
    );
    private static final List<String> TARGET_FORMATS = List.of("mp4", "avi", "mkv", "mov", "webm");
    private final ObjectStoreProvider objectStoreProvider;

    public VideoTranscodingClientPlugin() {
        this(ObjectStores::open);
    }

    public VideoTranscodingClientPlugin(ObjectStoreProvider objectStoreProvider) {
        this.objectStoreProvider = Objects.requireNonNull(objectStoreProvider, "objectStoreProvider");
    }

    @Override
    public String taskType() {
        return ConversionTaskTypes.VIDEO_TRANSCODING;
    }

    @Override
    public String displayName() {
        return "Video Transcoding";
    }

    @Override
    public List<String> supportedInputExtensions() {
        return INPUT_EXTENSIONS;
    }

    @Override
    public List<String> parameterOptions() {
        return TARGET_FORMATS;
    }

    @Override
    public String defaultParameter() {
        return "mp4";
    }

    @Override
    public List<Object> buildPayloads(List<Path> inputPaths, String parameter) throws Exception {
        normalizeParameter(parameter);
        return ConversionClientPayloads.buildFilePayloads(
                inputPaths,
                INPUT_EXTENSIONS,
                objectStoreProvider
        );
    }

    @Override
    public void saveResults(List<Object> results, Path outputDir) throws Exception {
        ConversionClientPayloads.saveFilePayloadResults(results, outputDir, objectStoreProvider);
    }
}
