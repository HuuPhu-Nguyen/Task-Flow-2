package client.plugins.conversion;

import client.ClientJobPlugin;
import conversion.model.ConversionTaskTypes;

import java.nio.file.Path;
import java.util.List;

public class ImageConversionClientPlugin implements ClientJobPlugin {
    private static final List<String> INPUT_EXTENSIONS = List.of(
            ".png", ".jpg", ".jpeg", ".bmp", ".gif", ".pdf"
    );
    private static final List<String> TARGET_FORMATS = List.of("png", "jpg", "bmp", "gif");

    @Override
    public String taskType() {
        return ConversionTaskTypes.IMAGE_CONVERSION;
    }

    @Override
    public String displayName() {
        return "Image Conversion";
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
        return "png";
    }

    @Override
    public List<Object> buildPayloads(List<Path> inputPaths, String parameter) throws Exception {
        normalizeParameter(parameter);
        return ConversionClientPayloads.buildFilePayloads(inputPaths, INPUT_EXTENSIONS);
    }

    @Override
    public void saveResults(List<Object> results, Path outputDir) throws Exception {
        ConversionClientPayloads.saveFilePayloadResults(results, outputDir);
    }
}
