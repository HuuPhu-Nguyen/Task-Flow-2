package client;

import java.nio.file.Path;
import java.util.List;

/**
 * Client-side adapter for a job type.
 *
 * <p>The coordinator and peer execution engine stay payload-agnostic; this SPI
 * owns the client-specific details of turning local inputs into task payloads
 * and turning job results back into local files.</p>
 */
public interface ClientJobPlugin {
    String taskType();

    String displayName();

    List<String> supportedInputExtensions();

    List<String> parameterOptions();

    String defaultParameter();

    default String normalizeParameter(String parameter) {
        String value = parameter == null || parameter.isBlank()
                ? defaultParameter()
                : parameter.trim();
        List<String> options = parameterOptions();
        if (options == null || options.isEmpty()) {
            return value;
        }
        return options.stream()
                .filter(option -> option.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unsupported parameter '" + value + "' for " + displayName()
                                + ". Supported values: " + options));
    }

    List<Object> buildPayloads(List<Path> inputPaths, String parameter) throws Exception;

    void saveResults(List<Object> results, Path outputDir) throws Exception;
}
