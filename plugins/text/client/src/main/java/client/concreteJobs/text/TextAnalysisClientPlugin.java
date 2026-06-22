package client.concreteJobs.text;

import client.ClientJobPlugin;
import client.PayloadLimits;
import com.google.gson.Gson;
import protocol.SafeFileNames;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;
import text.model.TextAnalysisTaskTypes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class TextAnalysisClientPlugin implements ClientJobPlugin {
    private static final List<String> INPUT_EXTENSIONS = List.of(".txt", ".md", ".csv", ".log");
    private static final List<String> RESULT_FORMATS = List.of("csv");
    private static final Gson GSON = new Gson();

    @Override
    public String taskType() {
        return TextAnalysisTaskTypes.TEXT_ANALYSIS;
    }

    @Override
    public String displayName() {
        return "Text Analysis";
    }

    @Override
    public List<String> supportedInputExtensions() {
        return INPUT_EXTENSIONS;
    }

    @Override
    public List<String> parameterOptions() {
        return RESULT_FORMATS;
    }

    @Override
    public String defaultParameter() {
        return "csv";
    }

    @Override
    public List<Object> buildPayloads(List<Path> inputPaths, String parameter) throws Exception {
        normalizeParameter(parameter);
        if (inputPaths == null || inputPaths.isEmpty()) {
            throw new IOException("At least one input text file is required.");
        }
        int maxTasksPerJob = PayloadLimits.maxTasksPerJob();
        if (inputPaths.size() > maxTasksPerJob) {
            throw new IOException("Input file count exceeds " + PayloadLimits.MAX_TASKS_PER_JOB_ENV
                    + " (" + maxTasksPerJob + "): " + inputPaths.size());
        }

        List<Object> payloads = new ArrayList<>();
        long maxInputBytes = PayloadLimits.maxInputBytes();
        long maxJobPayloadBytes = PayloadLimits.maxJobPayloadBytes();
        long totalPayloadBytes = 0L;
        for (Path path : inputPaths) {
            Path input = path.toAbsolutePath().normalize();
            if (!Files.isRegularFile(input)) {
                throw new IOException("Input file does not exist: " + input);
            }
            if (!hasAllowedExtension(input)) {
                throw new IOException("Unsupported input file type for " + input.getFileName()
                        + ". Supported extensions: " + INPUT_EXTENSIONS);
            }
            long size = Files.size(input);
            if (size > maxInputBytes) {
                throw new IOException("Input file exceeds " + PayloadLimits.MAX_INPUT_BYTES_ENV + " (" + maxInputBytes
                        + " bytes): " + input.getFileName());
            }
            if (PayloadLimits.wouldExceed(totalPayloadBytes, size, maxJobPayloadBytes)) {
                throw new IOException("Text job payload exceeds " + PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV
                        + " (" + maxJobPayloadBytes + " bytes): " + input.getFileName());
            }
            totalPayloadBytes += size;
            payloads.add(new TextAnalysisPayload(
                    input.getFileName().toString(),
                    Files.readString(input, StandardCharsets.UTF_8)
            ));
        }
        return payloads;
    }

    @Override
    public void saveResults(List<Object> results, Path outputDir) throws Exception {
        Path root = outputDir.toAbsolutePath().normalize();
        Files.createDirectories(root);

        Path outputPath = uniqueOutputPath(root, "text-analysis-results.csv");
        List<TextAnalysisResult> typedResults = results == null
                ? List.of()
                : results.stream()
                .map(raw -> GSON.fromJson(GSON.toJson(raw), TextAnalysisResult.class))
                .toList();

        List<String> lines = new ArrayList<>();
        lines.add("document,line_count,word_count,character_count,unique_word_count");
        for (TextAnalysisResult result : typedResults) {
            if (result == null) {
                continue;
            }
            lines.add(csv(result.documentName()) + ","
                    + result.lineCount() + ","
                    + result.wordCount() + ","
                    + result.characterCount() + ","
                    + result.uniqueWordCount());
        }
        Files.write(outputPath, lines, StandardCharsets.UTF_8);
    }

    private static boolean hasAllowedExtension(Path input) {
        String fileName = input.getFileName().toString().toLowerCase(Locale.ROOT);
        return INPUT_EXTENSIONS.stream().anyMatch(fileName::endsWith);
    }

    private static Path uniqueOutputPath(Path outputDir, String fileName) throws IOException {
        Path candidate = SafeFileNames.safeOutputPath(outputDir, fileName, "text-analysis-results.csv");
        if (!Files.exists(candidate)) {
            return candidate;
        }

        String safeName = candidate.getFileName().toString();
        int dot = safeName.lastIndexOf('.');
        String baseName = dot > 0 ? safeName.substring(0, dot) : safeName;
        String extension = dot > 0 ? safeName.substring(dot) : "";

        for (int copy = 1; copy < 10_000; copy++) {
            Path next = SafeFileNames.safeOutputPath(
                    outputDir,
                    baseName + "-" + copy + extension,
                    "text-analysis-results.csv"
            );
            if (!Files.exists(next)) {
                return next;
            }
        }
        throw new IOException("Could not create a unique text-analysis output file.");
    }

    private static String csv(String value) {
        String safeValue = value == null ? "" : value;
        if (!safeValue.contains(",") && !safeValue.contains("\"")
                && !safeValue.contains("\n") && !safeValue.contains("\r")) {
            return safeValue;
        }
        return "\"" + safeValue.replace("\"", "\"\"") + "\"";
    }
}
