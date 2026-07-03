package client.plugins.example;

import client.ClientJobPlugin;
import com.google.gson.Gson;
import example.model.ExampleJobSummary;
import example.model.ExamplePayload;
import example.model.ExampleTaskResult;
import example.model.ExampleTaskTypes;
import protocol.JobResultMessage;
import protocol.PayloadLimits;
import protocol.SafeFileNames;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class ExampleWordCountClientPlugin implements ClientJobPlugin {
    private static final List<String> INPUT_EXTENSIONS = List.of(".txt");
    private static final List<String> RESULT_FORMATS = List.of("summary");
    private static final String REPORT_FILE = "example-word-count-summary.txt";
    private static final Gson GSON = new Gson();

    @Override
    public String taskType() {
        return ExampleTaskTypes.WORD_COUNT;
    }

    @Override
    public String displayName() {
        return "Example Word Count";
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
        return "summary";
    }

    @Override
    public List<Object> buildPayloads(List<Path> inputPaths, String parameter) throws Exception {
        normalizeParameter(parameter);
        if (inputPaths == null || inputPaths.isEmpty()) {
            throw new IOException("At least one .txt input file is required for the example plugin.");
        }
        int maxTasks = PayloadLimits.maxTasksPerJob();
        if (inputPaths.size() > maxTasks) {
            throw new IOException("Example input file count exceeds " + PayloadLimits.MAX_TASKS_PER_JOB_ENV
                    + " (" + maxTasks + "): " + inputPaths.size());
        }

        long maxInputBytes = PayloadLimits.maxInputBytes();
        long maxJobPayloadBytes = PayloadLimits.maxJobPayloadBytes();
        long totalPayloadBytes = 0L;
        List<Object> payloads = new ArrayList<>();
        for (Path path : inputPaths) {
            Path input = path.toAbsolutePath().normalize();
            if (!Files.isRegularFile(input)) {
                throw new IOException("Example input file does not exist: " + input);
            }
            if (!hasAllowedExtension(input)) {
                throw new IOException("Unsupported example input file type for " + input.getFileName()
                        + ". Supported extensions: " + INPUT_EXTENSIONS);
            }
            long size = Files.size(input);
            if (size > maxInputBytes) {
                throw new IOException("Example input file exceeds " + PayloadLimits.MAX_INPUT_BYTES_ENV
                        + " (" + maxInputBytes + " bytes): " + input.getFileName());
            }
            if (PayloadLimits.wouldExceed(totalPayloadBytes, size, maxJobPayloadBytes)) {
                throw new IOException("Example job payload exceeds " + PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV
                        + " (" + maxJobPayloadBytes + " bytes): " + input.getFileName());
            }
            totalPayloadBytes += size;
            payloads.add(new ExamplePayload(
                    input.getFileName().toString(),
                    Files.readString(input, StandardCharsets.UTF_8)
            ));
        }
        return payloads;
    }

    @Override
    public void handleResult(JobResultMessage result, Path outputDir) throws Exception {
        if (result == null || !result.isSuccessful()) {
            String reason = result == null ? "missing result" : result.getErrorMessage();
            throw new IOException("Cannot handle failed example result: " + reason);
        }
        ExampleJobSummary summary = GSON.fromJson(GSON.toJson(result.getResultPayload()), ExampleJobSummary.class);
        if (summary == null) {
            throw new IOException("Example result summary is missing.");
        }
        writeReport(summary, outputDir);
    }

    @Override
    public void saveResults(List<Object> results, Path outputDir) throws Exception {
        List<ExampleTaskResult> typedResults = results == null
                ? List.of()
                : results.stream()
                .map(raw -> GSON.fromJson(GSON.toJson(raw), ExampleTaskResult.class))
                .toList();
        int total = typedResults.stream()
                .mapToInt(ExampleTaskResult::wordCount)
                .sum();
        writeReport(new ExampleJobSummary(typedResults.size(), total, typedResults), outputDir);
    }

    private static void writeReport(ExampleJobSummary summary, Path outputDir) throws IOException {
        Path root = outputDir.toAbsolutePath().normalize();
        Files.createDirectories(root);
        Path outputPath = uniqueOutputPath(root, REPORT_FILE);

        List<String> lines = new ArrayList<>();
        lines.add("TaskFlow example word count");
        lines.add("documents=" + summary.documentCount());
        lines.add("total_words=" + summary.totalWordCount());
        lines.add("");
        lines.add("document,word_count");
        for (ExampleTaskResult document : summary.documents()) {
            lines.add(csv(document.documentName()) + "," + document.wordCount());
        }
        Files.write(outputPath, lines, StandardCharsets.UTF_8);
    }

    private static boolean hasAllowedExtension(Path input) {
        String fileName = input.getFileName().toString().toLowerCase(Locale.ROOT);
        return INPUT_EXTENSIONS.stream().anyMatch(fileName::endsWith);
    }

    private static Path uniqueOutputPath(Path outputDir, String fileName) throws IOException {
        Path candidate = SafeFileNames.safeOutputPath(outputDir, fileName, REPORT_FILE);
        if (!Files.exists(candidate)) {
            return candidate;
        }

        String safeName = candidate.getFileName().toString();
        int dot = safeName.lastIndexOf('.');
        String baseName = dot > 0 ? safeName.substring(0, dot) : safeName;
        String extension = dot > 0 ? safeName.substring(dot) : "";
        for (int copy = 1; copy < 10_000; copy++) {
            Path next = SafeFileNames.safeOutputPath(outputDir, baseName + "-" + copy + extension, REPORT_FILE);
            if (!Files.exists(next)) {
                return next;
            }
        }
        throw new IOException("Could not create a unique example output file.");
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
