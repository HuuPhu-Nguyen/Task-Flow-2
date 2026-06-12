package server.concreteJobs.conversion;

import com.google.gson.Gson;
import protocol.FilePayload;
import protocol.SafeFileNames;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

final class ConversionClientPayloads {
    private static final Gson GSON = new Gson();

    private ConversionClientPayloads() {
    }

    static List<Object> buildFilePayloads(List<Path> inputPaths, List<String> allowedExtensions) throws IOException {
        if (inputPaths == null || inputPaths.isEmpty()) {
            throw new IOException("At least one input file is required.");
        }

        List<Object> payloads = new ArrayList<>();
        for (Path path : inputPaths) {
            Path input = path.toAbsolutePath().normalize();
            if (!Files.isRegularFile(input)) {
                throw new IOException("Input file does not exist: " + input);
            }
            if (!hasAllowedExtension(input, allowedExtensions)) {
                throw new IOException("Unsupported input file type for " + input.getFileName()
                        + ". Supported extensions: " + allowedExtensions);
            }
            String base64 = Base64.getEncoder().encodeToString(Files.readAllBytes(input));
            payloads.add(new FilePayload(input.getFileName().toString(), base64));
        }
        return payloads;
    }

    static void saveFilePayloadResults(List<Object> results, Path outputDir) throws IOException {
        Path root = outputDir.toAbsolutePath().normalize();
        Files.createDirectories(root);
        if (results == null || results.isEmpty()) {
            return;
        }

        int index = 0;
        for (Object raw : results) {
            FilePayload payload = GSON.fromJson(GSON.toJson(raw), FilePayload.class);
            if (payload == null || payload.base64Data() == null || payload.base64Data().isBlank()) {
                continue;
            }

            byte[] data;
            try {
                data = Base64.getDecoder().decode(payload.base64Data());
            } catch (IllegalArgumentException e) {
                throw new IOException("Result payload is not valid Base64 for " + payload.fileName(), e);
            }

            Path outputPath = uniqueOutputPath(root, payload.fileName(), "result-" + index + ".bin");
            Files.write(outputPath, data);
            index++;
        }
    }

    private static Path uniqueOutputPath(Path outputDir, String fileName, String fallback) throws IOException {
        Path candidate = SafeFileNames.safeOutputPath(outputDir, fileName, fallback);
        if (!Files.exists(candidate)) {
            return candidate;
        }

        String safeName = candidate.getFileName().toString();
        int dot = safeName.lastIndexOf('.');
        String baseName = dot > 0 ? safeName.substring(0, dot) : safeName;
        String extension = dot > 0 ? safeName.substring(dot) : "";

        for (int copy = 1; copy < 10_000; copy++) {
            Path next = SafeFileNames.safeOutputPath(outputDir, baseName + "-" + copy + extension, fallback);
            if (!Files.exists(next)) {
                return next;
            }
        }
        throw new IOException("Could not create a unique output file for " + fileName);
    }

    private static boolean hasAllowedExtension(Path input, List<String> allowedExtensions) {
        if (allowedExtensions == null || allowedExtensions.isEmpty()) {
            return true;
        }

        String fileName = input.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
        for (String extension : allowedExtensions) {
            if (extension == null || extension.isBlank()) {
                continue;
            }
            String normalized = extension.trim().toLowerCase(java.util.Locale.ROOT);
            if (normalized.startsWith("*.")) {
                normalized = normalized.substring(1);
            }
            if (!normalized.startsWith(".")) {
                normalized = "." + normalized;
            }
            if (fileName.endsWith(normalized)) {
                return true;
            }
        }
        return false;
    }
}
