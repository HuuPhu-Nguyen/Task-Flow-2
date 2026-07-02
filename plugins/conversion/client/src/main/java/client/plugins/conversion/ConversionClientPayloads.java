package client.plugins.conversion;

import protocol.LocalPayloadStorage;
import protocol.PayloadLimits;
import com.google.gson.Gson;
import conversion.model.FilePayload;
import protocol.PayloadReference;
import protocol.SafeFileNames;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

final class ConversionClientPayloads {
    private static final Gson GSON = new Gson();

    private ConversionClientPayloads() {
    }

    static List<Object> buildFilePayloads(List<Path> inputPaths, List<String> allowedExtensions) throws IOException {
        if (inputPaths == null || inputPaths.isEmpty()) {
            throw new IOException("At least one input file is required.");
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
        List<PayloadReference> storedReferences = new ArrayList<>();
        try {
            for (Path path : inputPaths) {
                Path input = path.toAbsolutePath().normalize();
                if (!Files.isRegularFile(input)) {
                    throw new IOException("Input file does not exist: " + input);
                }
                if (!hasAllowedExtension(input, allowedExtensions)) {
                    throw new IOException("Unsupported input file type for " + input.getFileName()
                            + ". Supported extensions: " + allowedExtensions);
                }
                long size = Files.size(input);
                if (size > maxInputBytes) {
                    throw new IOException("Input file exceeds " + PayloadLimits.MAX_INPUT_BYTES_ENV + " ("
                            + maxInputBytes + " bytes): " + input.getFileName());
                }

                FilePayload payload;
                long payloadBytes;
                if (LocalPayloadStorage.shouldExternalize(size)) {
                    PayloadReference reference = LocalPayloadStorage.storeFile(input, input.getFileName().toString());
                    storedReferences.add(reference);
                    payload = new FilePayload(input.getFileName().toString(), null, reference);
                    payloadBytes = serializedBytes(payload);
                } else {
                    long encodedBytes = PayloadLimits.base64EncodedLength(size);
                    String base64 = Base64.getEncoder().encodeToString(Files.readAllBytes(input));
                    payload = new FilePayload(input.getFileName().toString(), base64);
                    payloadBytes = Math.max(encodedBytes, base64.length());
                }

                if (PayloadLimits.wouldExceed(totalPayloadBytes, payloadBytes, maxJobPayloadBytes)) {
                    throw new IOException("Job payload exceeds " + PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV
                            + " (" + maxJobPayloadBytes + " bytes): " + input.getFileName());
                }
                totalPayloadBytes += payloadBytes;
                payloads.add(payload);
            }
        } catch (IOException | RuntimeException e) {
            cleanupStoredReferences(storedReferences);
            throw e;
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
            if (payload == null || (!payload.hasInlineData() && !payload.hasPayloadReference())) {
                continue;
            }

            long maxResultBytes = PayloadLimits.maxResultBytes();
            byte[] data = readPayloadBytes(payload, maxResultBytes);

            Path outputPath = uniqueOutputPath(root, payload.fileName(), "result-" + index + ".bin");
            Files.write(outputPath, data);
            index++;
        }
    }

    private static byte[] readPayloadBytes(FilePayload payload, long maxBytes) throws IOException {
        if (payload.hasInlineData() == payload.hasPayloadReference()) {
            throw new IOException("Result payload must contain exactly one of Base64 data or a payload reference: "
                    + payload.fileName());
        }
        if (payload.hasPayloadReference()) {
            return LocalPayloadStorage.read(payload.payloadReference(), maxBytes);
        }
        byte[] data;
        try {
            data = Base64.getDecoder().decode(payload.base64Data());
        } catch (IllegalArgumentException e) {
            throw new IOException("Result payload is not valid Base64 for " + payload.fileName(), e);
        }
        if (data.length > maxBytes) {
            throw new IOException("Result payload exceeds " + PayloadLimits.MAX_RESULT_BYTES_ENV
                    + " (" + maxBytes + " bytes): " + payload.fileName());
        }
        return data;
    }

    private static long serializedBytes(FilePayload payload) {
        return GSON.toJson(payload).getBytes(StandardCharsets.UTF_8).length;
    }

    private static void cleanupStoredReferences(List<PayloadReference> references) {
        for (PayloadReference reference : references) {
            try {
                LocalPayloadStorage.delete(reference);
            } catch (IOException ignored) {
                // Best-effort cleanup for references created during a failed payload build.
            }
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

        String fileName = input.getFileName().toString().toLowerCase(Locale.ROOT);
        for (String extension : allowedExtensions) {
            if (extension == null || extension.isBlank()) {
                continue;
            }
            String normalized = extension.trim().toLowerCase(Locale.ROOT);
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
