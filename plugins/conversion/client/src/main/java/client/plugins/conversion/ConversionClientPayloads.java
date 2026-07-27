package client.plugins.conversion;

import com.google.gson.Gson;
import conversion.model.FilePayload;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreProvider;
import objectstore.TaskFlowObjectKeys;
import protocol.PayloadLimits;
import protocol.SafeFileNames;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

final class ConversionClientPayloads {
    private static final Gson GSON = new Gson();
    private static final int COPY_BUFFER_BYTES = 64 * 1024;

    private ConversionClientPayloads() {
    }

    static List<Object> buildFilePayloads(List<Path> inputPaths,
                                          List<String> allowedExtensions,
                                          ObjectStoreProvider objectStoreProvider) throws IOException {
        Objects.requireNonNull(objectStoreProvider, "objectStoreProvider");
        List<InputFile> inputs = validateInputs(inputPaths, allowedExtensions);
        long inlineLimit = PayloadLimits.maxInlinePayloadBytes();
        boolean needsObjectStore = inputs.stream().anyMatch(input -> input.size() >= inlineLimit);

        List<ObjectReference> uploadedReferences = new ArrayList<>();
        try (ObjectStore objectStore = needsObjectStore ? objectStoreProvider.open() : null) {
            try {
                List<Object> payloads = new ArrayList<>(inputs.size());
                long maxJobPayloadBytes = PayloadLimits.maxJobPayloadBytes();
                long totalPayloadBytes = 0L;
                for (InputFile input : inputs) {
                    FilePayload payload;
                    long payloadBytes;
                    if (input.size() >= inlineLimit) {
                        ObjectReference reference = uploadInput(objectStore, input);
                        uploadedReferences.add(reference);
                        payload = new FilePayload(input.fileName(), null, reference);
                        payloadBytes = serializedBytes(payload);
                    } else {
                        long encodedBytes = PayloadLimits.base64EncodedLength(input.size());
                        String base64 = Base64.getEncoder().encodeToString(
                                Files.readAllBytes(input.path())
                        );
                        payload = new FilePayload(input.fileName(), base64);
                        payloadBytes = Math.max(encodedBytes, base64.length());
                    }

                    if (PayloadLimits.wouldExceed(
                            totalPayloadBytes,
                            payloadBytes,
                            maxJobPayloadBytes
                    )) {
                        throw new IOException("Job payload exceeds "
                                + PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV + " ("
                                + maxJobPayloadBytes + " bytes): " + input.fileName());
                    }
                    totalPayloadBytes += payloadBytes;
                    payloads.add(payload);
                }
                return payloads;
            } catch (IOException | RuntimeException e) {
                cleanupUploadedReferences(objectStore, uploadedReferences);
                throw e;
            }
        }
    }

    static void saveFilePayloadResults(List<Object> results,
                                       Path outputDir,
                                       ObjectStoreProvider objectStoreProvider) throws IOException {
        Objects.requireNonNull(objectStoreProvider, "objectStoreProvider");
        Path root = outputDir.toAbsolutePath().normalize();
        Files.createDirectories(root);
        if (results == null || results.isEmpty()) {
            return;
        }

        int index = 0;
        for (Object raw : results) {
            FilePayload payload = GSON.fromJson(GSON.toJson(raw), FilePayload.class);
            if (payload == null || (!payload.hasInlineData() && !payload.hasObjectReference())) {
                continue;
            }

            byte[] data = readPayloadBytes(
                    payload,
                    PayloadLimits.maxResultBytes(),
                    objectStoreProvider
            );
            Path outputPath = uniqueOutputPath(
                    root,
                    payload.fileName(),
                    "result-" + index + ".bin"
            );
            Files.write(outputPath, data);
            index++;
        }
    }

    private static List<InputFile> validateInputs(List<Path> inputPaths,
                                                  List<String> allowedExtensions) throws IOException {
        if (inputPaths == null || inputPaths.isEmpty()) {
            throw new IOException("At least one input file is required.");
        }
        int maxTasksPerJob = PayloadLimits.maxTasksPerJob();
        if (inputPaths.size() > maxTasksPerJob) {
            throw new IOException("Input file count exceeds " + PayloadLimits.MAX_TASKS_PER_JOB_ENV
                    + " (" + maxTasksPerJob + "): " + inputPaths.size());
        }

        long maxInputBytes = PayloadLimits.maxInputBytes();
        List<InputFile> inputs = new ArrayList<>(inputPaths.size());
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
                throw new IOException("Input file exceeds " + PayloadLimits.MAX_INPUT_BYTES_ENV
                        + " (" + maxInputBytes + " bytes): " + input.getFileName());
            }
            inputs.add(new InputFile(input, input.getFileName().toString(), size));
        }
        return inputs;
    }

    private static ObjectReference uploadInput(ObjectStore objectStore,
                                               InputFile input) throws IOException {
        if (objectStore == null) {
            throw new IOException("Object storage is required for " + input.fileName() + ".");
        }
        ObjectReference expected = new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", UUID.randomUUID().toString()),
                input.size(),
                sha256(input.path()),
                contentType(input.path())
        );
        try (InputStream content = Files.newInputStream(input.path())) {
            return objectStore.put(expected, content);
        }
    }

    private static byte[] readPayloadBytes(FilePayload payload,
                                           long maxBytes,
                                           ObjectStoreProvider objectStoreProvider) throws IOException {
        if (payload.hasInlineData() == payload.hasObjectReference()) {
            throw new IOException("Result payload must contain exactly one of Base64 data or an "
                    + "object reference: " + payload.fileName());
        }
        if (payload.hasObjectReference()) {
            ObjectReference reference = payload.objectReference();
            if (reference.contentLength() > maxBytes) {
                throw new IOException("Result object exceeds " + PayloadLimits.MAX_RESULT_BYTES_ENV
                        + " (" + maxBytes + " bytes): " + payload.fileName());
            }
            try (ObjectStore objectStore = objectStoreProvider.open();
                 InputStream content = objectStore.get(reference.key())) {
                return readBounded(content, maxBytes, payload.fileName());
            }
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

    private static byte[] readBounded(InputStream content,
                                      long maxBytes,
                                      String fileName) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[COPY_BUFFER_BYTES];
        long total = 0L;
        int read;
        while ((read = content.read(buffer)) != -1) {
            if (PayloadLimits.wouldExceed(total, read, maxBytes)) {
                throw new IOException("Result object exceeds " + PayloadLimits.MAX_RESULT_BYTES_ENV
                        + " (" + maxBytes + " bytes): " + fileName);
            }
            output.write(buffer, 0, read);
            total += read;
        }
        return output.toByteArray();
    }

    private static String sha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must be available.", e);
        }
        byte[] buffer = new byte[COPY_BUFFER_BYTES];
        try (InputStream content = Files.newInputStream(path)) {
            int read;
            while ((read = content.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String contentType(Path path) throws IOException {
        String detected = Files.probeContentType(path);
        return detected == null || detected.isBlank()
                ? "application/octet-stream"
                : detected;
    }

    private static long serializedBytes(FilePayload payload) {
        return GSON.toJson(payload).getBytes(StandardCharsets.UTF_8).length;
    }

    private static void cleanupUploadedReferences(ObjectStore objectStore,
                                                  List<ObjectReference> references) {
        if (objectStore == null) {
            return;
        }
        for (ObjectReference reference : references) {
            try {
                objectStore.delete(reference.key());
            } catch (IOException ignored) {
                // TF-0506 owns bounded cleanup for objects that survive best-effort deletion.
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
            Path next = SafeFileNames.safeOutputPath(
                    outputDir,
                    baseName + "-" + copy + extension,
                    fallback
            );
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

    private record InputFile(Path path, String fileName, long size) {
    }
}
