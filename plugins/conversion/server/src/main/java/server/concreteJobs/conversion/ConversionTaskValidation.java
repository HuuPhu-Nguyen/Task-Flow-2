package server.concreteJobs.conversion;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import conversion.model.FilePayload;
import protocol.JobSubmitMessage;

import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

final class ConversionTaskValidation {
    private static final Gson GSON = new Gson();

    private ConversionTaskValidation() {
    }

    static void validate(JobSubmitMessage message,
                         Collection<String> allowedTargetFormats,
                         Collection<String> allowedInputExtensions) {
        validateParameter(message.getParameter(), allowedTargetFormats);
        List<Object> payloads = message.getTaskPayloads();
        if (payloads == null || payloads.isEmpty()) {
            throw new IllegalArgumentException("Conversion jobs require at least one file payload.");
        }

        for (int i = 0; i < payloads.size(); i++) {
            FilePayload payload = parsePayload(payloads.get(i), i);
            validatePayload(payload, i, allowedInputExtensions);
        }
    }

    private static void validateParameter(String parameter, Collection<String> allowedTargetFormats) {
        if (parameter == null || parameter.isBlank()) {
            throw new IllegalArgumentException("Conversion target format is required.");
        }
        String normalized = parameter.trim().toLowerCase(Locale.ROOT);
        boolean supported = allowedTargetFormats.stream()
                .anyMatch(format -> format.equalsIgnoreCase(normalized));
        if (!supported) {
            throw new IllegalArgumentException("Unsupported conversion target format '" + parameter
                    + "'. Supported formats: " + allowedTargetFormats);
        }
    }

    private static FilePayload parsePayload(Object rawPayload, int index) {
        try {
            return GSON.fromJson(GSON.toJson(rawPayload), FilePayload.class);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("Conversion payload " + index + " is not a file payload.", e);
        }
    }

    private static void validatePayload(FilePayload payload,
                                        int index,
                                        Collection<String> allowedInputExtensions) {
        if (payload == null) {
            throw new IllegalArgumentException("Conversion payload " + index + " is required.");
        }
        if (payload.fileName() == null || payload.fileName().isBlank()) {
            throw new IllegalArgumentException("Conversion payload " + index + " requires a file name.");
        }
        if (!hasAllowedExtension(payload.fileName(), allowedInputExtensions)) {
            throw new IllegalArgumentException("Unsupported conversion input file type for " + payload.fileName()
                    + ". Supported extensions: " + allowedInputExtensions);
        }
        if (payload.base64Data() == null || payload.base64Data().isBlank()) {
            throw new IllegalArgumentException("Conversion payload " + index + " requires Base64 data.");
        }
        try {
            Base64.getDecoder().decode(payload.base64Data());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Conversion payload " + index + " has invalid Base64 data.", e);
        }
    }

    private static boolean hasAllowedExtension(String fileName, Collection<String> allowedInputExtensions) {
        if (allowedInputExtensions == null || allowedInputExtensions.isEmpty()) {
            return true;
        }
        String normalizedName = fileName.toLowerCase(Locale.ROOT);
        for (String extension : allowedInputExtensions) {
            if (extension == null || extension.isBlank()) {
                continue;
            }
            String normalizedExtension = extension.trim().toLowerCase(Locale.ROOT);
            if (normalizedExtension.startsWith("*.")) {
                normalizedExtension = normalizedExtension.substring(1);
            }
            if (!normalizedExtension.startsWith(".")) {
                normalizedExtension = "." + normalizedExtension;
            }
            if (normalizedName.endsWith(normalizedExtension)) {
                return true;
            }
        }
        return false;
    }
}
