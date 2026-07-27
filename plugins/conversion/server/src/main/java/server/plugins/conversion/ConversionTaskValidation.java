package server.plugins.conversion;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import conversion.model.FilePayload;
import protocol.JobSubmitMessage;
import protocol.PayloadLimits;

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

    static FilePayload validateResult(FilePayload result, String targetFormat) {
        if (result == null) {
            throw new IllegalArgumentException("Conversion result is required.");
        }
        if (result.fileName() == null || result.fileName().isBlank()) {
            throw new IllegalArgumentException("Conversion result requires a file name.");
        }
        String normalizedTarget = targetFormat == null
                ? ""
                : targetFormat.trim().toLowerCase(Locale.ROOT);
        if ("jpeg".equals(normalizedTarget)) {
            normalizedTarget = "jpg";
        }
        if (normalizedTarget.isBlank()
                || !hasAllowedExtension(result.fileName(), List.of(normalizedTarget))) {
            throw new IllegalArgumentException(
                    "Conversion result file name must use target format '" + normalizedTarget + "'."
            );
        }
        if (result.hasInlineData() == result.hasObjectReference()) {
            throw new IllegalArgumentException(
                    "Conversion result requires exactly one of Base64 data or an object reference."
            );
        }
        if (result.hasObjectReference()) {
            long contentLength = result.objectReference().contentLength();
            if (contentLength <= 0L) {
                throw new IllegalArgumentException(
                        "Conversion result object reference must point to a non-empty file."
                );
            }
            if (contentLength > PayloadLimits.maxResultBytes()) {
                throw new IllegalArgumentException(
                        "Conversion result object reference exceeds "
                                + PayloadLimits.MAX_RESULT_BYTES_ENV + " ("
                                + PayloadLimits.maxResultBytes() + " bytes)."
                );
            }
            return result;
        }

        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(result.base64Data());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Conversion result has invalid Base64 data.", e);
        }
        if (decoded.length == 0) {
            throw new IllegalArgumentException("Conversion result must not be empty.");
        }
        if (decoded.length > PayloadLimits.maxResultBytes()) {
            throw new IllegalArgumentException(
                    "Conversion result exceeds " + PayloadLimits.MAX_RESULT_BYTES_ENV + " ("
                            + PayloadLimits.maxResultBytes() + " bytes)."
            );
        }
        if (decoded.length >= PayloadLimits.maxInlinePayloadBytes()) {
            throw new IllegalArgumentException(
                    "Conversion result inline data must be smaller than "
                            + PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_ENV + " ("
                            + PayloadLimits.maxInlinePayloadBytes() + " bytes)."
            );
        }
        return result;
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
        if (payload.hasInlineData() == payload.hasObjectReference()) {
            throw new IllegalArgumentException("Conversion payload " + index
                    + " requires exactly one of Base64 data or an object reference.");
        }
        if (payload.hasObjectReference()) {
            validateObjectReference(payload, index);
            return;
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(payload.base64Data());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Conversion payload " + index + " has invalid Base64 data.", e);
        }
        long inlineLimit = PayloadLimits.maxInlinePayloadBytes();
        if (decoded.length >= inlineLimit) {
            throw new IllegalArgumentException("Conversion payload " + index
                    + " inline data must be smaller than "
                    + PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_ENV + " (" + inlineLimit
                    + " bytes).");
        }
    }

    private static void validateObjectReference(FilePayload payload, int index) {
        if (payload.objectReference().contentLength() <= 0) {
            throw new IllegalArgumentException("Conversion payload " + index
                    + " object reference must point to a non-empty file.");
        }
        if (payload.objectReference().contentLength() > PayloadLimits.maxInputBytes()) {
            throw new IllegalArgumentException("Conversion payload " + index
                    + " object reference exceeds " + PayloadLimits.MAX_INPUT_BYTES_ENV + " ("
                    + PayloadLimits.maxInputBytes() + " bytes).");
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
