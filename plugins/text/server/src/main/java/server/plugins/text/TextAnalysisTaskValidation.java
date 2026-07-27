package server.plugins.text;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import protocol.JobSubmitMessage;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;

import java.util.List;

final class TextAnalysisTaskValidation {
    private static final Gson GSON = new Gson();
    private static final List<String> RESULT_FORMATS = List.of("csv");

    private TextAnalysisTaskValidation() {
    }

    static void validate(JobSubmitMessage message) {
        validateParameter(message.getParameter());
        List<Object> payloads = message.getTaskPayloads();
        if (payloads == null || payloads.isEmpty()) {
            throw new IllegalArgumentException("Text analysis jobs require at least one text payload.");
        }

        for (int i = 0; i < payloads.size(); i++) {
            TextAnalysisPayload payload = parsePayload(payloads.get(i), i);
            validatePayload(payload, i);
        }
    }

    static TextAnalysisResult validateResult(TextAnalysisResult result) {
        if (result == null) {
            throw new IllegalArgumentException("Text analysis result is required.");
        }
        if (result.documentName() == null || result.documentName().isBlank()) {
            throw new IllegalArgumentException("Text analysis result requires a document name.");
        }
        if (result.lineCount() < 0
                || result.wordCount() < 0
                || result.characterCount() < 0
                || result.uniqueWordCount() < 0) {
            throw new IllegalArgumentException("Text analysis result counts must not be negative.");
        }
        if (result.uniqueWordCount() > result.wordCount()) {
            throw new IllegalArgumentException(
                    "Text analysis unique-word count must not exceed word count."
            );
        }
        return result;
    }

    private static void validateParameter(String parameter) {
        if (parameter == null || parameter.isBlank()) {
            throw new IllegalArgumentException("Text analysis result format is required.");
        }
        boolean supported = RESULT_FORMATS.stream()
                .anyMatch(format -> format.equalsIgnoreCase(parameter.trim()));
        if (!supported) {
            throw new IllegalArgumentException("Unsupported text analysis result format '" + parameter
                    + "'. Supported formats: " + RESULT_FORMATS);
        }
    }

    private static TextAnalysisPayload parsePayload(Object rawPayload, int index) {
        try {
            return GSON.fromJson(GSON.toJson(rawPayload), TextAnalysisPayload.class);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("Text analysis payload " + index
                    + " is not a text payload.", e);
        }
    }

    private static void validatePayload(TextAnalysisPayload payload, int index) {
        if (payload == null) {
            throw new IllegalArgumentException("Text analysis payload " + index + " is required.");
        }
        if (payload.documentName() == null || payload.documentName().isBlank()) {
            throw new IllegalArgumentException("Text analysis payload " + index + " requires a document name.");
        }
        if (payload.text() == null) {
            throw new IllegalArgumentException("Text analysis payload " + index + " requires text.");
        }
    }
}
