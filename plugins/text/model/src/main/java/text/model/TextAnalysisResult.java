package text.model;

public record TextAnalysisResult(
        String documentName,
        int lineCount,
        int wordCount,
        int characterCount,
        int uniqueWordCount
) {
}
