package server.concreteJobs.text;

public record TextAnalysisResult(
        String documentName,
        int lineCount,
        int wordCount,
        int characterCount,
        int uniqueWordCount
) {
}
