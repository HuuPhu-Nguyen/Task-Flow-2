package peer.processors;

import com.google.gson.Gson;
import peer.engine.TaskProcessor;
import protocol.TaskAssignMessage;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextAnalysisProcessor implements TaskProcessor<TextAnalysisResult> {
    private static final Pattern WORD_PATTERN = Pattern.compile("[\\p{L}\\p{N}']+");

    private final Gson gson = new Gson();

    @Override
    public TextAnalysisResult process(TaskAssignMessage task) {
        TextAnalysisPayload payload = gson.fromJson(gson.toJson(task.getPayload()), TextAnalysisPayload.class);
        String text = payload.text() == null ? "" : payload.text();

        int wordCount = 0;
        Set<String> uniqueWords = new HashSet<>();
        Matcher matcher = WORD_PATTERN.matcher(text);
        while (matcher.find()) {
            wordCount++;
            uniqueWords.add(matcher.group().toLowerCase(Locale.ROOT));
        }

        return new TextAnalysisResult(
                payload.documentName(),
                lineCount(text),
                wordCount,
                text.length(),
                uniqueWords.size()
        );
    }

    private static int lineCount(String text) {
        if (text.isEmpty()) {
            return 0;
        }
        return text.split("\\R", -1).length;
    }
}
