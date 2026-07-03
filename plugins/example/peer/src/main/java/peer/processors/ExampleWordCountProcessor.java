package peer.processors;

import com.google.gson.Gson;
import example.model.ExamplePayload;
import example.model.ExampleTaskResult;
import peer.engine.TaskProcessor;
import protocol.TaskAssignMessage;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExampleWordCountProcessor implements TaskProcessor<ExampleTaskResult> {
    private static final Pattern WORD_PATTERN = Pattern.compile("[\\p{L}\\p{N}']+");
    private final Gson gson = new Gson();

    @Override
    public ExampleTaskResult process(TaskAssignMessage task) {
        ExamplePayload payload = gson.fromJson(gson.toJson(task.getPayload()), ExamplePayload.class);
        if (payload == null || payload.documentName() == null || payload.documentName().isBlank()) {
            throw new IllegalArgumentException("Example task payload requires a document name.");
        }
        if (payload.text() == null) {
            throw new IllegalArgumentException("Example task payload requires text.");
        }
        return new ExampleTaskResult(payload.documentName(), countWords(payload.text()));
    }

    private static int countWords(String text) {
        int count = 0;
        Matcher matcher = WORD_PATTERN.matcher(text);
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
