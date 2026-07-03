package example.model;

import java.util.List;

public record ExampleJobSummary(int documentCount,
                                int totalWordCount,
                                List<ExampleTaskResult> documents) {
    public ExampleJobSummary {
        documents = documents == null ? List.of() : List.copyOf(documents);
    }
}
