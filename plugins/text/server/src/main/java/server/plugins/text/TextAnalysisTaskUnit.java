package server.plugins.text;

import server.job.TaskUnit;
import text.model.TextAnalysisPayload;

public class TextAnalysisTaskUnit extends TaskUnit<TextAnalysisPayload> {
    public TextAnalysisTaskUnit(String taskId, String jobId, TextAnalysisPayload payload) {
        super(taskId, jobId, payload);
    }
}
