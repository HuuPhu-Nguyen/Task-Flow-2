package server.concreteJobs.text;

import server.job.TaskUnit;

public class TextAnalysisTaskUnit extends TaskUnit<TextAnalysisPayload> {
    public TextAnalysisTaskUnit(String taskId, String jobId, TextAnalysisPayload payload) {
        super(taskId, jobId, payload);
    }
}
