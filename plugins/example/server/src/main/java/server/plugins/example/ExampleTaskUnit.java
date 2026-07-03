package server.plugins.example;

import example.model.ExamplePayload;
import server.job.TaskUnit;

public class ExampleTaskUnit extends TaskUnit<ExamplePayload> {
    public ExampleTaskUnit(String taskId, String jobId, ExamplePayload payload) {
        super(taskId, jobId, payload);
    }
}
