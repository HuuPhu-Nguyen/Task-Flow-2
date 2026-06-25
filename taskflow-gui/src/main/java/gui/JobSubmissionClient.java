package gui;

import java.io.PrintWriter;
import java.util.List;

interface JobSubmissionClient {
    String newJobId();

    void submitJob(String jobId, String taskType, List<?> payloads, String parameter, PrintWriter out);
}
