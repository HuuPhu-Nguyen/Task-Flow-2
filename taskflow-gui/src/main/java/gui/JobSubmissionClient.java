package gui;

import java.io.PrintWriter;
import java.util.List;

interface JobSubmissionClient {
    String submitJob(String taskType, List<?> payloads, String parameter, PrintWriter out);
}
