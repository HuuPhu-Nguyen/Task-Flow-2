package gui;

import client.ClientJobPlugin;

import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

final class GuiJobSubmitter {
    private GuiJobSubmitter() {
    }

    static SubmittedJob submitPreparedPayloads(
            JobSubmissionClient jobSubmissionClient,
            ClientJobPlugin plugin,
            List<Object> payloads,
            String targetFormat,
            PrintWriter out,
            BooleanSupplier connectionCurrent,
            Runnable onSendFailure,
            Set<String> activeJobIds) {
        if (!connectionCurrent.getAsBoolean()) {
            throw new IllegalStateException("Connection to coordinator changed before submission.");
        }

        String jobId = jobSubmissionClient.newJobId();
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalStateException("Job submission client returned a blank job id.");
        }

        if (!activeJobIds.add(jobId)) {
            throw new IllegalStateException("Job id is already active: " + jobId);
        }

        if (!connectionCurrent.getAsBoolean()) {
            activeJobIds.remove(jobId);
            throw new IllegalStateException("Connection to coordinator changed before submission.");
        }

        try {
            jobSubmissionClient.submitJob(jobId, plugin.taskType(), payloads, targetFormat, out);
        } catch (RuntimeException sendFailure) {
            activeJobIds.remove(jobId);
            onSendFailure.run();
            throw sendFailure;
        }

        return new SubmittedJob(jobId, plugin, activeJobIds.contains(jobId));
    }

    record SubmittedJob(String jobId, ClientJobPlugin plugin, boolean activeAfterSend) {
    }
}
