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

        String jobId;
        try {
            jobId = jobSubmissionClient.submitJob(plugin.taskType(), payloads, targetFormat, out);
        } catch (IllegalStateException sendFailure) {
            onSendFailure.run();
            throw sendFailure;
        }

        if (jobId != null) {
            activeJobIds.add(jobId);
        }
        return new SubmittedJob(jobId, plugin);
    }

    record SubmittedJob(String jobId, ClientJobPlugin plugin) {
    }
}
