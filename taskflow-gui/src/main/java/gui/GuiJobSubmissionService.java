package gui;

import client.ClientJobPlugin;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

final class GuiJobSubmissionService {
    private final JobSubmissionClient jobSubmissionClient;
    private final Set<String> activeJobIds;

    GuiJobSubmissionService(JobSubmissionClient jobSubmissionClient, Set<String> activeJobIds) {
        this.jobSubmissionClient = jobSubmissionClient;
        this.activeJobIds = activeJobIds;
    }

    GuiJobSubmitter.SubmittedJob submit(
            ClientJobPlugin plugin,
            List<Path> inputPaths,
            String targetFormat,
            PrintWriter out,
            BooleanSupplier connectionCurrent,
            Runnable onSendFailure,
            BooleanSupplier cancelled,
            Runnable beforeSubmit) throws Exception {
        List<Object> payloads = plugin.buildPayloads(inputPaths, targetFormat);
        if (cancelled.getAsBoolean()) {
            return null;
        }
        beforeSubmit.run();
        return GuiJobSubmitter.submitPreparedPayloads(
                jobSubmissionClient,
                plugin,
                payloads,
                targetFormat,
                out,
                connectionCurrent,
                onSendFailure,
                activeJobIds);
    }
}
