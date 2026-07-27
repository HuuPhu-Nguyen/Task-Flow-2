package server.plugins.text;

import peer.engine.PeerProcessorPlugin;
import peer.plugins.text.TextAnalysisProcessorPlugin;
import plugin.PluginContractTest;
import plugin.RetrySafety;
import protocol.JobSubmitMessage;
import server.job.TaskPlugin;
import server.job.TaskUnit;
import text.model.TextAnalysisPayload;
import text.model.TextAnalysisResult;
import text.model.TextAnalysisTaskTypes;

import java.time.Instant;
import java.util.List;
import java.util.Map;

class TextAnalysisPluginContractTest extends PluginContractTest {
    @Override
    protected TaskPlugin taskPlugin() {
        return new TextAnalysisTaskPlugin();
    }

    @Override
    protected PeerProcessorPlugin peerPlugin() {
        return new TextAnalysisProcessorPlugin();
    }

    @Override
    protected RetrySafety expectedRetrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    protected JobSubmitMessage validSubmission() {
        return submit(
                "job-text-contract",
                List.<Object>of(
                        new TextAnalysisPayload("first.txt", "one two"),
                        new TextAnalysisPayload("second.txt", "three\nfour five")
                )
        );
    }

    @Override
    protected JobSubmitMessage invalidSubmission() {
        return submit(
                "job-text-invalid",
                List.<Object>of(new TextAnalysisPayload("missing.txt", null))
        );
    }

    @Override
    protected Object validResultFor(TaskUnit<?> task) {
        TextAnalysisPayload payload = (TextAnalysisPayload) task.getPayload();
        String text = payload.text();
        int lineCount = text.isEmpty() ? 0 : text.split("\\R", -1).length;
        String trimmed = text.trim();
        int wordCount = trimmed.isEmpty() ? 0 : trimmed.split("\\s+").length;
        int uniqueWordCount = (int) java.util.Arrays.stream(
                        trimmed.isEmpty() ? new String[0] : trimmed.split("\\s+")
                )
                .map(String::toLowerCase)
                .distinct()
                .count();
        return new TextAnalysisResult(
                payload.documentName(),
                lineCount,
                wordCount,
                text.length(),
                uniqueWordCount
        );
    }

    @Override
    protected Object invalidResult() {
        return Map.of(
                "documentName", "invalid.txt",
                "lineCount", 1,
                "wordCount", -1,
                "characterCount", 4,
                "uniqueWordCount", 1
        );
    }

    @Override
    protected String serverModulePom() {
        return "plugins/text/server/pom.xml";
    }

    @Override
    protected String peerArtifactId() {
        return "taskflow-plugin-text-peer";
    }

    private static JobSubmitMessage submit(String jobId, List<Object> payloads) {
        return new JobSubmitMessage(
                "text-client",
                Instant.EPOCH.toString(),
                jobId,
                TextAnalysisTaskTypes.TEXT_ANALYSIS,
                payloads,
                "csv"
        );
    }
}
