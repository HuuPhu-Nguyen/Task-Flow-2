package server.plugins.conversion;

import conversion.model.FilePayload;
import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;
import plugin.PluginContractTest;
import plugin.RetrySafety;
import protocol.JobSubmitMessage;
import server.job.TaskUnit;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

abstract class ConversionPluginContractBinding extends PluginContractTest {
    @Override
    protected RetrySafety expectedRetrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    protected JobSubmitMessage validSubmission() {
        return submit(
                "job-" + taskType() + "-contract",
                List.<Object>of(
                        inlinePayload("first." + inputExtension(), "first"),
                        inlinePayload("second." + inputExtension(), "second")
                )
        );
    }

    @Override
    protected JobSubmitMessage invalidSubmission() {
        ObjectReference reference = reference(
                "invalid-" + taskType(),
                7L,
                inputContentType()
        );
        return submit(
                "job-" + taskType() + "-invalid",
                List.<Object>of(new FilePayload(
                        "ambiguous." + inputExtension(),
                        encoded("payload"),
                        reference
                ))
        );
    }

    @Override
    protected Object validResultFor(TaskUnit<?> task) {
        String resultKey = task.getAssignmentIdentity()
                .map(identity -> TaskFlowObjectKeys.attemptOutputKey(
                        task.getJobId(),
                        task.getTaskId(),
                        identity.attemptNumber(),
                        identity.assignmentId()
                ))
                .orElseGet(() -> TaskFlowObjectKeys.objectKey(
                        "plugin-contract",
                        "result-" + task.getTaskId()
                ));
        return new FilePayload(
                task.getTaskId() + "." + targetFormat(),
                null,
                new ObjectReference(
                        resultKey,
                        7L,
                        "0".repeat(64),
                        resultContentType()
                )
        );
    }

    @Override
    protected Object invalidResult() {
        return new FilePayload(
                "ambiguous." + targetFormat(),
                encoded("payload"),
                reference("invalid-result-" + taskType(), 7L, resultContentType())
        );
    }

    @Override
    protected List<Object> invalidResults() {
        return List.of(
                invalidResult(),
                Map.of(),
                new FilePayload("wrong-extension.bin", encoded("payload")),
                new FilePayload("invalid." + targetFormat(), "not-base64"),
                new FilePayload(
                        "empty." + targetFormat(),
                        null,
                        reference("empty-result-" + taskType(), 0L, resultContentType())
                )
        );
    }

    @Override
    protected String serverModulePom() {
        return "plugins/conversion/server/pom.xml";
    }

    @Override
    protected String peerArtifactId() {
        return "taskflow-plugin-conversion-peer";
    }

    @Override
    protected List<String> peerOnlyDependencyArtifactIds() {
        return List.of("pdfbox", "javacv-platform");
    }

    @Override
    protected JobSubmitMessage objectReferenceSubmission() {
        return submit(
                "job-" + taskType() + "-references",
                List.<Object>of(
                        new FilePayload(
                                "first." + inputExtension(),
                                null,
                                reference("input-first-" + taskType(), 7L, inputContentType())
                        ),
                        new FilePayload(
                                "second." + inputExtension(),
                                null,
                                reference("input-second-" + taskType(), 8L, inputContentType())
                        )
                )
        );
    }

    protected abstract String taskType();

    protected abstract String targetFormat();

    protected abstract String inputExtension();

    protected abstract String inputContentType();

    protected abstract String resultContentType();

    protected String submissionParameter() {
        return targetFormat();
    }

    private JobSubmitMessage submit(String jobId, List<Object> payloads) {
        return new JobSubmitMessage(
                "conversion-client",
                Instant.EPOCH.toString(),
                jobId,
                taskType(),
                payloads,
                submissionParameter()
        );
    }

    private static FilePayload inlinePayload(String fileName, String content) {
        return new FilePayload(fileName, encoded(content));
    }

    private static String encoded(String content) {
        return Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    private static ObjectReference reference(String name,
                                             long contentLength,
                                             String contentType) {
        return new ObjectReference(
                TaskFlowObjectKeys.objectKey("plugin-contract", name),
                contentLength,
                "0".repeat(64),
                contentType
        );
    }
}
