package plugin;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import objectstore.ObjectReference;
import org.junit.jupiter.api.Test;
import peer.engine.PeerProcessorPlugin;
import protocol.JobSubmitMessage;
import protocol.PayloadLimits;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;
import server.job.TaskUnit;
import server.runtime.TaskFlowClock;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reusable black-box contract for one role-split task plugin family.
 *
 * <p>Bindings supply only domain samples and concrete server/executor
 * providers. The behavioral methods are inherited unchanged by every plugin
 * family.</p>
 */
public abstract class PluginContractTest {
    private static final Gson GSON = new Gson();
    private static final String CONTRACT_PEER_ID = "plugin-contract-peer";
    private static final TaskFlowClock FIXED_CLOCK = new TaskFlowClock() {
        @Override
        public Instant now() {
            return Instant.EPOCH;
        }

        @Override
        public long nowEpochMillis() {
            return 0L;
        }
    };

    protected abstract TaskPlugin taskPlugin();

    protected abstract PeerProcessorPlugin peerPlugin();

    protected abstract RetrySafety expectedRetrySafety();

    /**
     * Returns a fresh submission with at least two tasks on every invocation.
     */
    protected abstract JobSubmitMessage validSubmission();

    protected abstract JobSubmitMessage invalidSubmission();

    protected abstract Object validResultFor(TaskUnit<?> task);

    protected abstract Object invalidResult();

    protected List<Object> invalidResults() {
        return List.of(invalidResult(), Map.of());
    }

    protected abstract String serverModulePom();

    protected abstract String peerArtifactId();

    protected List<String> peerOnlyDependencyArtifactIds() {
        return List.of();
    }

    /**
     * Returns a fresh object-reference submission when the family supports
     * referenced payloads, or {@code null} when references are not applicable.
     */
    protected JobSubmitMessage objectReferenceSubmission() {
        return null;
    }

    @Test
    public final void identicalInputProducesDeterministicTasksWithStableIdentifiers() {
        Map<String, JsonElement> first = canonicalTasks(initializedJob(validSubmission()));
        Map<String, JsonElement> second = canonicalTasks(initializedJob(validSubmission()));

        assertFalse(first.isEmpty(), "A valid contract submission must split into tasks.");
        assertEquals(first, second,
                "Identical input must produce the same task identifiers and payload snapshots.");
        assertEquals(first.size(), new LinkedHashSet<>(first.keySet()).size(),
                "Task identifiers must be unique.");
        assertTrue(first.keySet().stream().noneMatch(taskId -> taskId == null || taskId.isBlank()),
                "Task identifiers must be non-blank.");
    }

    @Test
    public final void payloadValidationAcceptsValidAndRejectsInvalidSubmissions() {
        TaskPlugin plugin = taskPlugin();

        assertDoesNotThrow(() -> plugin.validateSubmission(validSubmission()));
        assertThrows(IllegalArgumentException.class,
                () -> plugin.validateSubmission(invalidSubmission()));
    }

    @Test
    public final void pairedRolesDeclareTheSameRetrySafetyAndResourceProfile() {
        TaskPlugin server = taskPlugin();
        PeerProcessorPlugin executor = peerPlugin();

        assertEquals(server.taskType(), executor.taskType());
        assertEquals(expectedRetrySafety(), server.retrySafety());
        assertEquals(server.retrySafety(), executor.retrySafety());
        assertNotNull(server.resourceProfile());
        assertEquals(server.resourceProfile(), executor.resourceProfile());
        assertNotNull(executor.createProcessor());
    }

    @Test
    public final void resultValidationAcceptsValidAndRejectsMalformedResults() {
        EmbarrassinglyParallelJob<?, ?> job = initializedJob(validSubmission());
        TaskUnit<?> task = orderedTasks(job).getFirst();
        assertTrue(task.markAssigned(CONTRACT_PEER_ID, 0L));

        assertDoesNotThrow(() -> job.prepareTaskResult(validResultFor(task)));
        for (Object invalid : invalidResults()) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> job.prepareTaskResult(invalid)
            );
        }
    }

    @Test
    public final void aggregationIsDeterministicForTheSameCommittedResults() {
        JobSubmitMessage firstSubmission = validSubmission();
        JobSubmitMessage secondSubmission = validSubmission();
        EmbarrassinglyParallelJob<?, ?> first = initializedJob(firstSubmission);
        EmbarrassinglyParallelJob<?, ?> second = initializedJob(secondSubmission);

        List<String> forward = new ArrayList<>(first.getTasks().keySet());
        Collections.sort(forward);
        List<String> reverse = new ArrayList<>(forward);
        Collections.reverse(reverse);

        assignInOrder(first, forward);
        assignInOrder(second, forward);
        commitInOrder(first, forward);
        commitInOrder(second, reverse);

        assertTrue(first.isJobComplete());
        assertTrue(second.isJobComplete());
        assertEquals(
                GSON.toJsonTree(first.aggregateResultPayload()),
                GSON.toJsonTree(second.aggregateResultPayload()),
                "Aggregation must depend on committed results, not result arrival order."
        );
    }

    @Test
    public final void coordinatorRoleOmitsExecutorAndPeerOnlyDependencies() throws Exception {
        Set<String> forbiddenArtifacts = new LinkedHashSet<>();
        forbiddenArtifacts.add(peerArtifactId());
        forbiddenArtifacts.addAll(peerOnlyDependencyArtifactIds());

        assertNoProductionDependency(repoRoot().resolve(serverModulePom()), forbiddenArtifacts);
        assertNoProductionDependency(
                repoRoot().resolve("taskflow-coordinator/pom.xml"),
                forbiddenArtifacts
        );
    }

    @Test
    public final void objectReferencesRoundTripWhereRelevant() {
        JobSubmitMessage referenced = objectReferenceSubmission();
        if (referenced == null) {
            assertTrue(PayloadLimits.objectReferences(validSubmission().getTaskPayloads()).isEmpty(),
                    "A non-reference binding must use inline/model payloads.");
            return;
        }

        taskPlugin().validateSubmission(referenced);
        List<ObjectReference> submittedReferences =
                PayloadLimits.objectReferences(referenced.getTaskPayloads());
        assertFalse(submittedReferences.isEmpty(),
                "A reference-capable binding must supply referenced payloads.");

        EmbarrassinglyParallelJob<?, ?> job = initializedJob(referenced);
        List<ObjectReference> taskReferences = orderedTasks(job).stream()
                .flatMap(task -> PayloadLimits.objectReferences(task.getPayload()).stream())
                .toList();
        assertEquals(submittedReferences, taskReferences,
                "Task splitting must preserve portable object-reference metadata.");

        TaskUnit<?> firstTask = orderedTasks(job).getFirst();
        assertTrue(firstTask.markAssigned(CONTRACT_PEER_ID, 0L));
        Object preparedResult = job.prepareTaskResult(validResultFor(firstTask)).resultData();
        assertFalse(PayloadLimits.objectReferences(preparedResult).isEmpty(),
                "Reference-capable plugin results must preserve portable references.");
    }

    private EmbarrassinglyParallelJob<?, ?> initializedJob(JobSubmitMessage submission) {
        TaskPlugin plugin = taskPlugin();
        plugin.validateSubmission(submission);
        EmbarrassinglyParallelJob<?, ?> job =
                plugin.createJob(submission, submission.getNodeId());
        job.initializeTasks(submission);
        AtomicInteger assignmentSequence = new AtomicInteger();
        job.configureTransitionPorts(
                FIXED_CLOCK,
                () -> "00000000-0000-0000-0000-%012d"
                        .formatted(assignmentSequence.incrementAndGet())
        );
        return job;
    }

    private static void assignInOrder(EmbarrassinglyParallelJob<?, ?> job,
                                      List<String> taskIds) {
        for (String taskId : taskIds) {
            TaskUnit<?> task = job.getTasks().get(taskId);
            assertNotNull(task);
            assertTrue(task.markAssigned(CONTRACT_PEER_ID, 0L));
        }
    }

    private void commitInOrder(EmbarrassinglyParallelJob<?, ?> job, List<String> taskIds) {
        for (String taskId : taskIds) {
            TaskUnit<?> task = job.getTasks().get(taskId);
            assertNotNull(task);
            assertTrue(job.recordResult(
                    taskId,
                    CONTRACT_PEER_ID,
                    validResultFor(task)
            ).accepted());
        }
    }

    private static List<TaskUnit<?>> orderedTasks(EmbarrassinglyParallelJob<?, ?> job) {
        List<TaskUnit<?>> ordered = new ArrayList<>();
        ordered.addAll(job.getTasks().values());
        ordered.sort((left, right) -> left.getTaskId().compareTo(right.getTaskId()));
        return List.copyOf(ordered);
    }

    private static Map<String, JsonElement> canonicalTasks(EmbarrassinglyParallelJob<?, ?> job) {
        Map<String, JsonElement> canonical = new TreeMap<>();
        job.getTasks().forEach((taskId, task) ->
                canonical.put(taskId, GSON.toJsonTree(task.getPayload())));
        return canonical;
    }

    private static void assertNoProductionDependency(Path pom,
                                                     Set<String> forbiddenArtifacts) throws Exception {
        assertTrue(Files.isRegularFile(pom), "Missing role POM: " + pom);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        var document = factory.newDocumentBuilder().parse(pom.toFile());
        var dependencies = document.getElementsByTagName("dependency");

        for (int index = 0; index < dependencies.getLength(); index++) {
            var dependency = dependencies.item(index);
            String artifactId = childText(dependency, "artifactId");
            if (!forbiddenArtifacts.contains(artifactId)) {
                continue;
            }
            String scope = childText(dependency, "scope");
            assertEquals("test", scope,
                    pom + " must not carry executor-only dependency " + artifactId
                            + " on its production classpath.");
        }
    }

    private static String childText(org.w3c.dom.Node parent, String childName) {
        for (int index = 0; index < parent.getChildNodes().getLength(); index++) {
            org.w3c.dom.Node child = parent.getChildNodes().item(index);
            if (childName.equals(child.getNodeName())) {
                return child.getTextContent().trim();
            }
        }
        return "";
    }

    private static Path repoRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            if (Files.exists(current.resolve("mvnw.cmd"))
                    && Files.exists(current.resolve("pom.xml"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException(
                "Could not find repository root from " + System.getProperty("user.dir")
        );
    }
}
