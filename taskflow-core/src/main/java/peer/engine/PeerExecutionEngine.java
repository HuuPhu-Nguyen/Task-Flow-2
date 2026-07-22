package peer.engine;

import com.google.gson.Gson;
import messaging.SafeJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugin.RetrySafety;
import protocol.MessageValidator;
import protocol.PeerIdentity;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.io.PrintWriter;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.ServiceLoader;
import java.util.concurrent.*;
import java.util.function.LongSupplier;

public class PeerExecutionEngine implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeerExecutionEngine.class);

    private final ExecutorService executionPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final Gson gson = new Gson();
    private final String nodeId;
    private final Map<String, TaskProcessor<?>> processors = new ConcurrentHashMap<>();
    private final AssignmentCacheConfig assignmentCacheConfig;
    private final AssignmentResultCache assignmentCache;

    public PeerExecutionEngine(String nodeId) {
        this(nodeId, AssignmentCacheConfig.fromEnvironment());
    }

    public PeerExecutionEngine(String nodeId, AssignmentCacheConfig assignmentCacheConfig) {
        this(nodeId, assignmentCacheConfig, System::currentTimeMillis);
    }

    PeerExecutionEngine(String nodeId,
                        AssignmentCacheConfig assignmentCacheConfig,
                        LongSupplier clock) {
        this.nodeId = PeerIdentity.require(nodeId);
        this.assignmentCacheConfig = Objects.requireNonNull(
                assignmentCacheConfig,
                "assignmentCacheConfig"
        );
        this.assignmentCache = new AssignmentResultCache(
                this.nodeId,
                assignmentCacheConfig,
                Objects.requireNonNull(clock, "clock")
        );
        registerDiscoveredProcessors();
    }

    public String nodeId() {
        return nodeId;
    }

    public void registerProcessor(String type, TaskProcessor<?> processor) {
        String taskType = normalize(type);
        TaskProcessor<?> existing = processors.putIfAbsent(taskType, processor);
        if (existing != null) {
            throw new IllegalStateException("Duplicate peer processor for task type " + taskType);
        }
    }

    public void registerDiscoveredProcessors() {
        for (PeerProcessorPlugin plugin : ServiceLoader.load(PeerProcessorPlugin.class)) {
            String taskType = plugin.taskType();
            RetrySafety retrySafety = Objects.requireNonNull(
                    plugin.retrySafety(),
                    "Peer processor plugin " + plugin.getClass().getName() + " must declare retry safety."
            );
            registerProcessor(taskType, plugin.createProcessor());
            LOGGER.info(
                    "event=peer_processor_registered task_type={} retry_safety={} plugin_class={}",
                    normalize(taskType),
                    retrySafety,
                    plugin.getClass().getName()
            );
        }
    }

    public Set<String> getRegisteredTaskTypes() {
        return Set.copyOf(processors.keySet());
    }

    public AssignmentCacheConfig assignmentCacheConfig() {
        return assignmentCacheConfig;
    }

    public AssignmentCacheSnapshot assignmentCacheSnapshot() {
        return assignmentCache.snapshot();
    }

    public void shutdown() {
        assignmentCache.clear();
        executionPool.shutdownNow();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executionPool.awaitTermination(timeout, unit);
    }

    public boolean isShutdown() {
        return executionPool.isShutdown();
    }

    @Override
    public void close() {
        shutdown();
    }

    public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
        return executeAssignment(task).resultFuture();
    }

    public AssignmentExecution executeAssignment(TaskAssignMessage task) {
        MessageValidator.validate(task);
        AssignmentResultCache.Claim claim = assignmentCache.claim(task);
        AssignmentExecution execution = new AssignmentExecution(claim.disposition(), claim.resultFuture());
        logCacheDecision(task, execution.disposition());
        if (execution.disposition() != AssignmentExecution.Disposition.STARTED) {
            return execution;
        }

        CompletableFuture<TaskResultMessage> processorFuture;
        try {
            processorFuture = CompletableFuture.supplyAsync(() -> executeUncached(task), executionPool);
        } catch (RuntimeException e) {
            assignmentCache.invalidate(task, execution.resultFuture());
            execution.resultFuture().completeExceptionally(e);
            throw e;
        }
        processorFuture.whenComplete((result, failure) -> {
            if (failure != null) {
                assignmentCache.invalidate(task, execution.resultFuture());
                execution.resultFuture().completeExceptionally(failure);
                return;
            }
            try {
                assignmentCache.markCompleted(task, execution.resultFuture(), result);
                execution.resultFuture().complete(result);
            } catch (RuntimeException e) {
                assignmentCache.invalidate(task, execution.resultFuture());
                execution.resultFuture().completeExceptionally(e);
            }
        });
        return execution;
    }

    private TaskResultMessage executeUncached(TaskAssignMessage task) {
        try {
            String taskType = task.getTaskType();
            TaskProcessor<?> processor = processors.get(normalize(taskType));
            if (processor == null) {
                throw new RuntimeException("No processor: " + taskType);
            }

            Object result = process(processor, task);
            return new TaskResultMessage(
                    nodeId,
                    java.time.Instant.now().toString(),
                    task.getTaskId(),
                    task.getJobId(),
                    task.getAttemptNumber(),
                    task.getAssignmentId(),
                    result,
                    true,
                    null
            );
        } catch (Exception e) {
            return new TaskResultMessage(
                    nodeId,
                    java.time.Instant.now().toString(),
                    task.getTaskId(),
                    task.getJobId(),
                    task.getAttemptNumber(),
                    task.getAssignmentId(),
                    null,
                    false,
                    e.getMessage()
            );
        }
    }

    public CompletableFuture<Boolean> submitTask(TaskAssignMessage task, PrintWriter out) {
        AssignmentExecution execution;
        try {
            execution = executeAssignment(task);
        } catch (RuntimeException error) {
            LOGGER.warn("event=task_result_send_failed node_id={} job_id={} task_id={} error={}",
                    nodeId, task.getJobId(), task.getTaskId(), error.getMessage(), error);
            return CompletableFuture.completedFuture(false);
        }
        if (execution.disposition() == AssignmentExecution.Disposition.DUPLICATE_RUNNING) {
            return CompletableFuture.completedFuture(true);
        }
        return execution.resultFuture().thenApply(response -> {
            boolean sent = SafeJsonWriter.send(out, gson, response);
            if (!sent) {
                LOGGER.warn("event=task_result_send_failed node_id={} job_id={} task_id={}",
                        nodeId, response.getJobId(), response.getTaskId());
            }
            return sent;
        }).exceptionally(error -> {
            LOGGER.warn("event=task_result_send_failed node_id={} job_id={} task_id={} error={}",
                    nodeId, task.getJobId(), task.getTaskId(), error.getMessage(), error);
            return false;
        });
    }

    private void logCacheDecision(TaskAssignMessage task, AssignmentExecution.Disposition disposition) {
        if (disposition == AssignmentExecution.Disposition.STARTED) {
            return;
        }
        AssignmentCacheSnapshot snapshot = assignmentCache.snapshot();
        LOGGER.info(
                "event=task_assignment_cache_hit peer_id={} task_id={} assignment_id={} disposition={} "
                        + "cache_size={} cache_evictions_total={}",
                nodeId,
                task.getTaskId(),
                task.getAssignmentId(),
                disposition,
                snapshot.size(),
                snapshot.evictionCount()
        );
    }

    private <R> R process(TaskProcessor<R> processor, TaskAssignMessage task) throws Exception {
        return processor.process(task);
    }

    private String normalize(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("Task type is required.");
        }
        return taskType.trim().toUpperCase(Locale.ROOT);
    }
}
