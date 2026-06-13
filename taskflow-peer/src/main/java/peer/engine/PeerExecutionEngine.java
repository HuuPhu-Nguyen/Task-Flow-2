package peer.engine;

import com.google.gson.Gson;
import messaging.SafeJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.TaskResultMessage;
import protocol.TaskAssignMessage;
import java.io.PrintWriter;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.ServiceLoader;
import java.util.concurrent.*;

public class PeerExecutionEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeerExecutionEngine.class);

    private final ExecutorService executionPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final Gson gson = new Gson();
    private final String nodeId;
    private final Map<String, TaskProcessor<?>> processors = new ConcurrentHashMap<>();

    public PeerExecutionEngine(String nodeId) {
        this.nodeId = nodeId;
        registerDiscoveredProcessors();
    }

    public void registerProcessor(String type, TaskProcessor<?> processor) {
        processors.put(normalize(type), processor);
    }

    public void registerDiscoveredProcessors() {
        for (PeerProcessorPlugin plugin : ServiceLoader.load(PeerProcessorPlugin.class)) {
            registerProcessor(plugin.taskType(), plugin.createProcessor());
        }
    }

    public Set<String> getRegisteredTaskTypes() {
        return Set.copyOf(processors.keySet());
    }

    public void shutdown() {
        executionPool.shutdownNow();
    }

    public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
        return CompletableFuture.supplyAsync(() -> {
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
                        null,
                        false,
                        e.getMessage()
                );
            }
        }, executionPool);
    }

    public CompletableFuture<Boolean> submitTask(TaskAssignMessage task, PrintWriter out) {
        return executeTask(task).thenApply(response -> {
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
