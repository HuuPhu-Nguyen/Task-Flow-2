package peer.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugin.RetrySafety;
import plugin.TaskResourceCatalog;
import plugin.TaskResourceProfile;
import protocol.MessageValidator;
import protocol.PeerIdentity;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.TransientDeliveryException;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.ServiceLoader;
import java.util.concurrent.*;
import java.util.function.LongSupplier;

public class PeerExecutionEngine implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeerExecutionEngine.class);

    private final ExecutorService executionPool;
    private final int executionPoolSize;
    private final String nodeId;
    private final Map<String, TaskProcessor<?>> processors = new ConcurrentHashMap<>();
    private final Map<String, TaskResourceProfile> resourceProfiles = new ConcurrentHashMap<>();
    private final AssignmentCacheConfig assignmentCacheConfig;
    private final AssignmentResultCache assignmentCache;
    private volatile ExecutorCapacityTracker capacityTracker;
    private volatile TaskResourceCatalog resourceCatalog;
    private volatile ExecutorCapacityConfig capacityConfig;
    private volatile Runnable capacityChangeListener = () -> {
    };

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
        this.executionPoolSize = Math.max(1, Runtime.getRuntime().availableProcessors());
        this.executionPool = Executors.newFixedThreadPool(executionPoolSize);
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

    public synchronized void registerProcessor(String type,
                                               TaskResourceProfile resourceProfile,
                                               TaskProcessor<?> processor) {
        if (capacityTracker != null) {
            throw new IllegalStateException(
                    "Peer processor catalog is already frozen for this process."
            );
        }
        String taskType = normalize(type);
        Objects.requireNonNull(resourceProfile, "resourceProfile");
        Objects.requireNonNull(processor, "processor");
        TaskProcessor<?> existing = processors.putIfAbsent(taskType, processor);
        if (existing != null) {
            throw new IllegalStateException("Duplicate peer processor for task type " + taskType);
        }
        resourceProfiles.put(taskType, resourceProfile);
    }

    public void registerDiscoveredProcessors() {
        for (PeerProcessorPlugin plugin : ServiceLoader.load(PeerProcessorPlugin.class)) {
            String taskType = plugin.taskType();
            RetrySafety retrySafety = Objects.requireNonNull(
                    plugin.retrySafety(),
                    "Peer processor plugin " + plugin.getClass().getName() + " must declare retry safety."
            );
            TaskResourceProfile resourceProfile = Objects.requireNonNull(
                    plugin.resourceProfile(),
                    "Peer processor plugin " + plugin.getClass().getName()
                            + " must declare a resource profile."
            );
            registerProcessor(taskType, resourceProfile, plugin.createProcessor());
            LOGGER.info(
                    "event=peer_processor_registered task_type={} retry_safety={} "
                            + "capacity_unit_cost={} plugin_class={}",
                    normalize(taskType),
                    retrySafety,
                    resourceProfile.capacityUnitCost(),
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

    public ExecutorCapacitySnapshot capacitySnapshot() {
        return capacityTracker().snapshot();
    }

    public PongMessage capacityHeartbeat(String timestamp) {
        ExecutorCapacitySnapshot snapshot = capacitySnapshot();
        return new PongMessage(
                nodeId,
                timestamp,
                getRegisteredTaskTypes(),
                snapshot.executorInstanceId(),
                snapshot.sequence(),
                snapshot.totalCapacityUnits(),
                snapshot.availableCapacityUnits(),
                snapshot.maxConcurrencyByTaskType()
        );
    }

    public ExecutorCapacityConfig capacityConfig() {
        capacityTracker();
        return capacityConfig;
    }

    public TaskResourceCatalog resourceCatalog() {
        capacityTracker();
        return resourceCatalog;
    }

    public int executionPoolSize() {
        return executionPoolSize;
    }

    public void onCapacityChanged(Runnable listener) {
        this.capacityChangeListener = Objects.requireNonNull(listener, "listener");
    }

    public void shutdown() {
        assignmentCache.clear();
        executionPool.shutdownNow();
        ExecutorCapacityTracker tracker = capacityTracker;
        if (tracker != null && tracker.clear()) {
            notifyCapacityChanged();
        }
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

        ExecutorCapacityTracker tracker = capacityTracker();
        ExecutorCapacityTracker.ReserveOutcome reserveOutcome;
        try {
            reserveOutcome = tracker.reserve(task);
        } catch (RuntimeException e) {
            assignmentCache.invalidate(task, execution.resultFuture());
            execution.resultFuture().completeExceptionally(e);
            throw e;
        }
        if (reserveOutcome == ExecutorCapacityTracker.ReserveOutcome.IDENTITY_MISMATCH) {
            assignmentCache.invalidate(task, execution.resultFuture());
            AssignmentCacheConflictException conflict = new AssignmentCacheConflictException(
                    "Assignment ID " + task.getAssignmentId()
                            + " conflicts with an executor capacity reservation."
            );
            execution.resultFuture().completeExceptionally(conflict);
            throw conflict;
        }
        if (reserveOutcome == ExecutorCapacityTracker.ReserveOutcome.ALREADY_RESERVED) {
            assignmentCache.invalidate(task, execution.resultFuture());
            TransientDeliveryException stillRunning = new TransientDeliveryException(
                    "assignment_execution_already_running",
                    "Assignment " + task.getAssignmentId()
                            + " is still executing after deduplication-cache eviction.",
                    null
            );
            execution.resultFuture().completeExceptionally(stillRunning);
            throw stillRunning;
        }
        if (reserveOutcome == ExecutorCapacityTracker.ReserveOutcome.RESERVED) {
            logLocalOvercommitIfPresent(task, tracker);
            notifyCapacityChanged();
        }

        CompletableFuture<TaskResultMessage> processorFuture;
        try {
            processorFuture = CompletableFuture.supplyAsync(() -> executeUncached(task), executionPool);
        } catch (RuntimeException e) {
            releaseLocalCapacity(task, tracker);
            assignmentCache.invalidate(task, execution.resultFuture());
            execution.resultFuture().completeExceptionally(e);
            throw e;
        }
        processorFuture.whenComplete((result, failure) -> {
            releaseLocalCapacity(task, tracker);
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

    private synchronized ExecutorCapacityTracker capacityTracker() {
        if (capacityTracker != null) {
            return capacityTracker;
        }
        resourceCatalog = TaskResourceCatalog.capture(resourceProfiles);
        capacityConfig = ExecutorCapacityConfig.fromEnvironment(
                resourceCatalog.taskTypes(),
                executionPoolSize
        );
        capacityTracker = new ExecutorCapacityTracker(capacityConfig, resourceCatalog);
        for (String taskType : resourceCatalog.taskTypes()) {
            int cost = resourceCatalog.require(taskType).capacityUnitCost();
            if (cost > capacityConfig.totalCapacityUnits()) {
                LOGGER.warn(
                        "event=executor_task_cost_exceeds_total_capacity peer_id={} "
                                + "task_type={} capacity_unit_cost={} total_capacity_units={}",
                        nodeId,
                        taskType,
                        cost,
                        capacityConfig.totalCapacityUnits()
                );
            }
        }
        return capacityTracker;
    }

    private void releaseLocalCapacity(TaskAssignMessage task,
                                      ExecutorCapacityTracker tracker) {
        ExecutorCapacityTracker.ReleaseOutcome outcome = tracker.release(task);
        if (outcome == ExecutorCapacityTracker.ReleaseOutcome.IDENTITY_MISMATCH) {
            LOGGER.error(
                    "event=executor_capacity_identity_mismatch peer_id={} task_id={} "
                            + "attempt_number={} assignment_id={} task_type={}",
                    nodeId,
                    task.getTaskId(),
                    task.getAttemptNumber(),
                    task.getAssignmentId(),
                    task.getTaskType()
            );
            return;
        }
        if (outcome == ExecutorCapacityTracker.ReleaseOutcome.RELEASED) {
            notifyCapacityChanged();
        }
    }

    private void logLocalOvercommitIfPresent(TaskAssignMessage task,
                                             ExecutorCapacityTracker tracker) {
        if (!tracker.overcommitted()) {
            return;
        }
        LOGGER.warn(
                "event=executor_capacity_overcommitted peer_id={} task_id={} "
                        + "attempt_number={} assignment_id={} task_type={} "
                        + "reserved_units={} total_capacity_units={}",
                nodeId,
                task.getTaskId(),
                task.getAttemptNumber(),
                task.getAssignmentId(),
                task.getTaskType(),
                tracker.reservedUnits(),
                capacityConfig.totalCapacityUnits()
        );
    }

    private void notifyCapacityChanged() {
        try {
            capacityChangeListener.run();
        } catch (RuntimeException e) {
            LOGGER.warn(
                    "event=executor_capacity_notification_failed peer_id={} error={}",
                    nodeId,
                    e.getMessage(),
                    e
            );
        }
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
