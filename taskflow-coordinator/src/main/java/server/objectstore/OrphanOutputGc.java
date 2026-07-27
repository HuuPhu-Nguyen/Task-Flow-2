package server.objectstore;

import objectstore.ObjectListing;
import objectstore.ObjectMetadata;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.TaskFlowObjectKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.db.OrphanOutputStateStore;
import server.runtime.TaskFlowClock;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically removes old, non-authoritative attempt outputs in bounded
 * batches.
 */
public final class OrphanOutputGc implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrphanOutputGc.class);
    private static final String OUTPUT_SCAN_PREFIX = TaskFlowObjectKeys.prefix("jobs");
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 2_000L;

    private final ObjectStore objectStore;
    private final OrphanOutputStateStore stateStore;
    private final OrphanOutputGcConfig config;
    private final TaskFlowClock clock;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private String scanStartAfter;

    public OrphanOutputGc(
            ObjectStore objectStore,
            OrphanOutputStateStore stateStore,
            OrphanOutputGcConfig config,
            TaskFlowClock clock
    ) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "orphan-output-gc");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.scheduleWithFixedDelay(
                    this::runBatchSafely,
                    0L,
                    config.intervalMillis(),
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException e) {
            if (!executor.isShutdown()) {
                throw e;
            }
        }
    }

    BatchResult runBatch() {
        long now = clock.nowEpochMillis();
        long safetyCutoff = now >= config.safetyWindowMillis()
                ? now - config.safetyWindowMillis()
                : -1L;
        int retryBudget = Math.max(1, config.batchSize() / 2);
        OrphanOutputStateStore.DeletionFailureBatch failureBatch =
                stateStore.loadOrphanOutputDeletionFailures(retryBudget);
        if (failureBatch.outcome() == OrphanOutputStateStore.LoadOutcome.STORAGE_FAILURE) {
            LOGGER.warn("event=orphan_output_gc_deferred reason=retry_state_unavailable");
            return BatchResult.storageDeferred();
        }

        MutableBatchResult result = new MutableBatchResult();
        Set<String> processedKeys = new HashSet<>();
        for (OrphanOutputStateStore.DeletionFailure failure : failureBatch.failures()) {
            result.examined++;
            processedKeys.add(failure.objectKey());
            if (!processCandidate(failure.objectKey(), result) && result.storeUnavailable) {
                return finish(result);
            }
        }

        int discoveryBudget = config.batchSize() - result.examined;
        if (discoveryBudget > 0) {
            discover(safetyCutoff, discoveryBudget, processedKeys, result);
        }
        return finish(result);
    }

    private void discover(
            long safetyCutoff,
            int discoveryBudget,
            Set<String> processedKeys,
            MutableBatchResult result
    ) {
        ObjectListing listing;
        try {
            listing = objectStore.list(OUTPUT_SCAN_PREFIX, scanStartAfter, discoveryBudget);
        } catch (ObjectStoreException e) {
            result.storeUnavailable = true;
            result.failed++;
            LOGGER.warn(
                    "event=orphan_output_gc_deferred reason=object_store_list_failed "
                            + "failure_reason={} error={}",
                    e.reason(),
                    e.getMessage()
            );
            return;
        }

        if (listing.objects().isEmpty()) {
            scanStartAfter = null;
            return;
        }
        scanStartAfter = listing.nextStartAfter();
        for (ObjectMetadata metadata : listing.objects()) {
            result.examined++;
            if (processedKeys.contains(metadata.key())
                    || metadata.lastModifiedAtEpochMillis() > safetyCutoff) {
                continue;
            }
            Optional<TaskFlowObjectKeys.AttemptOutputIdentity> identity =
                    TaskFlowObjectKeys.parseAttemptOutputKey(metadata.key());
            if (identity.isEmpty()) {
                continue;
            }
            if (!processCandidate(identity.get().key(), result) && result.storeUnavailable) {
                return;
            }
        }
    }

    private boolean processCandidate(String objectKey, MutableBatchResult result) {
        Optional<TaskFlowObjectKeys.AttemptOutputIdentity> parsed =
                TaskFlowObjectKeys.parseAttemptOutputKey(objectKey);
        if (parsed.isEmpty()) {
            clearFailure(objectKey);
            result.preserved++;
            return true;
        }

        OrphanOutputStateStore.AttemptOutputClassification classification =
                stateStore.classifyAttemptOutput(parsed.get());
        switch (classification) {
            case ACTIVE -> {
                clearFailure(objectKey);
                result.active++;
                LOGGER.debug(
                        "event=orphan_output_preserved object_key={} classification=active",
                        objectKey
                );
                return true;
            }
            case AUTHORITATIVE -> {
                clearFailure(objectKey);
                result.authoritative++;
                LOGGER.debug(
                        "event=orphan_output_preserved object_key={} classification=authoritative",
                        objectKey
                );
                return true;
            }
            case STORAGE_FAILURE -> {
                result.failed++;
                OrphanOutputStateStore.MutationOutcome recorded =
                        stateStore.recordOrphanOutputDeletionFailure(
                                objectKey,
                                clock.nowEpochMillis(),
                                "authority_unavailable"
                        );
                LOGGER.warn(
                        "event=orphan_output_gc_deferred object_key={} "
                                + "reason=authority_unavailable retry_recorded={}",
                        objectKey,
                        recorded != OrphanOutputStateStore.MutationOutcome.STORAGE_FAILURE
                );
                return true;
            }
            case ORPHAN_CANDIDATE -> {
                return deleteCandidate(objectKey, result);
            }
        }
        throw new IllegalStateException("Unhandled output classification: " + classification);
    }

    private boolean deleteCandidate(String objectKey, MutableBatchResult result) {
        try {
            objectStore.delete(objectKey);
            clearFailure(objectKey);
            result.deleted++;
            LOGGER.info(
                    "event=orphan_output_deleted object_key={} classification=orphan_candidate",
                    objectKey
            );
            return true;
        } catch (ObjectStoreException e) {
            result.failed++;
            OrphanOutputStateStore.MutationOutcome recorded =
                    stateStore.recordOrphanOutputDeletionFailure(
                            objectKey,
                            clock.nowEpochMillis(),
                            e.reason() + ": " + String.valueOf(e.getMessage())
                    );
            if (recorded == OrphanOutputStateStore.MutationOutcome.STORAGE_FAILURE) {
                LOGGER.error(
                        "event=orphan_output_delete_failure_unrecorded object_key={} "
                                + "failure_reason={} error={}",
                        objectKey,
                        e.reason(),
                        e.getMessage()
                );
            } else {
                LOGGER.warn(
                        "event=orphan_output_delete_deferred object_key={} "
                                + "failure_reason={} retry_recorded=true error={}",
                        objectKey,
                        e.reason(),
                        e.getMessage()
                );
            }
            result.storeUnavailable =
                    e.reason() == ObjectStoreException.Reason.STORAGE_FAILURE;
            return false;
        }
    }

    private void clearFailure(String objectKey) {
        OrphanOutputStateStore.MutationOutcome outcome =
                stateStore.clearOrphanOutputDeletionFailure(objectKey);
        if (outcome == OrphanOutputStateStore.MutationOutcome.STORAGE_FAILURE) {
            LOGGER.warn(
                    "event=orphan_output_gc_retry_clear_failed object_key={}",
                    objectKey
            );
        }
    }

    private BatchResult finish(MutableBatchResult result) {
        BatchResult completed = result.snapshot();
        if (completed.examined() > 0 || completed.failed() > 0) {
            LOGGER.info(
                    "event=orphan_output_gc_batch examined={} deleted={} active={} "
                            + "authoritative={} preserved={} failed={} "
                            + "store_unavailable={} batch_limit={}",
                    completed.examined(),
                    completed.deleted(),
                    completed.active(),
                    completed.authoritative(),
                    completed.preserved(),
                    completed.failed(),
                    completed.storeUnavailable(),
                    config.batchSize()
            );
        }
        return completed;
    }

    private void runBatchSafely() {
        try {
            runBatch();
        } catch (RuntimeException e) {
            LOGGER.warn(
                    "event=orphan_output_gc_deferred reason=unexpected_failure error={}",
                    e.getMessage(),
                    e
            );
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        executor.shutdownNow();
        boolean stopped;
        try {
            stopped = executor.awaitTermination(
                    SHUTDOWN_TIMEOUT_MILLIS,
                    TimeUnit.MILLISECONDS
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopped = false;
        }
        try {
            objectStore.close();
        } finally {
            if (!stopped) {
                throw new IllegalStateException(
                        "Orphan-output collector did not stop within the shutdown bound."
                );
            }
        }
    }

    record BatchResult(
            int examined,
            int deleted,
            int active,
            int authoritative,
            int preserved,
            int failed,
            boolean storeUnavailable
    ) {
        static BatchResult storageDeferred() {
            return new BatchResult(0, 0, 0, 0, 0, 1, false);
        }
    }

    private static final class MutableBatchResult {
        private int examined;
        private int deleted;
        private int active;
        private int authoritative;
        private int preserved;
        private int failed;
        private boolean storeUnavailable;

        private BatchResult snapshot() {
            return new BatchResult(
                    examined,
                    deleted,
                    active,
                    authoritative,
                    preserved,
                    failed,
                    storeUnavailable
            );
        }
    }
}
