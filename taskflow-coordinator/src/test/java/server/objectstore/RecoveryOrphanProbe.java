package server.objectstore;

import objectstore.ObjectStore;
import objectstore.TaskFlowObjectKeys;
import server.db.DatabaseManager;
import server.runtime.TaskFlowClock;

/**
 * Test-source access to the bounded production GC batch for TF-0708.
 */
public final class RecoveryOrphanProbe {
    private RecoveryOrphanProbe() {
    }

    public static CleanupResult clean(
            ObjectStore objectStore,
            DatabaseManager database,
            TaskFlowClock clock,
            int expectedObjects,
            int batchSize
    ) throws Exception {
        int batches = 0;
        int deleted = 0;
        int examined = 0;
        int maximumExamined = 0;
        long startedAt = System.nanoTime();
        try (OrphanOutputGc gc = new OrphanOutputGc(
                objectStore,
                database,
                new OrphanOutputGcConfig(true, 1L, 60_000L, batchSize),
                clock
        )) {
            while (deleted < expectedObjects) {
                OrphanOutputGc.BatchResult batch = gc.runBatch();
                batches++;
                deleted += batch.deleted();
                examined += batch.examined();
                maximumExamined = Math.max(
                        maximumExamined,
                        batch.examined()
                );
                if (batch.storeUnavailable()
                        || batch.failed() != 0
                        || batch.examined() > batchSize
                        || batches > expectedObjects + 1) {
                    throw new IllegalStateException(
                            "Invalid orphan cleanup batch: " + batch
                    );
                }
                if (batch.examined() == 0 && deleted < expectedObjects) {
                    throw new IllegalStateException(
                            "Orphan cleanup stopped before deleting all objects."
                    );
                }
            }
            long duration = System.nanoTime() - startedAt;
            if (deleted != expectedObjects
                    || gc.metricsSnapshot().orphanOutputsTotal()
                    != expectedObjects
                    || !objectStore.list(
                    TaskFlowObjectKeys.prefix("jobs"),
                    1
            ).objects().isEmpty()) {
                throw new IllegalStateException(
                        "Orphan cleanup postcondition did not hold."
                );
            }
            return new CleanupResult(
                    duration,
                    deleted,
                    examined,
                    batches,
                    maximumExamined
            );
        }
    }

    public record CleanupResult(
            long durationNanos,
            int deletedObjects,
            int examinedObjects,
            int batches,
            int maximumExaminedInBatch
    ) {
        public CleanupResult {
            if (durationNanos <= 0L
                    || deletedObjects < 1
                    || examinedObjects < deletedObjects
                    || batches < 1
                    || maximumExaminedInBatch < 1) {
                throw new IllegalArgumentException(
                        "Orphan cleanup measurements are invalid."
                );
            }
        }
    }
}
