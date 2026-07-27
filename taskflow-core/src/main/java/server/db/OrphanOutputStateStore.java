package server.db;

import objectstore.TaskFlowObjectKeys;

import java.util.List;

/**
 * SQLite-owned authority and retry state used by orphan-output collection.
 */
public interface OrphanOutputStateStore {
    int MAX_FAILURE_BATCH_SIZE = 1_000;

    enum AttemptOutputClassification {
        ACTIVE,
        AUTHORITATIVE,
        ORPHAN_CANDIDATE,
        STORAGE_FAILURE
    }

    enum MutationOutcome {
        COMMITTED,
        ALREADY_APPLIED,
        STORAGE_FAILURE
    }

    enum LoadOutcome {
        LOADED,
        STORAGE_FAILURE
    }

    record DeletionFailure(
            String objectKey,
            long firstFailedAt,
            long lastAttemptAt,
            int attemptCount,
            String lastError
    ) {
        public DeletionFailure {
            objectKey = TaskFlowObjectKeys.requireObjectKey(objectKey);
            firstFailedAt = Math.max(0L, firstFailedAt);
            lastAttemptAt = Math.max(firstFailedAt, lastAttemptAt);
            if (attemptCount < 1) {
                throw new IllegalArgumentException("attemptCount must be positive.");
            }
            lastError = lastError == null ? "" : lastError;
        }
    }

    record DeletionFailureBatch(
            LoadOutcome outcome,
            List<DeletionFailure> failures
    ) {
        public DeletionFailureBatch {
            outcome = outcome == null ? LoadOutcome.STORAGE_FAILURE : outcome;
            failures = failures == null ? List.of() : List.copyOf(failures);
            if (outcome == LoadOutcome.STORAGE_FAILURE && !failures.isEmpty()) {
                throw new IllegalArgumentException(
                        "A failed deletion-retry load cannot contain rows."
                );
            }
        }

        public static DeletionFailureBatch loaded(List<DeletionFailure> failures) {
            return new DeletionFailureBatch(LoadOutcome.LOADED, failures);
        }

        public static DeletionFailureBatch storageFailure() {
            return new DeletionFailureBatch(LoadOutcome.STORAGE_FAILURE, List.of());
        }
    }

    AttemptOutputClassification classifyAttemptOutput(
            TaskFlowObjectKeys.AttemptOutputIdentity identity
    );

    DeletionFailureBatch loadOrphanOutputDeletionFailures(int limit);

    MutationOutcome recordOrphanOutputDeletionFailure(
            String objectKey,
            long failedAt,
            String error
    );

    MutationOutcome clearOrphanOutputDeletionFailure(String objectKey);

    static int requireFailureBatchLimit(int limit) {
        if (limit < 1 || limit > MAX_FAILURE_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "Failure batch limit must be between 1 and "
                            + MAX_FAILURE_BATCH_SIZE + "."
            );
        }
        return limit;
    }
}
