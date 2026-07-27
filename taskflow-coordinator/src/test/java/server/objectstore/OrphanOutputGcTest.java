package server.objectstore;

import objectstore.ObjectListing;
import objectstore.ObjectMetadata;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.db.OrphanOutputStateStore;
import server.runtime.TaskFlowClock;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrphanOutputGcTest {
    private static final String ASSIGNMENT_1 =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String ASSIGNMENT_2 =
            "550e8400-e29b-41d4-a716-446655440001";
    private static final String ASSIGNMENT_3 =
            "550e8400-e29b-41d4-a716-446655440002";

    @TempDir
    Path tempDir;

    @Test
    void uploadedOutputWithoutResultBecomesCollectable() throws Exception {
        MutableClock clock = new MutableClock(1_000L);
        FakeObjectStore objects = new FakeObjectStore(clock);
        String key = outputKey("job-crash", "task-crash", 1, ASSIGNMENT_1);
        objects.putIfAbsent(reference(key), new ByteArrayInputStream(new byte[]{1}));

        try (DatabaseManager database = new DatabaseManager(
                tempDir.resolve("orphan-output-gc.db").toString()
        );
             OrphanOutputGc gc = collector(objects, database, clock, 1_000L, 10)) {
            database.insertJob("job-crash", "TEST_TASK", "requester-1", 1);
            database.insertTask("task-crash", "job-crash");
            assertTrue(database.markTaskAssigned(
                    "task-crash",
                    "peer-1",
                    100L,
                    "lease-1",
                    500L,
                    1,
                    ASSIGNMENT_1
            ));
            assertTrue(database.markTaskRetried(
                    "task-crash",
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "executor_crashed",
                    600L
            ));

            OrphanOutputGc.BatchResult beforeWindow = gc.runBatch();
            assertEquals(0, beforeWindow.deleted());
            assertTrue(objects.contains(key));

            clock.set(2_001L);
            OrphanOutputGc.BatchResult afterWindow = gc.runBatch();
            assertEquals(1, afterWindow.deleted());
            assertFalse(objects.contains(key));
        }
    }

    @Test
    void safetyWindowCutoffDoesNotUnderflowNearEpoch() throws Exception {
        MutableClock clock = new MutableClock(500L);
        FakeObjectStore objects = new FakeObjectStore(clock);
        FakeStateStore state = new FakeStateStore();
        String key = outputKey("job-epoch", "task-epoch", 1, ASSIGNMENT_1);
        objects.seed(reference(key), 0L);
        state.classify(key, OrphanOutputStateStore.AttemptOutputClassification.ORPHAN_CANDIDATE);

        try (OrphanOutputGc gc = collector(objects, state, clock, 1_000L, 10)) {
            assertEquals(0, gc.runBatch().deleted());
            assertTrue(objects.contains(key));

            clock.set(1_000L);
            assertEquals(1, gc.runBatch().deleted());
            assertFalse(objects.contains(key));
        }
    }

    @Test
    void staleAttemptIsDeletedWhileActiveAndAuthoritativeOutputsArePreserved() throws Exception {
        MutableClock clock = new MutableClock(10_000L);
        FakeObjectStore objects = new FakeObjectStore(clock);
        FakeStateStore state = new FakeStateStore();
        String stale = outputKey("job-1", "task-1", 1, ASSIGNMENT_1);
        String active = outputKey("job-1", "task-1", 2, ASSIGNMENT_2);
        String authoritative = outputKey("job-2", "task-2", 1, ASSIGNMENT_3);
        objects.seed(reference(stale), 1_000L);
        objects.seed(reference(active), 1_000L);
        objects.seed(reference(authoritative), 1_000L);
        state.classify(stale, OrphanOutputStateStore.AttemptOutputClassification.ORPHAN_CANDIDATE);
        state.classify(active, OrphanOutputStateStore.AttemptOutputClassification.ACTIVE);
        state.classify(
                authoritative,
                OrphanOutputStateStore.AttemptOutputClassification.AUTHORITATIVE
        );

        try (OrphanOutputGc gc = collector(objects, state, clock, 1_000L, 10)) {
            OrphanOutputGc.BatchResult result = gc.runBatch();

            assertEquals(1, result.deleted());
            assertEquals(1, result.active());
            assertEquals(1, result.authoritative());
            assertEquals(1L, gc.metricsSnapshot().orphanOutputsTotal());
            assertFalse(objects.contains(stale));
            assertTrue(objects.contains(active));
            assertTrue(objects.contains(authoritative));
        }
    }

    @Test
    void repeatedDeletionOfAlreadyMissingObjectClearsDurableRetry() throws Exception {
        MutableClock clock = new MutableClock(10_000L);
        FakeObjectStore objects = new FakeObjectStore(clock);
        FakeStateStore state = new FakeStateStore();
        String missing = outputKey("job-missing", "task-missing", 1, ASSIGNMENT_1);
        state.classify(
                missing,
                OrphanOutputStateStore.AttemptOutputClassification.ORPHAN_CANDIDATE
        );
        state.recordOrphanOutputDeletionFailure(missing, 1_000L, "prior outage");

        try (OrphanOutputGc gc = collector(objects, state, clock, 1_000L, 10)) {
            OrphanOutputGc.BatchResult result = gc.runBatch();

            assertEquals(1, result.deleted());
            assertTrue(state.failures.isEmpty());
            assertEquals(List.of(missing), objects.deleteAttempts);
        }
    }

    @Test
    void objectStoreOutageRecordsFailureAndLaterBoundedBatchRetriesIt() throws Exception {
        MutableClock clock = new MutableClock(10_000L);
        FakeObjectStore objects = new FakeObjectStore(clock);
        FakeStateStore state = new FakeStateStore();
        String key = outputKey("job-outage", "task-outage", 1, ASSIGNMENT_1);
        objects.seed(reference(key), 1_000L);
        state.classify(key, OrphanOutputStateStore.AttemptOutputClassification.ORPHAN_CANDIDATE);

        try (OrphanOutputGc gc = collector(objects, state, clock, 1_000L, 4)) {
            objects.deleteAvailable = false;
            OrphanOutputGc.BatchResult unavailable = gc.runBatch();
            assertTrue(unavailable.storeUnavailable());
            assertEquals(1, unavailable.failed());
            assertEquals(0L, gc.metricsSnapshot().orphanOutputsTotal());
            assertEquals(1, state.failures.get(key).attemptCount());
            assertTrue(objects.contains(key));

            objects.deleteAvailable = true;
            OrphanOutputGc.BatchResult recovered = gc.runBatch();
            assertEquals(1, recovered.deleted());
            assertEquals(1L, gc.metricsSnapshot().orphanOutputsTotal());
            assertTrue(state.failures.isEmpty());
            assertFalse(objects.contains(key));
        }
    }

    @Test
    void listingOutageDefersWithoutImmediateRetryAndBatchNeverExceedsBound()
            throws Exception {
        MutableClock clock = new MutableClock(10_000L);
        FakeObjectStore objects = new FakeObjectStore(clock);
        FakeStateStore state = new FakeStateStore();
        for (int index = 0; index < 5; index++) {
            String key = outputKey(
                    "job-bound-" + index,
                    "task-bound-" + index,
                    1,
                    ASSIGNMENT_1
            );
            objects.seed(reference(key), 1_000L);
            state.classify(
                    key,
                    OrphanOutputStateStore.AttemptOutputClassification.ORPHAN_CANDIDATE
            );
        }

        try (OrphanOutputGc gc = collector(objects, state, clock, 1_000L, 2)) {
            objects.listAvailable = false;
            OrphanOutputGc.BatchResult outage = gc.runBatch();
            assertTrue(outage.storeUnavailable());
            assertEquals(1, objects.listAttempts);
            assertEquals(0, objects.deleteAttempts.size());

            objects.listAvailable = true;
            OrphanOutputGc.BatchResult bounded = gc.runBatch();
            assertEquals(2, bounded.examined());
            assertEquals(2, bounded.deleted());
            assertEquals(2, objects.deleteAttempts.size());
        }
    }

    private static OrphanOutputGc collector(
            ObjectStore objects,
            OrphanOutputStateStore state,
            TaskFlowClock clock,
            long safetyWindowMillis,
            int batchSize
    ) {
        return new OrphanOutputGc(
                objects,
                state,
                new OrphanOutputGcConfig(true, safetyWindowMillis, 60_000L, batchSize),
                clock
        );
    }

    private static String outputKey(
            String jobId,
            String taskId,
            int attemptNumber,
            String assignmentId
    ) {
        return TaskFlowObjectKeys.attemptOutputKey(
                jobId,
                taskId,
                attemptNumber,
                assignmentId
        );
    }

    private static ObjectReference reference(String key) {
        return new ObjectReference(key, 1L, "0".repeat(64), "application/octet-stream");
    }

    private static final class MutableClock implements TaskFlowClock {
        private long epochMillis;

        private MutableClock(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        private void set(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        @Override
        public Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public long nowEpochMillis() {
            return epochMillis;
        }
    }

    private static final class FakeObjectStore implements ObjectStore {
        private final MutableClock clock;
        private final TreeMap<String, ObjectMetadata> objects = new TreeMap<>();
        private final List<String> deleteAttempts = new ArrayList<>();
        private boolean listAvailable = true;
        private boolean deleteAvailable = true;
        private int listAttempts;

        private FakeObjectStore(MutableClock clock) {
            this.clock = clock;
        }

        private void seed(ObjectReference reference, long createdAt) {
            objects.put(reference.key(), new ObjectMetadata(reference, createdAt));
        }

        private boolean contains(String key) {
            return objects.containsKey(key);
        }

        @Override
        public ObjectReference put(ObjectReference reference, InputStream content) {
            seed(reference, clock.nowEpochMillis());
            return reference;
        }

        @Override
        public ObjectReference putIfAbsent(ObjectReference reference, InputStream content)
                throws ObjectStoreException {
            if (objects.containsKey(reference.key())) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.ALREADY_EXISTS,
                        "exists"
                );
            }
            return put(reference, content);
        }

        @Override
        public InputStream get(String key) {
            return InputStream.nullInputStream();
        }

        @Override
        public ObjectReference stat(String key) {
            return objects.get(key).reference();
        }

        @Override
        public void delete(String key) throws ObjectStoreException {
            deleteAttempts.add(key);
            if (!deleteAvailable) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.STORAGE_FAILURE,
                        "injected delete outage"
                );
            }
            objects.remove(key);
        }

        @Override
        public ObjectReference copy(String sourceKey, String destinationKey) {
            ObjectReference source = objects.get(sourceKey).reference();
            ObjectReference copied = new ObjectReference(
                    destinationKey,
                    source.contentLength(),
                    source.sha256(),
                    source.contentType()
            );
            seed(copied, clock.nowEpochMillis());
            return copied;
        }

        @Override
        public ObjectListing list(String prefix, String startAfter, int limit)
                throws ObjectStoreException {
            listAttempts++;
            if (!listAvailable) {
                throw new ObjectStoreException(
                        ObjectStoreException.Reason.STORAGE_FAILURE,
                        "injected list outage"
                );
            }
            int validatedLimit = ObjectStore.requireListLimit(limit);
            List<ObjectMetadata> listed = objects.values().stream()
                    .filter(metadata -> metadata.key().startsWith(prefix))
                    .filter(metadata -> startAfter == null
                            || metadata.key().compareTo(startAfter) > 0)
                    .sorted(Comparator.comparing(ObjectMetadata::key))
                    .limit(validatedLimit)
                    .toList();
            String next = listed.size() == validatedLimit
                    ? listed.getLast().key()
                    : null;
            return new ObjectListing(listed, next);
        }

        @Override
        public void close() {
        }
    }

    private static final class FakeStateStore implements OrphanOutputStateStore {
        private final Map<String, AttemptOutputClassification> classifications =
                new HashMap<>();
        private final Map<String, DeletionFailure> failures = new LinkedHashMap<>();

        private void classify(String key, AttemptOutputClassification classification) {
            classifications.put(key, classification);
        }

        @Override
        public AttemptOutputClassification classifyAttemptOutput(
                TaskFlowObjectKeys.AttemptOutputIdentity identity
        ) {
            return classifications.getOrDefault(
                    identity.key(),
                    AttemptOutputClassification.ORPHAN_CANDIDATE
            );
        }

        @Override
        public DeletionFailureBatch loadOrphanOutputDeletionFailures(int limit) {
            int validatedLimit = OrphanOutputStateStore.requireFailureBatchLimit(limit);
            return DeletionFailureBatch.loaded(
                    failures.values().stream()
                            .sorted(Comparator.comparingLong(DeletionFailure::lastAttemptAt))
                            .limit(validatedLimit)
                            .toList()
            );
        }

        @Override
        public MutationOutcome recordOrphanOutputDeletionFailure(
                String objectKey,
                long failedAt,
                String error
        ) {
            DeletionFailure previous = failures.get(objectKey);
            failures.put(
                    objectKey,
                    new DeletionFailure(
                            objectKey,
                            previous == null ? failedAt : previous.firstFailedAt(),
                            failedAt,
                            previous == null ? 1 : previous.attemptCount() + 1,
                            error
                    )
            );
            return MutationOutcome.COMMITTED;
        }

        @Override
        public MutationOutcome clearOrphanOutputDeletionFailure(String objectKey) {
            return failures.remove(objectKey) == null
                    ? MutationOutcome.ALREADY_APPLIED
                    : MutationOutcome.COMMITTED;
        }
    }
}
