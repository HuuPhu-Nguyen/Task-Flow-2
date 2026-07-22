package peer.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

final class AssignmentResultCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(AssignmentResultCache.class);

    private final String peerId;
    private final AssignmentCacheConfig config;
    private final LongSupplier clock;
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);

    private long runningDuplicateCount;
    private long completedDuplicateCount;
    private long evictionCount;
    private long capacityEvictionCount;
    private long ttlEvictionCount;

    AssignmentResultCache(String peerId, AssignmentCacheConfig config, LongSupplier clock) {
        this.peerId = peerId;
        this.config = config;
        this.clock = clock;
    }

    synchronized Claim claim(TaskAssignMessage assignment) {
        long now = clock.getAsLong();
        evictExpired(now);

        String assignmentId = assignment.getAssignmentId();
        Entry existing = entries.get(assignmentId);
        if (existing != null) {
            existing.identity.requireMatch(assignment);
            if (existing.state == EntryState.RUNNING) {
                runningDuplicateCount++;
                return new Claim(
                        AssignmentExecution.Disposition.DUPLICATE_RUNNING,
                        existing.resultFuture
                );
            }
            completedDuplicateCount++;
            return new Claim(
                    AssignmentExecution.Disposition.DUPLICATE_COMPLETED,
                    existing.resultFuture
            );
        }

        while (entries.size() >= config.maxEntries()) {
            Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
            if (!iterator.hasNext()) {
                break;
            }
            Map.Entry<String, Entry> eldest = iterator.next();
            iterator.remove();
            recordEviction(eldest.getKey(), eldest.getValue(), "capacity");
            capacityEvictionCount++;
        }

        CompletableFuture<TaskResultMessage> resultFuture = new CompletableFuture<>();
        entries.put(
                assignmentId,
                new Entry(
                        AssignmentIdentity.from(assignment),
                        EntryState.RUNNING,
                        resultFuture,
                        expiresAt(now)
                )
        );
        return new Claim(AssignmentExecution.Disposition.STARTED, resultFuture);
    }

    synchronized void markCompleted(TaskAssignMessage assignment,
                                    CompletableFuture<TaskResultMessage> expectedFuture,
                                    TaskResultMessage result) {
        Entry entry = entries.get(assignment.getAssignmentId());
        if (entry == null || entry.resultFuture != expectedFuture) {
            return;
        }
        entry.identity.requireMatch(assignment);
        if (entry.state != EntryState.RUNNING) {
            return;
        }
        requireMatchingResult(assignment, result);
        entry.state = EntryState.COMPLETED;
        entry.expiresAtMillis = expiresAt(clock.getAsLong());
    }

    synchronized void invalidate(TaskAssignMessage assignment,
                                 CompletableFuture<TaskResultMessage> expectedFuture) {
        Entry entry = entries.get(assignment.getAssignmentId());
        if (entry != null
                && entry.resultFuture == expectedFuture
                && entry.identity.matches(assignment)) {
            entries.remove(assignment.getAssignmentId());
        }
    }

    synchronized AssignmentCacheSnapshot snapshot() {
        evictExpired(clock.getAsLong());
        int runningEntries = 0;
        int completedEntries = 0;
        for (Entry entry : entries.values()) {
            if (entry.state == EntryState.RUNNING) {
                runningEntries++;
            } else {
                completedEntries++;
            }
        }
        return new AssignmentCacheSnapshot(
                entries.size(),
                runningEntries,
                completedEntries,
                runningDuplicateCount,
                completedDuplicateCount,
                evictionCount,
                capacityEvictionCount,
                ttlEvictionCount
        );
    }

    synchronized void clear() {
        entries.clear();
    }

    private void evictExpired(long now) {
        Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Entry> candidate = iterator.next();
            if (candidate.getValue().expiresAtMillis <= now) {
                iterator.remove();
                recordEviction(candidate.getKey(), candidate.getValue(), "ttl");
                ttlEvictionCount++;
            }
        }
    }

    private void recordEviction(String assignmentId, Entry entry, String reason) {
        evictionCount++;
        LOGGER.info(
                "event=assignment_cache_evicted peer_id={} assignment_id={} state={} reason={} "
                        + "cache_size={} eviction_count={}",
                peerId,
                assignmentId,
                entry.state,
                reason,
                entries.size(),
                evictionCount
        );
    }

    private long expiresAt(long now) {
        if (Long.MAX_VALUE - now < config.ttlMillis()) {
            return Long.MAX_VALUE;
        }
        return now + config.ttlMillis();
    }

    private static void requireMatchingResult(TaskAssignMessage assignment, TaskResultMessage result) {
        if (result == null
                || !assignment.getTaskId().equals(result.getTaskId())
                || !assignment.getJobId().equals(result.getJobId())
                || assignment.getAttemptNumber() != result.getAttemptNumber()
                || !assignment.getAssignmentId().equals(result.getAssignmentId())) {
            throw new IllegalArgumentException("Task result does not match its assignment identity.");
        }
    }

    record Claim(AssignmentExecution.Disposition disposition,
                 CompletableFuture<TaskResultMessage> resultFuture) {
    }

    private enum EntryState {
        RUNNING,
        COMPLETED
    }

    private record AssignmentIdentity(String taskId,
                                      String jobId,
                                      int attemptNumber,
                                      String assignmentId) {
        private static AssignmentIdentity from(TaskAssignMessage assignment) {
            return new AssignmentIdentity(
                    assignment.getTaskId(),
                    assignment.getJobId(),
                    assignment.getAttemptNumber(),
                    assignment.getAssignmentId()
            );
        }

        private boolean matches(TaskAssignMessage assignment) {
            return taskId.equals(assignment.getTaskId())
                    && jobId.equals(assignment.getJobId())
                    && attemptNumber == assignment.getAttemptNumber()
                    && assignmentId.equals(assignment.getAssignmentId());
        }

        private void requireMatch(TaskAssignMessage assignment) {
            if (!matches(assignment)) {
                throw new AssignmentCacheConflictException(
                        "Assignment ID " + assignmentId + " was reused for a different task identity."
                );
            }
        }
    }

    private static final class Entry {
        private final AssignmentIdentity identity;
        private EntryState state;
        private final CompletableFuture<TaskResultMessage> resultFuture;
        private long expiresAtMillis;

        private Entry(AssignmentIdentity identity,
                      EntryState state,
                      CompletableFuture<TaskResultMessage> resultFuture,
                      long expiresAtMillis) {
            this.identity = identity;
            this.state = state;
            this.resultFuture = resultFuture;
            this.expiresAtMillis = expiresAtMillis;
        }
    }
}
