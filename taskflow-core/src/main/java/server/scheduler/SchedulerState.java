package server.scheduler;

import protocol.RequesterTokens;
import server.job.EmbarrassinglyParallelJob;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Scheduler-owned in-memory projection indexes.
 *
 * <p>This object does not decide or execute task transitions. Focused services
 * update it only after their existing persistence/transition boundary permits
 * the projection change.</p>
 */
final class SchedulerState {
    private final Map<String, EmbarrassinglyParallelJob<?, ?>> activeJobs = new LinkedHashMap<>();
    private final Map<String, String> requesterTokenHashes = new LinkedHashMap<>();
    private final Map<String, String> requesterIdentityKeys = new LinkedHashMap<>();
    private final Map<String, String> requestHashes = new LinkedHashMap<>();

    boolean hasActiveJob(String jobId) {
        return activeJobs.containsKey(jobId);
    }

    EmbarrassinglyParallelJob<?, ?> activeJob(String jobId) {
        return activeJobs.get(jobId);
    }

    Collection<EmbarrassinglyParallelJob<?, ?>> activeJobs() {
        return activeJobs.values();
    }

    List<EmbarrassinglyParallelJob<?, ?>> activeJobsSnapshot() {
        return List.copyOf(activeJobs.values());
    }

    int activeJobCount() {
        return activeJobs.size();
    }

    void addActiveJob(EmbarrassinglyParallelJob<?, ?> job,
                      String requesterTokenHash,
                      String requesterIdentityKey) {
        addActiveJob(job, requesterTokenHash, requesterIdentityKey, "");
    }

    void addActiveJob(EmbarrassinglyParallelJob<?, ?> job,
                      String requesterTokenHash,
                      String requesterIdentityKey,
                      String requestHash) {
        activeJobs.put(job.getJobId(), job);
        if (RequesterTokens.hasTokenHash(requesterTokenHash)) {
            requesterTokenHashes.put(job.getJobId(), requesterTokenHash);
        }
        if (hasText(requesterIdentityKey)) {
            requesterIdentityKeys.put(job.getJobId(), requesterIdentityKey);
        }
        if (hasText(requestHash)) {
            requestHashes.put(job.getJobId(), requestHash);
        }
    }

    void removeJob(String jobId) {
        activeJobs.remove(jobId);
        requesterTokenHashes.remove(jobId);
        requesterIdentityKeys.remove(jobId);
        requestHashes.remove(jobId);
    }

    String requesterTokenHash(String jobId) {
        return requesterTokenHashes.get(jobId);
    }

    String requesterIdentityKey(String jobId) {
        return requesterIdentityKeys.get(jobId);
    }

    String requestHash(String jobId) {
        return requestHashes.get(jobId);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
