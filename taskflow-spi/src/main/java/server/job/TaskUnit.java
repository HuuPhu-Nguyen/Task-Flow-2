package server.job;

import java.util.Optional;

public abstract class TaskUnit<T> {
    protected final String taskId;
    protected final String jobId;
    protected final T payload;

    protected TaskStatus status = TaskStatus.PENDING;
    protected String assignedPeerId;

    private long startTime;
    private long pendingSinceMillis;
    private String leaseOwnerId = "";
    private long leaseExpiresAtMillis;

    private int retryCount;
    private int assignmentAttemptNumber;
    private AssignmentIdentity assignmentIdentity;

    public enum FailureOutcome {
        RETRY_SCHEDULED,
        TERMINAL_FAILURE,
        IGNORED
    }

    public enum TaskStatus {
        PENDING, ASSIGNED, COMPLETED, FAILED
    }

    public TaskUnit(String taskId, String jobId, T payload) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.payload = payload;
        this.retryCount = 0;
        this.assignmentAttemptNumber = 0;
        this.assignmentIdentity = null;
        this.status= TaskStatus.PENDING;
        this.pendingSinceMillis = System.currentTimeMillis();
    }

    /**
     * Marks this task completed only when the same assigned peer reports success.
     * Returns the measured attempt duration in milliseconds, or -1 when ignored.
     */
    public synchronized long markCompletedBy(String peerId) {
        if (this.status != TaskStatus.ASSIGNED) {
            return -1;
        }
        if (this.assignedPeerId == null || !this.assignedPeerId.equals(peerId)) {
            return -1;
        }
        long now = System.currentTimeMillis();
        long duration = (this.startTime > 0) ? (now - this.startTime) : -1;
        this.status = TaskStatus.COMPLETED;
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.leaseOwnerId = "";
        this.leaseExpiresAtMillis = 0L;
        this.assignmentIdentity = null;
        this.pendingSinceMillis = -1L;
        return duration;
    }

    /**
     * Applies a result only for the exact current assignment generation.
     * The supplied completion time makes duration calculation deterministic
     * and lets persistence and memory use the same timestamp.
     */
    public synchronized long markCompletedBy(String peerId,
                                             int attemptNumber,
                                             String assignmentId,
                                             long completedAtMillis) {
        if (!matchesAssignment(peerId, attemptNumber, assignmentId)) {
            return -1L;
        }
        long normalizedCompletedAt = Math.max(0L, completedAtMillis);
        long duration = this.startTime > 0L
                ? Math.max(0L, normalizedCompletedAt - this.startTime)
                : 0L;
        this.status = TaskStatus.COMPLETED;
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.leaseOwnerId = "";
        this.leaseExpiresAtMillis = 0L;
        this.assignmentIdentity = null;
        this.pendingSinceMillis = -1L;
        return duration;
    }

    public synchronized boolean matchesAssignment(String peerId,
                                                  int attemptNumber,
                                                  String assignmentId) {
        return this.status == TaskStatus.ASSIGNED
                && this.assignmentIdentity != null
                && this.assignmentIdentity.attemptNumber() == attemptNumber
                && this.assignmentIdentity.assignmentId().equals(assignmentId)
                && this.assignmentIdentity.workerId().equals(peerId);
    }

    public synchronized boolean markAssigned(String peerId, long startAtMillis) {
        return markAssigned(peerId, startAtMillis, "", 0L);
    }

    public synchronized boolean markAssigned(String peerId,
                                             long startAtMillis,
                                             String leaseOwnerId,
                                             long leaseExpiresAtMillis) {
        if (this.status != TaskStatus.PENDING) {
            return false;
        }
        long normalizedLeaseExpiry = Math.max(0L, leaseExpiresAtMillis);
        int nextAttemptNumber;
        try {
            nextAttemptNumber = Math.incrementExact(this.assignmentAttemptNumber);
        } catch (ArithmeticException e) {
            throw new IllegalStateException("Assignment attempt number overflow for task " + taskId, e);
        }
        AssignmentIdentity nextIdentity = AssignmentIdentity.create(
                taskId,
                nextAttemptNumber,
                peerId,
                normalizedLeaseExpiry
        );
        applyAssignment(nextIdentity, startAtMillis, leaseOwnerId);
        return true;
    }

    /**
     * Installs an assignment identity already committed by the authoritative
     * state store. The generation must be exactly the next local generation.
     */
    public synchronized boolean markAssigned(AssignmentIdentity committedIdentity,
                                             long startAtMillis,
                                             String leaseOwnerId) {
        if (this.status != TaskStatus.PENDING) {
            return false;
        }
        if (committedIdentity == null) {
            throw new IllegalArgumentException("Committed assignment identity is required.");
        }
        if (!taskId.equals(committedIdentity.taskId())) {
            throw new IllegalArgumentException("Committed assignment identity belongs to a different task.");
        }
        int expectedAttemptNumber;
        try {
            expectedAttemptNumber = Math.incrementExact(this.assignmentAttemptNumber);
        } catch (ArithmeticException e) {
            throw new IllegalStateException("Assignment attempt number overflow for task " + taskId, e);
        }
        if (committedIdentity.attemptNumber() != expectedAttemptNumber) {
            throw new IllegalArgumentException(
                    "Committed assignment attempt number must be exactly " + expectedAttemptNumber + "."
            );
        }
        applyAssignment(committedIdentity, startAtMillis, leaseOwnerId);
        return true;
    }

    private void applyAssignment(AssignmentIdentity identity,
                                 long startAtMillis,
                                 String leaseOwnerId) {
        this.assignedPeerId = identity.workerId();
        this.startTime = startAtMillis;
        this.leaseOwnerId = leaseOwnerId == null ? "" : leaseOwnerId;
        this.leaseExpiresAtMillis = identity.leaseExpiresAtEpochMillis();
        this.assignmentAttemptNumber = identity.attemptNumber();
        this.assignmentIdentity = identity;
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.ASSIGNED;
    }

    public synchronized void resetToPending() {
        if (this.status == TaskStatus.COMPLETED || this.status == TaskStatus.FAILED) {
            return;
        }
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.leaseOwnerId = "";
        this.leaseExpiresAtMillis = 0L;
        this.assignmentIdentity = null;
        this.pendingSinceMillis = System.currentTimeMillis();
        this.status = TaskStatus.PENDING;
    }

    public synchronized FailureOutcome failAttemptBy(String peerId, int maxRetries) {
        if (this.status != TaskStatus.ASSIGNED) {
            return FailureOutcome.IGNORED;
        }
        if (this.assignedPeerId == null || !this.assignedPeerId.equals(peerId)) {
            return FailureOutcome.IGNORED;
        }
        this.retryCount++;
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.leaseOwnerId = "";
        this.leaseExpiresAtMillis = 0L;
        this.assignmentIdentity = null;

        if (this.retryCount >= maxRetries) {
            this.status = TaskStatus.FAILED;
            this.pendingSinceMillis = -1L;
            return FailureOutcome.TERMINAL_FAILURE;
        }

        this.pendingSinceMillis = System.currentTimeMillis();
        this.status = TaskStatus.PENDING;
        return FailureOutcome.RETRY_SCHEDULED;
    }

    public synchronized TaskStatus getStatus() {
        return status;
    }

    public String getTaskId() {
        return taskId;
    }

    public synchronized long getStartTime() { return this.startTime; }
    public synchronized long getPendingSinceMillis() { return this.pendingSinceMillis; }
    public synchronized String getLeaseOwnerId() { return this.leaseOwnerId; }
    public synchronized long getLeaseExpiresAtMillis() { return this.leaseExpiresAtMillis; }

    public synchronized boolean isLeaseExpired(long nowMillis) {
        return this.status == TaskStatus.ASSIGNED
                && this.leaseExpiresAtMillis > 0L
                && nowMillis >= this.leaseExpiresAtMillis;
    }

    public T getPayload() {
        return payload;
    }

    public synchronized String getAssignedPeerId() {
        return assignedPeerId;
    }

    /**
     * Returns failed-attempt count used by retry policy. This is not the
     * monotonic assignment-generation number returned by {@link #getAttemptNumber()}.
     */
    public synchronized int getRetryCount() {
        return retryCount;
    }

    /**
     * Returns the most recently created assignment-generation number. A
     * pending task that has never been assigned returns zero.
     */
    public synchronized int getAttemptNumber() {
        return assignmentAttemptNumber;
    }

    /**
     * Returns the current immutable assignment identity while the task is
     * assigned. Re-reading it for dispatch/outbox replay does not create a new
     * generation.
     */
    public synchronized Optional<AssignmentIdentity> getAssignmentIdentity() {
        return Optional.ofNullable(assignmentIdentity);
    }

    public synchronized void restorePendingForResume(int retryCount) {
        restorePendingForResume(retryCount, 0);
    }

    public synchronized void restorePendingForResume(int retryCount, int lastAssignmentAttemptNumber) {
        int restoredAttemptNumber = requireRestoredAttemptNumber(lastAssignmentAttemptNumber);
        this.retryCount = Math.max(0, retryCount);
        this.assignmentAttemptNumber = restoredAttemptNumber;
        this.assignmentIdentity = null;
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.leaseOwnerId = "";
        this.leaseExpiresAtMillis = 0L;
        this.pendingSinceMillis = System.currentTimeMillis();
        this.status = TaskStatus.PENDING;
    }

    public synchronized void restoreAssignedForResume(String assignedPeerId,
                                                      long startedAt,
                                                      String leaseOwnerId,
                                                      long leaseExpiresAtMillis,
                                                      int retryCount) {
        restoreAssignedForResume(
                assignedPeerId,
                startedAt,
                leaseOwnerId,
                leaseExpiresAtMillis,
                retryCount,
                0
        );
    }

    public synchronized void restoreAssignedForResume(String assignedPeerId,
                                                      long startedAt,
                                                      String leaseOwnerId,
                                                      long leaseExpiresAtMillis,
                                                      int retryCount,
                                                      int lastAssignmentAttemptNumber) {
        int restoredAttemptNumber = requireRestoredAttemptNumber(lastAssignmentAttemptNumber);
        this.retryCount = Math.max(0, retryCount);
        this.assignmentAttemptNumber = restoredAttemptNumber;
        this.assignmentIdentity = null;
        this.assignedPeerId = assignedPeerId;
        this.startTime = Math.max(0L, startedAt);
        this.leaseOwnerId = leaseOwnerId == null ? "" : leaseOwnerId;
        this.leaseExpiresAtMillis = Math.max(0L, leaseExpiresAtMillis);
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.ASSIGNED;
    }

    public synchronized void restoreAssignedForResume(AssignmentIdentity restoredIdentity,
                                                      long startedAt,
                                                      String leaseOwnerId,
                                                      int retryCount) {
        if (restoredIdentity == null) {
            throw new IllegalArgumentException("Restored assignment identity is required.");
        }
        if (!taskId.equals(restoredIdentity.taskId())) {
            throw new IllegalArgumentException("Restored assignment identity belongs to a different task.");
        }
        int restoredAttemptNumber = requireRestoredAttemptNumber(restoredIdentity.attemptNumber());
        this.retryCount = Math.max(0, retryCount);
        this.assignmentAttemptNumber = restoredAttemptNumber;
        this.assignmentIdentity = restoredIdentity;
        this.assignedPeerId = restoredIdentity.workerId();
        this.startTime = Math.max(0L, startedAt);
        this.leaseOwnerId = leaseOwnerId == null ? "" : leaseOwnerId;
        this.leaseExpiresAtMillis = restoredIdentity.leaseExpiresAtEpochMillis();
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.ASSIGNED;
    }

    public synchronized void restoreCompletedForResume(int retryCount) {
        restoreCompletedForResume(retryCount, 0);
    }

    public synchronized void restoreCompletedForResume(int retryCount, int lastAssignmentAttemptNumber) {
        int restoredAttemptNumber = requireRestoredAttemptNumber(lastAssignmentAttemptNumber);
        this.retryCount = Math.max(0, retryCount);
        this.assignmentAttemptNumber = restoredAttemptNumber;
        this.assignmentIdentity = null;
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.leaseOwnerId = "";
        this.leaseExpiresAtMillis = 0L;
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.COMPLETED;
    }

    public synchronized void restoreFailedForResume(int retryCount) {
        restoreFailedForResume(retryCount, 0);
    }

    public synchronized void restoreFailedForResume(int retryCount, int lastAssignmentAttemptNumber) {
        int restoredAttemptNumber = requireRestoredAttemptNumber(lastAssignmentAttemptNumber);
        this.retryCount = Math.max(0, retryCount);
        this.assignmentAttemptNumber = restoredAttemptNumber;
        this.assignmentIdentity = null;
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.leaseOwnerId = "";
        this.leaseExpiresAtMillis = 0L;
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.FAILED;
    }

    private int requireRestoredAttemptNumber(int lastAssignmentAttemptNumber) {
        if (lastAssignmentAttemptNumber < 0) {
            throw new IllegalArgumentException("Restored assignment attempt number must not be negative.");
        }
        if (lastAssignmentAttemptNumber < this.assignmentAttemptNumber) {
            throw new IllegalArgumentException("Restored assignment attempt number must not decrease.");
        }
        return lastAssignmentAttemptNumber;
    }

    public String getJobId(){
        return jobId;
    }
}
