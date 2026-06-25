package server.job;

public abstract class TaskUnit<T> {
    protected final String taskId;
    protected final String jobId;
    protected final T payload;

    protected TaskStatus status = TaskStatus.PENDING;
    protected String assignedPeerId;

    private long startTime;
    private long pendingSinceMillis;

    private int retryCount;

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
        this.pendingSinceMillis = -1L;
        return duration;
    }

    public synchronized boolean markAssigned(String peerId, long startAtMillis) {
        if (this.status != TaskStatus.PENDING) {
            return false;
        }
        this.assignedPeerId = peerId;
        this.startTime = startAtMillis;
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.ASSIGNED;
        return true;
    }

    public synchronized void resetToPending() {
        if (this.status == TaskStatus.COMPLETED || this.status == TaskStatus.FAILED) {
            return;
        }
        this.assignedPeerId = null;
        this.startTime = 0L;
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

    public T getPayload() {
        return payload;
    }

    public synchronized String getAssignedPeerId() {
        return assignedPeerId;
    }

    public synchronized int getRetryCount() {
        return retryCount;
    }

    public synchronized void restorePendingForResume(int retryCount) {
        this.retryCount = Math.max(0, retryCount);
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.pendingSinceMillis = System.currentTimeMillis();
        this.status = TaskStatus.PENDING;
    }

    public synchronized void restoreCompletedForResume(int retryCount) {
        this.retryCount = Math.max(0, retryCount);
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.COMPLETED;
    }

    public synchronized void restoreFailedForResume(int retryCount) {
        this.retryCount = Math.max(0, retryCount);
        this.assignedPeerId = null;
        this.startTime = 0L;
        this.pendingSinceMillis = -1L;
        this.status = TaskStatus.FAILED;
    }

    public String getJobId(){
        return jobId;
    }
}
