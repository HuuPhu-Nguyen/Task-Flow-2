package server.scheduler;

import plugin.RetrySafety;
import plugin.TaskResourceProfile;
import protocol.JobSubmitMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;

import java.util.concurrent.atomic.AtomicInteger;

public final class UnsafeTestTaskPlugin implements TaskPlugin {
    static final String TASK_TYPE = "UNSAFE_TEST_TASK";
    private static final AtomicInteger CREATE_CALLS = new AtomicInteger();

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.UNSAFE_TO_RETRY;
    }

    @Override
    public TaskResourceProfile resourceProfile() {
        return TaskResourceProfile.ofCapacityUnits(1);
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        CREATE_CALLS.incrementAndGet();
        throw new IllegalStateException("Unsafe test plugin should be rejected before job creation.");
    }

    static void resetCreateCalls() {
        CREATE_CALLS.set(0);
    }

    static int createCalls() {
        return CREATE_CALLS.get();
    }
}
