package peer.engine;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounds immediate heartbeat work to one queued/running publish and one dirty
 * rerun while preserving a capacity change that arrives during publication.
 */
public final class CoalescingCapacityHeartbeat {
    private static final int IDLE = 0;
    private static final int SCHEDULED = 1;
    private static final int DIRTY = 2;

    private final Executor executor;
    private final Runnable publisher;
    private final AtomicInteger state = new AtomicInteger(IDLE);

    public CoalescingCapacityHeartbeat(Executor executor, Runnable publisher) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
    }

    public void request() {
        while (true) {
            int current = state.get();
            if (current == DIRTY || state.compareAndSet(SCHEDULED, DIRTY)) {
                return;
            }
            if (current == IDLE && state.compareAndSet(IDLE, SCHEDULED)) {
                try {
                    executor.execute(this::drain);
                } catch (RuntimeException e) {
                    state.set(IDLE);
                    throw e;
                }
                return;
            }
        }
    }

    private void drain() {
        while (true) {
            try {
                publisher.run();
            } catch (RuntimeException e) {
                state.set(IDLE);
                throw e;
            }
            if (state.compareAndSet(SCHEDULED, IDLE)) {
                return;
            }
            if (!state.compareAndSet(DIRTY, SCHEDULED)) {
                throw new IllegalStateException("Unexpected capacity heartbeat state.");
            }
        }
    }
}
