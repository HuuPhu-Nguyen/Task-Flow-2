package server.runtime;

import java.time.Instant;

/**
 * Runtime time source for coordinator-owned state transitions and protocol timestamps.
 */
public interface TaskFlowClock {
    Instant now();

    long nowEpochMillis();
}
