package server.runtime;

import java.time.Clock;
import java.time.Instant;

/** Production UTC wall-clock adapter. */
public enum SystemTaskFlowClock implements TaskFlowClock {
    INSTANCE;

    private final Clock clock = Clock.systemUTC();

    @Override
    public Instant now() {
        return clock.instant();
    }

    @Override
    public long nowEpochMillis() {
        return clock.millis();
    }
}
