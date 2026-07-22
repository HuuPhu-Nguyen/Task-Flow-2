package server.runtime;

import java.util.UUID;

/** Production random-UUID assignment-ID adapter. */
public enum UuidAssignmentIdGenerator implements AssignmentIdGenerator {
    INSTANCE;

    @Override
    public String nextAssignmentId() {
        return UUID.randomUUID().toString();
    }
}
