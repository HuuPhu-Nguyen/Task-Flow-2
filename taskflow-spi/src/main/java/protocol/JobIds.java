package protocol;

import java.util.UUID;

public final class JobIds {
    private static final String JOB_PREFIX = "JOB";

    private JobIds() {
    }

    public static String newJobId(String peerId) {
        String requesterId = PeerIdentity.require(peerId);
        return JOB_PREFIX + "_" + requesterId + "_"
                + System.currentTimeMillis() + "_"
                + UUID.randomUUID();
    }
}
