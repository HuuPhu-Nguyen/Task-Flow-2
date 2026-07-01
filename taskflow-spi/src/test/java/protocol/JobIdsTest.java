package protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobIdsTest {

    @Test
    void generatedJobIdsIncludeSanitizedPeerIdTimestampAndFullUuid() {
        String jobId = JobIds.newJobId(" gui peer/1 ");

        assertTrue(jobId.matches(
                "JOB_gui_peer_1_\\d+_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
    }

    @Test
    void generatedJobIdsAreUniqueForSamePeer() {
        String first = JobIds.newJobId("peer-1");
        String second = JobIds.newJobId("peer-1");

        assertNotEquals(first, second);
    }

    @Test
    void generatedJobIdsRejectBlankPeerIds() {
        assertThrows(IllegalArgumentException.class, () -> JobIds.newJobId("..."));
    }
}
