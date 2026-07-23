package protocol;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerIdentityTest {

    @Test
    void sanitizesPeerIdsForRoutingLoggingAndFileNames() {
        assertEquals("peer-alpha_01", PeerIdentity.sanitize(" peer-alpha 01 "));
        assertEquals("peer_bad_path", PeerIdentity.sanitize("../peer.bad/path"));
        assertEquals("peer_bad", PeerIdentity.require("peer@bad"));
    }

    @Test
    void configuredPeerIdUsesSharedEnvironmentNameAndSanitization() {
        String peerId = PeerIdentity.configuredOrGenerated(
                Map.of(PeerIdentity.PEER_ID_ENV, " gui peer/1 "),
                "fallback");

        assertEquals("gui_peer_1", peerId);
    }

    @Test
    void blankConfiguredPeerIdFallsBackToGeneratedRuntimeId() {
        String peerId = PeerIdentity.configuredOrGenerated(
                Map.of(PeerIdentity.PEER_ID_ENV, " "),
                "rabbit peer");

        assertTrue(peerId.startsWith("rabbit_peer_"));
        assertFalse(peerId.contains(" "));
    }

    @Test
    void generatedPeerIdsAreUniqueAndSafe() {
        String first = PeerIdentity.generated("participant");
        String second = PeerIdentity.generated("participant");

        assertNotEquals(first, second);
        assertTrue(first.matches("[A-Za-z0-9_-]+"));
        assertTrue(second.matches("[A-Za-z0-9_-]+"));
    }

    @Test
    void requireRejectsBlankIdsAfterSanitization() {
        assertThrows(IllegalArgumentException.class, () -> PeerIdentity.require("..."));
    }
}
