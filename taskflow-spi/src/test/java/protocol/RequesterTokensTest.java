package protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequesterTokensTest {
    @Test
    void generatedTokensHashAndMatch() {
        String token = RequesterTokens.newToken();
        String hash = RequesterTokens.hashToken(token);

        assertTrue(RequesterTokens.hasToken(token));
        assertTrue(RequesterTokens.hasTokenHash(hash));
        assertTrue(RequesterTokens.matches(token, hash));
        assertFalse(RequesterTokens.matches("wrong-token", hash));
        assertNotEquals(token, hash);
    }
}
