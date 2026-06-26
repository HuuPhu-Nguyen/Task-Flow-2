package protocol;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public final class RequesterTokens {
    private static final int TOKEN_BYTES = 32;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Base64.Encoder TOKEN_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private RequesterTokens() {
    }

    public static String newToken() {
        byte[] bytes = new byte[TOKEN_BYTES];
        RANDOM.nextBytes(bytes);
        return TOKEN_ENCODER.encodeToString(bytes);
    }

    public static String hashToken(String token) {
        if (token == null || token.isBlank()) {
            return "";
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(token.getBytes(StandardCharsets.UTF_8));
            return TOKEN_ENCODER.encodeToString(hashed);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    public static boolean hasToken(String token) {
        return token != null && !token.isBlank();
    }

    public static boolean hasTokenHash(String tokenHash) {
        return tokenHash != null && !tokenHash.isBlank();
    }

    public static boolean matches(String token, String expectedTokenHash) {
        if (!hasToken(token) || !hasTokenHash(expectedTokenHash)) {
            return false;
        }
        byte[] candidate = hashToken(token).getBytes(StandardCharsets.UTF_8);
        byte[] expected = expectedTokenHash.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(candidate, expected);
    }
}
