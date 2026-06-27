package server.scheduler;

import protocol.JobResultRequestMessage;
import protocol.RequesterIdentity;
import protocol.RequesterTokens;

final class JobResultRequestAuthorizer {
    private JobResultRequestAuthorizer() {
    }

    static Authorization authorize(JobResultRequestMessage request,
                                   String expectedTokenHash,
                                   String expectedIdentityKey) {
        if (!RequesterTokens.hasToken(request.getRequesterToken())) {
            return Authorization.deny("Requester token is required.");
        }
        if (!RequesterTokens.hasTokenHash(expectedTokenHash)) {
            return Authorization.deny("Requester token is unavailable for this job.");
        }
        if (!RequesterTokens.matches(request.getRequesterToken(), expectedTokenHash)) {
            return Authorization.deny("Requester token does not match job owner.");
        }
        if (hasText(expectedIdentityKey)
                && !RequesterIdentity.hasIdentity(request.getRequesterPublicKey(), request.getRequesterSignature())) {
            return Authorization.deny("Requester identity signature is required.");
        }
        if (hasText(expectedIdentityKey) && !expectedIdentityKey.equals(request.getRequesterPublicKey())) {
            return Authorization.deny("Requester identity key does not match job owner.");
        }
        if (hasText(expectedIdentityKey) && !RequesterIdentity.verifyJobResultRequest(request)) {
            return Authorization.deny("Requester identity signature is invalid.");
        }
        return Authorization.allow();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    record Authorization(boolean authorized, String errorMessage) {
        private static Authorization allow() {
            return new Authorization(true, "");
        }

        private static Authorization deny(String errorMessage) {
            return new Authorization(false, errorMessage);
        }
    }
}
