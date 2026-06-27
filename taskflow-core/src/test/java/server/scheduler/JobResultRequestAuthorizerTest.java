package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultRequestMessage;
import protocol.RequesterIdentity;
import protocol.RequesterTokens;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobResultRequestAuthorizerTest {
    private static final String REQUESTER_ID = "requester-1";
    private static final String TIME = "2026-06-27T00:00:00Z";
    private static final String JOB_ID = "job-1";
    private static final String REQUESTER_TOKEN = "requester-token";

    @Test
    void authorizesMatchingTokenWhenJobIsNotIdentityBound() {
        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                request(REQUESTER_TOKEN),
                RequesterTokens.hashToken(REQUESTER_TOKEN),
                ""
        );

        assertTrue(authorization.authorized());
        assertEquals("", authorization.errorMessage());
    }

    @Test
    void rejectsMissingRequesterToken() {
        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                request(""),
                RequesterTokens.hashToken(REQUESTER_TOKEN),
                ""
        );

        assertDenied(authorization, "Requester token is required.");
    }

    @Test
    void rejectsUnavailablePersistedRequesterTokenHash() {
        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                request(REQUESTER_TOKEN),
                "",
                ""
        );

        assertDenied(authorization, "Requester token is unavailable for this job.");
    }

    @Test
    void rejectsWrongRequesterToken() {
        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                request("wrong-token"),
                RequesterTokens.hashToken(REQUESTER_TOKEN),
                ""
        );

        assertDenied(authorization, "Requester token does not match job owner.");
    }

    @Test
    void rejectsMissingIdentitySignatureForIdentityBoundJob() {
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();

        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                request(REQUESTER_TOKEN),
                RequesterTokens.hashToken(REQUESTER_TOKEN),
                identity.publicKey()
        );

        assertDenied(authorization, "Requester identity signature is required.");
    }

    @Test
    void rejectsMismatchedRequesterIdentityKey() {
        RequesterIdentity.Credentials owner = RequesterIdentity.newCredentials();
        RequesterIdentity.Credentials wrongIdentity = RequesterIdentity.newCredentials();

        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                signedRequest(REQUESTER_TOKEN, wrongIdentity, JOB_ID),
                RequesterTokens.hashToken(REQUESTER_TOKEN),
                owner.publicKey()
        );

        assertDenied(authorization, "Requester identity key does not match job owner.");
    }

    @Test
    void rejectsInvalidRequesterIdentitySignature() {
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();

        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                signedRequest(REQUESTER_TOKEN, identity, "different-job"),
                RequesterTokens.hashToken(REQUESTER_TOKEN),
                identity.publicKey()
        );

        assertDenied(authorization, "Requester identity signature is invalid.");
    }

    @Test
    void authorizesValidRequesterIdentitySignature() {
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();

        JobResultRequestAuthorizer.Authorization authorization = JobResultRequestAuthorizer.authorize(
                signedRequest(REQUESTER_TOKEN, identity, JOB_ID),
                RequesterTokens.hashToken(REQUESTER_TOKEN),
                identity.publicKey()
        );

        assertTrue(authorization.authorized());
        assertEquals("", authorization.errorMessage());
    }

    private static JobResultRequestMessage request(String requesterToken) {
        return new JobResultRequestMessage(REQUESTER_ID, TIME, JOB_ID, requesterToken);
    }

    private static JobResultRequestMessage signedRequest(String requesterToken,
                                                        RequesterIdentity.Credentials identity,
                                                        String signedJobId) {
        String signature = RequesterIdentity.signJobResultRequest(
                identity.privateKey(),
                REQUESTER_ID,
                TIME,
                signedJobId,
                requesterToken
        );
        return new JobResultRequestMessage(
                REQUESTER_ID,
                TIME,
                JOB_ID,
                requesterToken,
                identity.publicKey(),
                signature
        );
    }

    private static void assertDenied(JobResultRequestAuthorizer.Authorization authorization, String message) {
        assertFalse(authorization.authorized());
        assertEquals(message, authorization.errorMessage());
    }
}
