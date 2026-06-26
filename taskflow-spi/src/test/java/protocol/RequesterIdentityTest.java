package protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequesterIdentityTest {
    @Test
    void signsAndVerifiesJobSubmitMessages() {
        RequesterIdentity.Credentials credentials = RequesterIdentity.newCredentials();
        String signature = RequesterIdentity.signJobSubmit(
                credentials.privateKey(),
                "client-1",
                "2026-06-26T00:00:00Z",
                "job-1",
                "TEXT_ANALYSIS",
                "csv",
                "token-1"
        );
        JobSubmitMessage message = new JobSubmitMessage(
                "client-1",
                "2026-06-26T00:00:00Z",
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "csv",
                "token-1",
                credentials.publicKey(),
                signature
        );

        assertTrue(RequesterIdentity.verifyJobSubmit(message));
        assertFalse(RequesterIdentity.verifyJobSubmit(new JobSubmitMessage(
                "client-1",
                "2026-06-26T00:00:00Z",
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "csv",
                "wrong-token",
                credentials.publicKey(),
                signature
        )));
    }

    @Test
    void signsAndVerifiesJobResultRequests() {
        RequesterIdentity.Credentials credentials = RequesterIdentity.newCredentials();
        String signature = RequesterIdentity.signJobResultRequest(
                credentials.privateKey(),
                "client-1",
                "2026-06-26T00:00:00Z",
                "job-1",
                "token-1"
        );
        JobResultRequestMessage message = new JobResultRequestMessage(
                "client-1",
                "2026-06-26T00:00:00Z",
                "job-1",
                "token-1",
                credentials.publicKey(),
                signature
        );

        assertTrue(RequesterIdentity.verifyJobResultRequest(message));
        assertFalse(RequesterIdentity.verifyJobResultRequest(new JobResultRequestMessage(
                "client-1",
                "2026-06-26T00:00:00Z",
                "job-2",
                "token-1",
                credentials.publicKey(),
                signature
        )));
    }
}
