package gui;

import protocol.RequesterIdentity;

import java.util.Optional;

interface GuiRequesterTokenStore {
    RequesterIdentity.Credentials requesterIdentity();

    /** Returns the existing token for a job id or durably creates it once. */
    String createTokenForJob(String jobId);

    Optional<String> tokenForJob(String jobId);

    void forgetToken(String jobId);
}
