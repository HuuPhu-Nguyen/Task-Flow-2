package gui;

import protocol.RequesterIdentity;

import java.util.Optional;

interface GuiRequesterTokenStore {
    RequesterIdentity.Credentials requesterIdentity();

    String createTokenForJob(String jobId);

    Optional<String> tokenForJob(String jobId);

    void forgetToken(String jobId);
}
