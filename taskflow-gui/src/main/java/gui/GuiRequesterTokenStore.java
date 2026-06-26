package gui;

import java.util.Optional;

interface GuiRequesterTokenStore {
    String createTokenForJob(String jobId);

    Optional<String> tokenForJob(String jobId);

    void forgetToken(String jobId);
}
