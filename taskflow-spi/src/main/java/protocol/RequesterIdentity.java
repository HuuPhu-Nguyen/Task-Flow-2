package protocol;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public final class RequesterIdentity {
    private static final String KEY_ALGORITHM = "Ed25519";
    private static final String SIGNATURE_ALGORITHM = "Ed25519";
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private RequesterIdentity() {
    }

    public record Credentials(String publicKey, String privateKey) {
    }

    public static Credentials newCredentials() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
            KeyPair keyPair = generator.generateKeyPair();
            return new Credentials(
                    ENCODER.encodeToString(keyPair.getPublic().getEncoded()),
                    ENCODER.encodeToString(keyPair.getPrivate().getEncoded())
            );
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Could not generate requester identity.", e);
        }
    }

    public static boolean hasIdentity(String publicKey, String signature) {
        return publicKey != null && !publicKey.isBlank()
                && signature != null && !signature.isBlank();
    }

    public static boolean hasPartialIdentity(String publicKey, String signature) {
        boolean hasPublicKey = publicKey != null && !publicKey.isBlank();
        boolean hasSignature = signature != null && !signature.isBlank();
        return hasPublicKey != hasSignature;
    }

    public static String signJobSubmit(String privateKey,
                                       String nodeId,
                                       String time,
                                       String jobId,
                                       String taskType,
                                       String parameter,
                                       String requesterToken) {
        return sign(privateKey, canonicalJobSubmit(nodeId, time, jobId, taskType, parameter, requesterToken));
    }

    public static String signJobResultRequest(String privateKey,
                                              String nodeId,
                                              String time,
                                              String jobId,
                                              String requesterToken) {
        return sign(privateKey, canonicalJobResultRequest(nodeId, time, jobId, requesterToken));
    }

    public static boolean verifyJobSubmit(JobSubmitMessage message) {
        if (message == null || !hasIdentity(message.getRequesterPublicKey(), message.getRequesterSignature())) {
            return false;
        }
        return verify(
                message.getRequesterPublicKey(),
                message.getRequesterSignature(),
                canonicalJobSubmit(
                        message.getNodeId(),
                        message.getTime(),
                        message.getJobId(),
                        message.getTaskType(),
                        message.getParameter(),
                        message.getRequesterToken()
                )
        );
    }

    public static boolean verifyJobResultRequest(JobResultRequestMessage message) {
        if (message == null || !hasIdentity(message.getRequesterPublicKey(), message.getRequesterSignature())) {
            return false;
        }
        return verify(
                message.getRequesterPublicKey(),
                message.getRequesterSignature(),
                canonicalJobResultRequest(
                        message.getNodeId(),
                        message.getTime(),
                        message.getJobId(),
                        message.getRequesterToken()
                )
        );
    }

    private static String sign(String encodedPrivateKey, String payload) {
        try {
            Signature signer = Signature.getInstance(SIGNATURE_ALGORITHM);
            signer.initSign(privateKey(encodedPrivateKey));
            signer.update(payload.getBytes(StandardCharsets.UTF_8));
            return ENCODER.encodeToString(signer.sign());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Could not sign requester identity payload.", e);
        }
    }

    private static boolean verify(String encodedPublicKey, String encodedSignature, String payload) {
        try {
            Signature verifier = Signature.getInstance(SIGNATURE_ALGORITHM);
            verifier.initVerify(publicKey(encodedPublicKey));
            verifier.update(payload.getBytes(StandardCharsets.UTF_8));
            return verifier.verify(DECODER.decode(encodedSignature));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            return false;
        }
    }

    private static PublicKey publicKey(String encodedPublicKey) throws GeneralSecurityException {
        byte[] keyBytes = DECODER.decode(encodedPublicKey);
        return KeyFactory.getInstance(KEY_ALGORITHM).generatePublic(new X509EncodedKeySpec(keyBytes));
    }

    private static PrivateKey privateKey(String encodedPrivateKey) throws GeneralSecurityException {
        byte[] keyBytes = DECODER.decode(encodedPrivateKey);
        return KeyFactory.getInstance(KEY_ALGORITHM).generatePrivate(new PKCS8EncodedKeySpec(keyBytes));
    }

    private static String canonicalJobSubmit(String nodeId,
                                             String time,
                                             String jobId,
                                             String taskType,
                                             String parameter,
                                             String requesterToken) {
        return String.join("\n",
                MessageType.JOB_SUBMIT,
                value(nodeId),
                value(time),
                value(jobId),
                value(taskType),
                value(parameter),
                value(requesterToken)
        );
    }

    private static String canonicalJobResultRequest(String nodeId,
                                                    String time,
                                                    String jobId,
                                                    String requesterToken) {
        return String.join("\n",
                MessageType.JOB_RESULT_REQUEST,
                value(nodeId),
                value(time),
                value(jobId),
                value(requesterToken)
        );
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }
}
