package gui;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.RequesterIdentity;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileGuiRequesterTokenStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsTokensAcrossStoreInstances() {
        Path storePath = tempDir.resolve("requester-tokens.properties");
        FileGuiRequesterTokenStore firstStore = new FileGuiRequesterTokenStore(storePath);

        String token = firstStore.createTokenForJob("job-1");

        FileGuiRequesterTokenStore secondStore = new FileGuiRequesterTokenStore(storePath);
        assertEquals(token, secondStore.tokenForJob("job-1").orElseThrow());
    }

    @Test
    void persistsRequesterIdentityAcrossStoreInstances() {
        Path storePath = tempDir.resolve("requester-tokens.properties");
        FileGuiRequesterTokenStore firstStore = new FileGuiRequesterTokenStore(storePath);

        RequesterIdentity.Credentials identity = firstStore.requesterIdentity();

        FileGuiRequesterTokenStore secondStore = new FileGuiRequesterTokenStore(storePath);
        RequesterIdentity.Credentials reloaded = secondStore.requesterIdentity();
        assertEquals(identity.publicKey(), reloaded.publicKey());
        assertEquals(identity.privateKey(), reloaded.privateKey());
    }

    @Test
    void forgetTokenRemovesPersistedToken() {
        Path storePath = tempDir.resolve("requester-tokens.properties");
        FileGuiRequesterTokenStore store = new FileGuiRequesterTokenStore(storePath);

        store.createTokenForJob("job-1");
        store.forgetToken("job-1");

        FileGuiRequesterTokenStore reloaded = new FileGuiRequesterTokenStore(storePath);
        assertTrue(reloaded.tokenForJob("job-1").isEmpty());
    }

    @Test
    void restrictsStorePermissionsWhenPosixPermissionsAreSupported() throws Exception {
        assumeTrue(Files.getFileAttributeView(tempDir, PosixFileAttributeView.class) != null);
        Path storeDirectory = tempDir.resolve("nested");
        Path storePath = storeDirectory.resolve("requester-tokens.properties");
        FileGuiRequesterTokenStore store = new FileGuiRequesterTokenStore(storePath);

        store.createTokenForJob("job-1");

        assertEquals(
                Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
                Files.getPosixFilePermissions(storePath)
        );
        assertEquals(
                Set.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE
                ),
                Files.getPosixFilePermissions(storeDirectory)
        );
    }
}
