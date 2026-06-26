package gui;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

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
    void forgetTokenRemovesPersistedToken() {
        Path storePath = tempDir.resolve("requester-tokens.properties");
        FileGuiRequesterTokenStore store = new FileGuiRequesterTokenStore(storePath);

        store.createTokenForJob("job-1");
        store.forgetToken("job-1");

        FileGuiRequesterTokenStore reloaded = new FileGuiRequesterTokenStore(storePath);
        assertTrue(reloaded.tokenForJob("job-1").isEmpty());
    }
}
