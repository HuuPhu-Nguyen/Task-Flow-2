package objectstore.minio;

import com.github.dockerjava.api.model.ExposedPort;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.MinIOContainer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MinioObjectStoreContractTest extends ObjectStoreContractTest {
    private static final String IMAGE = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    private static final String BUCKET = "taskflow-contract-" + UUID.randomUUID();
    private static final MinIOContainer MINIO = new MinIOContainer(IMAGE);

    @BeforeAll
    static void startMinio() throws Exception {
        MINIO.start();
        try (MinioClient client = minioClient()) {
            client.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        }
    }

    @AfterAll
    static void stopMinio() {
        MINIO.stop();
    }

    @Override
    protected ObjectStore createStore() {
        return new MinioObjectStore(minioClient(), BUCKET);
    }

    @Override
    protected ObjectStore createUnavailableStore() {
        MinioClient unavailableClient = MinioClient.builder()
                .endpoint("http://127.0.0.1:1")
                .credentials("unused-access", "unused-secret")
                .build();
        unavailableClient.setTimeout(250L, 250L, 250L);
        return new MinioObjectStore(unavailableClient, BUCKET);
    }

    @Test
    void missingRequiredDigestMetadataIsClassified() throws Exception {
        String key = TaskFlowObjectKeys.objectKey("contracts", UUID.randomUUID().toString(), "missing-digest");
        byte[] content = "missing digest".getBytes(StandardCharsets.UTF_8);
        try (MinioClient client = minioClient()) {
            client.putObject(
                    PutObjectArgs.builder()
                            .bucket(BUCKET)
                            .object(key)
                            .stream(new ByteArrayInputStream(content), (long) content.length, -1L)
                            .contentType("text/plain")
                            .build()
            );
        }

        try (ObjectStore store = createStore()) {
            ObjectStoreException failure = assertThrows(ObjectStoreException.class, () -> store.stat(key));
            assertEquals(ObjectStoreException.Reason.INVALID_METADATA, failure.reason());
        }
    }

    @Test
    void containerRestartPreservesStreamedObjectMetadataAndDeletion() throws Exception {
        byte[] content = "object survives MinIO restart".getBytes(StandardCharsets.UTF_8);
        String key = TaskFlowObjectKeys.objectKey(
                "contracts",
                UUID.randomUUID().toString(),
                "restart"
        );
        ObjectReference expected = new ObjectReference(
                key,
                content.length,
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content)),
                "text/plain"
        );

        try (ObjectStore store = createStore()) {
            assertEquals(expected, store.put(expected, new ByteArrayInputStream(content)));
        }

        restartMinioContainer();

        try (ObjectStore store = createStore()) {
            assertEquals(expected, store.stat(key));
            try (InputStream download = store.get(key)) {
                assertArrayEquals(content, download.readAllBytes());
            }
            store.delete(key);
            ObjectStoreException missing =
                    assertThrows(ObjectStoreException.class, () -> store.stat(key));
            assertEquals(ObjectStoreException.Reason.NOT_FOUND, missing.reason());
        }
    }

    private static void restartMinioContainer() {
        MINIO.getDockerClient()
                .stopContainerCmd(MINIO.getContainerId())
                .withTimeout(20)
                .exec();
        MINIO.getDockerClient()
                .startContainerCmd(MINIO.getContainerId())
                .exec();
        Unreliables.retryUntilSuccess(45, TimeUnit.SECONDS, () -> {
            try (MinioClient client = minioClient()) {
                client.listBuckets();
                return true;
            }
        });
    }

    private static MinioClient minioClient() {
        var bindings = MINIO.getDockerClient()
                .inspectContainerCmd(MINIO.getContainerId())
                .exec()
                .getNetworkSettings()
                .getPorts()
                .getBindings()
                .get(ExposedPort.tcp(9000));
        if (bindings == null || bindings.length == 0) {
            throw new IllegalStateException("MinIO S3 port is not mapped.");
        }
        return MinioClient.builder()
                .endpoint("http://" + MINIO.getHost() + ":" + bindings[0].getHostPortSpec())
                .credentials(MINIO.getUserName(), MINIO.getPassword())
                .build();
    }
}
