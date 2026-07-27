package objectstore.minio;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import objectstore.ObjectStoreException;
import objectstore.TaskFlowObjectKeys;
import objectstore.ObjectStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

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

    private static MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(MINIO.getS3URL())
                .credentials(MINIO.getUserName(), MINIO.getPassword())
                .build();
    }
}
