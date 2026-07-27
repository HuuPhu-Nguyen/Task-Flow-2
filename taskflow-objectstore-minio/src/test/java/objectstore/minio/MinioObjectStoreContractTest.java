package objectstore.minio;

import client.plugins.conversion.ImageConversionClientPlugin;
import com.google.gson.Gson;
import com.github.dockerjava.api.model.ExposedPort;
import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.ObjectStoreProvider;
import objectstore.PayloadIntegrityException;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peer.processors.ImageConversionProcessor;
import protocol.PayloadLimits;
import protocol.TaskAssignMessage;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.MinIOContainer;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MinioObjectStoreContractTest extends ObjectStoreContractTest {
    private static final String IMAGE = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    private static final String BUCKET = "taskflow-contract-" + UUID.randomUUID();
    private static final MinIOContainer MINIO = new MinIOContainer(IMAGE);

    @TempDir
    Path tempDir;

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

    @Test
    void serviceLoaderDiscoversMinioRuntimeProvider() {
        List<Class<? extends ObjectStoreProvider>> providerTypes =
                ServiceLoader.load(ObjectStoreProvider.class).stream()
                .map(ServiceLoader.Provider::type)
                .toList();

        assertEquals(List.of(MinioObjectStoreProvider.class), providerTypes);
    }

    @Test
    void separateConversionParticipantsExchangeInputOnlyByPortableObjectKey() throws Exception {
        byte[] inputBytes = pngBytes();
        Path input = tempDir.resolve("remote-input.png");
        Files.write(input, inputBytes);
        MinioObjectStoreProvider submitterProvider = provider();
        MinioObjectStoreProvider executorProvider = provider();
        FilePayload wirePayload;
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "1");
        try {
            List<Object> payloads = new ImageConversionClientPlugin(submitterProvider)
                    .buildPayloads(List.of(input), "png");
            String wireJson = new Gson().toJson(payloads.getFirst());
            assertFalse(wireJson.contains("storageType"));
            assertFalse(wireJson.contains("location"));
            assertFalse(wireJson.contains(input.toAbsolutePath().toString()));
            wirePayload = new Gson().fromJson(wireJson, FilePayload.class);
        } finally {
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
        }

        assertTrue(wirePayload.hasObjectReference());
        assertTrue(wirePayload.objectReference().key().startsWith("taskflow/inputs/"));
        FilePayload firstResult = null;
        FilePayload secondResult = null;
        try {
            String firstAssignmentId = "550e8400-e29b-41d4-a716-446655440000";
            String secondAssignmentId = "550e8400-e29b-41d4-a716-446655440002";
            TaskAssignMessage firstAssignment = new TaskAssignMessage(
                    "coordinator-1",
                    Instant.now().toString(),
                    "task-remote",
                    "job-remote",
                    ConversionTaskTypes.IMAGE_CONVERSION,
                    1,
                    firstAssignmentId,
                    1_780_000_000_000L,
                    wirePayload,
                    "png"
            );
            TaskAssignMessage secondAssignment = new TaskAssignMessage(
                    "coordinator-1",
                    Instant.now().toString(),
                    "task-remote",
                    "job-remote",
                    ConversionTaskTypes.IMAGE_CONVERSION,
                    2,
                    secondAssignmentId,
                    1_780_000_000_000L,
                    wirePayload,
                    "png"
            );

            System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "1");
            try {
                ImageConversionProcessor processor =
                        new ImageConversionProcessor(executorProvider);
                firstResult = processor.process(firstAssignment);
                secondResult = processor.process(secondAssignment);
            } finally {
                System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
            }

            assertTrue(firstResult.hasObjectReference());
            assertTrue(secondResult.hasObjectReference());
            assertEquals(
                    TaskFlowObjectKeys.attemptOutputKey(
                            "job-remote",
                            "task-remote",
                            1,
                            firstAssignmentId
                    ),
                    firstResult.objectReference().key()
            );
            assertEquals(
                    TaskFlowObjectKeys.attemptOutputKey(
                            "job-remote",
                            "task-remote",
                            2,
                            secondAssignmentId
                    ),
                    secondResult.objectReference().key()
            );
            assertNotEquals(
                    firstResult.objectReference().key(),
                    secondResult.objectReference().key()
            );
            try (ObjectStore store = provider().open();
                 InputStream firstOutput = store.get(firstResult.objectReference().key());
                 InputStream secondOutput = store.get(secondResult.objectReference().key())) {
                assertNotNull(ImageIO.read(firstOutput));
                assertNotNull(ImageIO.read(secondOutput));
            }
        } finally {
            try (ObjectStore store = provider().open()) {
                store.delete(wirePayload.objectReference().key());
                if (firstResult != null && firstResult.hasObjectReference()) {
                    store.delete(firstResult.objectReference().key());
                }
                if (secondResult != null && secondResult.hasObjectReference()) {
                    store.delete(secondResult.objectReference().key());
                }
            }
        }
    }

    @Test
    void corruptObjectBytesAreRejectedBeforeImageProcessing() throws Exception {
        byte[] expectedBytes = pngBytes();
        byte[] corruptBytes = expectedBytes.clone();
        corruptBytes[corruptBytes.length - 1] ^= 1;
        String key = TaskFlowObjectKeys.objectKey(
                "inputs",
                UUID.randomUUID().toString(),
                "corrupt-image"
        );
        ObjectReference reference = new ObjectReference(
                key,
                expectedBytes.length,
                HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(expectedBytes)
                ),
                "image/png"
        );
        try {
            try (MinioClient client = minioClient()) {
                client.putObject(
                        PutObjectArgs.builder()
                                .bucket(BUCKET)
                                .object(key)
                                .stream(
                                        new ByteArrayInputStream(corruptBytes),
                                        (long) corruptBytes.length,
                                        -1L
                                )
                                .contentType(reference.contentType())
                                .userMetadata(Map.of(
                                        "taskflow-sha256",
                                        reference.sha256()
                                ))
                                .build()
                );
            }
            FilePayload input = new FilePayload("corrupt.png", null, reference);
            TaskAssignMessage assignment = new TaskAssignMessage(
                    "coordinator-1",
                    Instant.now().toString(),
                    "task-corrupt",
                    "job-corrupt",
                    ConversionTaskTypes.IMAGE_CONVERSION,
                    1,
                    "550e8400-e29b-41d4-a716-446655440001",
                    1_780_000_000_000L,
                    input,
                    "png"
            );

            PayloadIntegrityException failure = assertThrows(
                    PayloadIntegrityException.class,
                    () -> new ImageConversionProcessor(provider()).process(assignment)
            );

            assertEquals(PayloadIntegrityException.Mismatch.SHA256, failure.mismatch());
            assertEquals(reference.sha256(), failure.expectedSha256());
        } finally {
            try (ObjectStore store = provider().open()) {
                store.delete(key);
            }
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

    private static MinioObjectStoreProvider provider() {
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
        return new MinioObjectStoreProvider(
                "http://" + MINIO.getHost() + ":" + bindings[0].getHostPortSpec(),
                MINIO.getUserName(),
                MINIO.getPassword(),
                BUCKET
        );
    }

    private static byte[] pngBytes() throws Exception {
        BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, Color.RED.getRGB());
        image.setRGB(1, 0, Color.GREEN.getRGB());
        image.setRGB(0, 1, Color.BLUE.getRGB());
        image.setRGB(1, 1, Color.WHITE.getRGB());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ImageIO.write(image, "png", output);
        return output.toByteArray();
    }
}
