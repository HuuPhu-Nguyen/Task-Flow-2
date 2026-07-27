package objectstore.minio;

import io.minio.MinioClient;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.ObjectStoreProvider;

/**
 * Environment-backed MinIO/S3-compatible runtime provider.
 */
public final class MinioObjectStoreProvider implements ObjectStoreProvider {
    public static final String ENDPOINT_PROPERTY = "taskflow.minioEndpoint";
    public static final String ENDPOINT_ENV = "TASKFLOW_MINIO_ENDPOINT";
    public static final String ACCESS_KEY_PROPERTY = "taskflow.minioAccessKey";
    public static final String ACCESS_KEY_ENV = "TASKFLOW_MINIO_ACCESS_KEY";
    public static final String SECRET_KEY_PROPERTY = "taskflow.minioSecretKey";
    public static final String SECRET_KEY_ENV = "TASKFLOW_MINIO_SECRET_KEY";
    public static final String BUCKET_PROPERTY = "taskflow.minioBucket";
    public static final String BUCKET_ENV = "TASKFLOW_MINIO_BUCKET";
    public static final String DEFAULT_ENDPOINT = "http://localhost:9000";
    public static final String DEFAULT_BUCKET = "taskflow";

    private final Configuration explicitConfiguration;

    public MinioObjectStoreProvider() {
        explicitConfiguration = null;
    }

    public MinioObjectStoreProvider(String endpoint,
                                    String accessKey,
                                    String secretKey,
                                    String bucket) {
        explicitConfiguration = new Configuration(
                requireText(endpoint, "MinIO endpoint"),
                requireText(accessKey, "MinIO access key"),
                requireText(secretKey, "MinIO secret key"),
                requireText(bucket, "MinIO bucket")
        );
    }

    @Override
    public ObjectStore open() throws ObjectStoreException {
        Configuration configuration = explicitConfiguration == null
                ? configuredEnvironment()
                : explicitConfiguration;
        try {
            MinioClient client = MinioClient.builder()
                    .endpoint(configuration.endpoint())
                    .credentials(configuration.accessKey(), configuration.secretKey())
                    .build();
            return new MinioObjectStore(client, configuration.bucket());
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.STORAGE_FAILURE,
                    "MinIO object-store configuration is invalid.",
                    e
            );
        }
    }

    private static Configuration configuredEnvironment() throws ObjectStoreException {
        String endpoint = configuredValue(ENDPOINT_PROPERTY, ENDPOINT_ENV, DEFAULT_ENDPOINT);
        String accessKey = configuredValue(ACCESS_KEY_PROPERTY, ACCESS_KEY_ENV, null);
        String secretKey = configuredValue(SECRET_KEY_PROPERTY, SECRET_KEY_ENV, null);
        String bucket = configuredValue(BUCKET_PROPERTY, BUCKET_ENV, DEFAULT_BUCKET);
        if (accessKey == null || secretKey == null) {
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.STORAGE_FAILURE,
                    ACCESS_KEY_ENV + " and " + SECRET_KEY_ENV
                            + " are required for object-backed payloads."
            );
        }
        return new Configuration(endpoint, accessKey, secretKey, bucket);
    }

    private static String configuredValue(String propertyName,
                                          String environmentName,
                                          String defaultValue) {
        String value = System.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            value = System.getenv(environmentName);
        }
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required.");
        }
        return value.trim();
    }

    private record Configuration(
            String endpoint,
            String accessKey,
            String secretKey,
            String bucket
    ) {
    }
}
