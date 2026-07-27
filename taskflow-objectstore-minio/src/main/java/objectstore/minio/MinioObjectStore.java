package objectstore.minio;

import io.minio.CopyObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.SourceObject;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
import objectstore.ObjectListing;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.TaskFlowObjectKeys;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * MinIO/S3-compatible adapter for the framework-owned {@link ObjectStore} port.
 *
 * <p>The adapter owns the supplied client and closes it when this store is
 * closed. Bucket creation and runtime configuration are deliberately outside
 * this TF-0501 adapter boundary.</p>
 */
public final class MinioObjectStore implements ObjectStore {
    private static final String SHA256_METADATA_KEY = "taskflow-sha256";
    private static final Set<String> NOT_FOUND_CODES = Set.of("NoSuchKey", "NoSuchObject");

    private final MinioClient client;
    private final String bucket;

    public MinioObjectStore(MinioClient client, String bucket) {
        this.client = Objects.requireNonNull(client, "client");
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("bucket is required.");
        }
        this.bucket = bucket.trim();
    }

    @Override
    public ObjectReference put(ObjectReference reference, InputStream content) throws ObjectStoreException {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(content, "content");
        try {
            client.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(reference.key())
                            .stream(content, reference.contentLength(), -1L)
                            .contentType(reference.contentType())
                            .userMetadata(java.util.Map.of(SHA256_METADATA_KEY, reference.sha256()))
                            .build()
            );
            return stat(reference.key());
        } catch (MinioException | IllegalStateException e) {
            throw translate("put", reference.key(), e);
        }
    }

    @Override
    public InputStream get(String key) throws ObjectStoreException {
        String validatedKey = TaskFlowObjectKeys.requireObjectKey(key);
        try {
            return client.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucket)
                            .object(validatedKey)
                            .build()
            );
        } catch (MinioException | IllegalStateException e) {
            throw translate("get", validatedKey, e);
        }
    }

    @Override
    public ObjectReference stat(String key) throws ObjectStoreException {
        String validatedKey = TaskFlowObjectKeys.requireObjectKey(key);
        try {
            StatObjectResponse response = client.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucket)
                            .object(validatedKey)
                            .build()
            );
            return referenceFrom(validatedKey, response);
        } catch (MinioException | IllegalStateException e) {
            throw translate("stat", validatedKey, e);
        }
    }

    @Override
    public void delete(String key) throws ObjectStoreException {
        String validatedKey = TaskFlowObjectKeys.requireObjectKey(key);
        try {
            client.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(bucket)
                            .object(validatedKey)
                            .build()
            );
        } catch (MinioException | IllegalStateException e) {
            throw translate("delete", validatedKey, e);
        }
    }

    @Override
    public ObjectReference copy(String sourceKey, String destinationKey) throws ObjectStoreException {
        String validatedSource = TaskFlowObjectKeys.requireObjectKey(sourceKey);
        String validatedDestination = TaskFlowObjectKeys.requireObjectKey(destinationKey);
        try {
            client.copyObject(
                    CopyObjectArgs.builder()
                            .bucket(bucket)
                            .object(validatedDestination)
                            .source(
                                    SourceObject.builder()
                                            .bucket(bucket)
                                            .object(validatedSource)
                                            .build()
                            )
                            .build()
            );
            return stat(validatedDestination);
        } catch (MinioException | IllegalStateException e) {
            throw translate("copy", validatedSource, e);
        }
    }

    @Override
    public ObjectListing list(String prefix, String startAfter, int limit) throws ObjectStoreException {
        String validatedPrefix = TaskFlowObjectKeys.requirePrefix(prefix);
        String validatedStartAfter = startAfter == null
                ? null
                : TaskFlowObjectKeys.requireStartAfter(validatedPrefix, startAfter);
        int validatedLimit = ObjectStore.requireListLimit(limit);

        List<ObjectReference> objects = new ArrayList<>(validatedLimit);
        try {
            ListObjectsArgs.Builder builder = ListObjectsArgs.builder()
                    .bucket(bucket)
                    .prefix(validatedPrefix)
                    .recursive(true)
                    .maxKeys(validatedLimit);
            if (validatedStartAfter != null) {
                builder.startAfter(validatedStartAfter);
            }
            for (Result<Item> result : client.listObjects(builder.build())) {
                Item item = result.get();
                if (item.isDir()) {
                    continue;
                }
                objects.add(stat(item.objectName()));
                if (objects.size() == validatedLimit) {
                    break;
                }
            }
        } catch (MinioException | IllegalStateException e) {
            throw translate("list", validatedPrefix, e);
        }
        String nextStartAfter = objects.size() == validatedLimit
                ? objects.getLast().key()
                : null;
        return new ObjectListing(objects, nextStartAfter);
    }

    @Override
    public void close() throws ObjectStoreException {
        try {
            client.close();
        } catch (Exception e) {
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.STORAGE_FAILURE,
                    "MinIO client close failed.",
                    e
            );
        }
    }

    private ObjectReference referenceFrom(String key, StatObjectResponse response) throws ObjectStoreException {
        String sha256 = response.userMetadata().getFirst(SHA256_METADATA_KEY);
        try {
            return new ObjectReference(key, response.size(), sha256, response.contentType());
        } catch (IllegalArgumentException e) {
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.INVALID_METADATA,
                    "Object metadata is invalid for key '" + key + "'.",
                    e
            );
        }
    }

    private ObjectStoreException translate(String operation, String key, Throwable failure) {
        if (failure instanceof ErrorResponseException response
                && NOT_FOUND_CODES.contains(response.errorResponse().code())) {
            return new ObjectStoreException(
                    ObjectStoreException.Reason.NOT_FOUND,
                    "Object not found during " + operation + ": " + key,
                    failure
            );
        }
        return new ObjectStoreException(
                ObjectStoreException.Reason.STORAGE_FAILURE,
                "Object-store " + operation + " failed for '" + key + "'.",
                failure
        );
    }
}
