package objectstore.minio;

import objectstore.ObjectListing;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.TaskFlowObjectKeys;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryObjectStore implements ObjectStore {
    private final Map<String, StoredObject> objects = new ConcurrentHashMap<>();
    private final boolean available;
    private boolean closed;

    InMemoryObjectStore() {
        this(true);
    }

    InMemoryObjectStore(boolean available) {
        this.available = available;
    }

    @Override
    public ObjectReference put(ObjectReference reference, InputStream content) throws ObjectStoreException {
        requireAvailable("put", reference.key());
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            content.transferTo(bytes);
            objects.put(reference.key(), new StoredObject(reference, bytes.toByteArray()));
            return reference;
        } catch (IOException e) {
            throw storageFailure("put", reference.key(), e);
        }
    }

    @Override
    public InputStream get(String key) throws ObjectStoreException {
        String validatedKey = TaskFlowObjectKeys.requireObjectKey(key);
        requireAvailable("get", validatedKey);
        StoredObject object = objects.get(validatedKey);
        if (object == null) {
            throw notFound("get", validatedKey);
        }
        return new ByteArrayInputStream(object.content());
    }

    @Override
    public ObjectReference stat(String key) throws ObjectStoreException {
        String validatedKey = TaskFlowObjectKeys.requireObjectKey(key);
        requireAvailable("stat", validatedKey);
        StoredObject object = objects.get(validatedKey);
        if (object == null) {
            throw notFound("stat", validatedKey);
        }
        return object.reference();
    }

    @Override
    public void delete(String key) throws ObjectStoreException {
        String validatedKey = TaskFlowObjectKeys.requireObjectKey(key);
        requireAvailable("delete", validatedKey);
        objects.remove(validatedKey);
    }

    @Override
    public ObjectReference copy(String sourceKey, String destinationKey) throws ObjectStoreException {
        String source = TaskFlowObjectKeys.requireObjectKey(sourceKey);
        String destination = TaskFlowObjectKeys.requireObjectKey(destinationKey);
        requireAvailable("copy", source);
        StoredObject object = objects.get(source);
        if (object == null) {
            throw notFound("copy", source);
        }
        ObjectReference copied = new ObjectReference(
                destination,
                object.reference().contentLength(),
                object.reference().sha256(),
                object.reference().contentType()
        );
        objects.put(destination, new StoredObject(copied, object.content().clone()));
        return copied;
    }

    @Override
    public ObjectListing list(String prefix, String startAfter, int limit) throws ObjectStoreException {
        String validatedPrefix = TaskFlowObjectKeys.requirePrefix(prefix);
        String validatedStartAfter = startAfter == null
                ? null
                : TaskFlowObjectKeys.requireStartAfter(validatedPrefix, startAfter);
        int validatedLimit = ObjectStore.requireListLimit(limit);
        requireAvailable("list", validatedPrefix);

        ArrayList<ObjectReference> listed = objects.values().stream()
                .map(StoredObject::reference)
                .filter(reference -> reference.key().startsWith(validatedPrefix))
                .filter(reference -> validatedStartAfter == null
                        || reference.key().compareTo(validatedStartAfter) > 0)
                .sorted(Comparator.comparing(ObjectReference::key))
                .limit(validatedLimit)
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        String nextStartAfter = listed.size() == validatedLimit ? listed.getLast().key() : null;
        return new ObjectListing(listed, nextStartAfter);
    }

    @Override
    public void close() {
        closed = true;
    }

    private void requireAvailable(String operation, String key) throws ObjectStoreException {
        if (!available || closed) {
            throw storageFailure(operation, key, new IOException("In-memory object store is unavailable."));
        }
    }

    private static ObjectStoreException notFound(String operation, String key) {
        return new ObjectStoreException(
                ObjectStoreException.Reason.NOT_FOUND,
                "Object not found during " + operation + ": " + key
        );
    }

    private static ObjectStoreException storageFailure(String operation, String key, Throwable cause) {
        return new ObjectStoreException(
                ObjectStoreException.Reason.STORAGE_FAILURE,
                "In-memory object-store " + operation + " failed for '" + key + "'.",
                cause
        );
    }

    private record StoredObject(ObjectReference reference, byte[] content) {
    }
}
