package peer.processors;

import objectstore.ObjectListing;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.PayloadIntegrityVerifier;
import objectstore.TaskFlowObjectKeys;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class ProcessorMemoryObjectStore implements ObjectStore {
    private final Map<String, StoredObject> objects = new ConcurrentHashMap<>();

    @Override
    public ObjectReference put(ObjectReference reference, InputStream content)
            throws ObjectStoreException {
        StoredObject stored = read(reference, content);
        objects.put(reference.key(), stored);
        return reference;
    }

    @Override
    public ObjectReference putIfAbsent(ObjectReference reference, InputStream content)
            throws ObjectStoreException {
        StoredObject stored = read(reference, content);
        if (objects.putIfAbsent(reference.key(), stored) != null) {
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.ALREADY_EXISTS,
                    "Test object already exists."
            );
        }
        return reference;
    }

    @Override
    public InputStream get(String key) throws ObjectStoreException {
        StoredObject stored = requireStored(key);
        return new ByteArrayInputStream(stored.content());
    }

    @Override
    public ObjectReference stat(String key) throws ObjectStoreException {
        return requireStored(key).reference();
    }

    @Override
    public void delete(String key) {
        objects.remove(TaskFlowObjectKeys.requireObjectKey(key));
    }

    @Override
    public ObjectReference copy(String sourceKey, String destinationKey)
            throws ObjectStoreException {
        throw new ObjectStoreException(
                ObjectStoreException.Reason.STORAGE_FAILURE,
                "Copy is not used by this test."
        );
    }

    @Override
    public ObjectListing list(String prefix, String startAfter, int limit) {
        return new ObjectListing(List.of(), null);
    }

    @Override
    public void close() {
        // Shared test instance deliberately survives separate provider opens.
    }

    private StoredObject read(ObjectReference reference, InputStream content)
            throws ObjectStoreException {
        try {
            return new StoredObject(
                    reference,
                    PayloadIntegrityVerifier.readVerified(
                            content,
                            reference,
                            reference.contentLength()
                    )
            );
        } catch (IOException e) {
            if (e instanceof ObjectStoreException objectStoreException) {
                throw objectStoreException;
            }
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.STORAGE_FAILURE,
                    "Test upload failed.",
                    e
            );
        }
    }

    private StoredObject requireStored(String key) throws ObjectStoreException {
        String validatedKey = TaskFlowObjectKeys.requireObjectKey(key);
        StoredObject stored = objects.get(validatedKey);
        if (stored == null) {
            throw new ObjectStoreException(
                    ObjectStoreException.Reason.NOT_FOUND,
                    "Missing test object."
            );
        }
        return stored;
    }

    private record StoredObject(ObjectReference reference, byte[] content) {
    }
}
