package objectstore;

import java.io.InputStream;

/**
 * Streaming data-plane boundary for TaskFlow-owned objects.
 *
 * <p>Implementations must preserve the supplied metadata. End-to-end digest
 * calculation and content verification are separate consumers of this port
 * and are introduced by TF-0504.</p>
 */
public interface ObjectStore extends AutoCloseable {
    int MAX_LIST_PAGE_SIZE = 1_000;

    ObjectReference put(ObjectReference reference, InputStream content) throws ObjectStoreException;

    InputStream get(String key) throws ObjectStoreException;

    ObjectReference stat(String key) throws ObjectStoreException;

    void delete(String key) throws ObjectStoreException;

    ObjectReference copy(String sourceKey, String destinationKey) throws ObjectStoreException;

    ObjectListing list(String prefix, String startAfter, int limit) throws ObjectStoreException;

    default ObjectListing list(String prefix, int limit) throws ObjectStoreException {
        return list(prefix, null, limit);
    }

    static int requireListLimit(int limit) {
        if (limit < 1 || limit > MAX_LIST_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "List limit must be between 1 and " + MAX_LIST_PAGE_SIZE + "."
            );
        }
        return limit;
    }

    @Override
    void close() throws ObjectStoreException;
}
