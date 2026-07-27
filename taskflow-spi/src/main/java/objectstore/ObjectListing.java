package objectstore;

import java.util.List;

/**
 * One bounded, lexically ordered object-listing page.
 *
 * @param objects returned object metadata
 * @param nextStartAfter last returned key when another page may exist, otherwise {@code null}
 */
public record ObjectListing(
        List<ObjectMetadata> objects,
        String nextStartAfter
) {
    public ObjectListing {
        if (objects == null) {
            throw new IllegalArgumentException("objects are required.");
        }
        objects = List.copyOf(objects);
        if (nextStartAfter != null) {
            nextStartAfter = TaskFlowObjectKeys.requireObjectKey(nextStartAfter);
            if (objects.isEmpty() || !nextStartAfter.equals(objects.getLast().key())) {
                throw new IllegalArgumentException("nextStartAfter must be the last returned object key.");
            }
        }
    }
}
