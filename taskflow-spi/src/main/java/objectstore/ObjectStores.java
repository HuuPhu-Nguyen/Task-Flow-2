package objectstore;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Discovers the single object-store provider packaged with a runtime.
 */
public final class ObjectStores {
    private ObjectStores() {
    }

    public static ObjectStore open() throws ObjectStoreException {
        try {
            Iterator<ObjectStoreProvider> providers =
                    ServiceLoader.load(ObjectStoreProvider.class).iterator();
            if (!providers.hasNext()) {
                throw storageFailure(
                        "No ObjectStoreProvider is installed in this runtime.",
                        null
                );
            }
            ObjectStoreProvider provider = providers.next();
            if (providers.hasNext()) {
                throw storageFailure(
                        "Multiple ObjectStoreProvider implementations are installed; exactly one is required.",
                        null
                );
            }
            return provider.open();
        } catch (ServiceConfigurationError e) {
            throw storageFailure("Object-store provider discovery failed.", e);
        }
    }

    private static ObjectStoreException storageFailure(String message, Throwable cause) {
        if (cause == null) {
            return new ObjectStoreException(ObjectStoreException.Reason.STORAGE_FAILURE, message);
        }
        return new ObjectStoreException(
                ObjectStoreException.Reason.STORAGE_FAILURE,
                message,
                cause
        );
    }
}
