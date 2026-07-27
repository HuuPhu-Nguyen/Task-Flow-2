package objectstore;

/**
 * Runtime construction boundary for a configured object-store adapter.
 *
 * <p>Providers are discovered through {@link java.util.ServiceLoader}. Keeping
 * construction in the SPI lets participant and coordinator runtimes include
 * an adapter without coupling conversion plugins or coordinator source to a
 * vendor SDK.</p>
 */
@FunctionalInterface
public interface ObjectStoreProvider {
    ObjectStore open() throws ObjectStoreException;
}
