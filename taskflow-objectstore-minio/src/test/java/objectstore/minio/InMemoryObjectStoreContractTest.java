package objectstore.minio;

import objectstore.ObjectStore;

class InMemoryObjectStoreContractTest extends ObjectStoreContractTest {
    @Override
    protected ObjectStore createStore() {
        return new InMemoryObjectStore();
    }

    @Override
    protected ObjectStore createUnavailableStore() {
        return new InMemoryObjectStore(false);
    }
}
