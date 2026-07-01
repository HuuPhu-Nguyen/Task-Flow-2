package server.registry;

import java.util.List;

public interface PeerRegistryStore {
    boolean upsertPeerRecord(PeerRegistryRecord record);

    List<PeerRegistryRecord> loadPeerRecords();
}
