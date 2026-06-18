package com.veritasdb.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-version store: each key holds a list of versions ordered by commit timestamp.
 * Reads at a snapshot timestamp see the newest version whose commitTs <= snapshot.
 */
public final class MvccStore {

    private final Map<String, List<VersionedValue>> versions = new ConcurrentHashMap<>();

    /** Read the value visible at the given snapshot timestamp. */
    public synchronized Optional<String> read(String key, long snapshotTs) {
        List<VersionedValue> list = versions.get(key);
        if (list == null) return Optional.empty();

        // Pick the newest version with commitTs <= snapshotTs (scan all, no early break)
        VersionedValue visible = null;
        for (VersionedValue v : list) {
            if (v.commitTs() <= snapshotTs) {
                if (visible == null || v.commitTs() > visible.commitTs()) {
                    visible = v;
                }
            }
        }
        if (visible == null || visible.tombstone()) return Optional.empty();
        return Optional.of(visible.value());
    }

    public synchronized long latestCommitTs(String key) {
        List<VersionedValue> list = versions.get(key);
        if (list == null || list.isEmpty()) return -1;
        long max = -1;
        for (VersionedValue v : list) {
            if (v.commitTs() > max) max = v.commitTs();
        }
        return max;
    }

    public synchronized void commitVersion(String key, VersionedValue v) {
        versions.computeIfAbsent(key, k -> new ArrayList<>()).add(v);
    }
}
