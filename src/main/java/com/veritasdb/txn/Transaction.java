package com.veritasdb.txn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A transaction with snapshot isolation.
 *   - snapshotTs: the read timestamp captured at begin
 *   - writes: buffered until commit (write buffer)
 *   - readSet: keys read, for OCC conflict validation at commit
 */
public final class Transaction {

    private final long snapshotTs;
    private final Map<String, String> writes = new HashMap<>(); // null value => delete
    private final Set<String> readSet = new HashSet<>();
    private boolean active = true;

    public Transaction(long snapshotTs) {
        this.snapshotTs = snapshotTs;
    }

    public long snapshotTs() { return snapshotTs; }
    public boolean isActive() { return active; }
    public void close() { active = false; }

    public void bufferPut(String key, String value) { writes.put(key, value); }
    public void bufferDelete(String key) { writes.put(key, null); }

    public boolean hasLocalWrite(String key) { return writes.containsKey(key); }
    public String localWrite(String key) { return writes.get(key); }

    public void recordRead(String key) { readSet.add(key); }

    public Map<String, String> writes() { return writes; }
    public Set<String> readSet() { return readSet; }
}
