package com.veritasdb.storage;
import java.util.concurrent.ConcurrentSkipListMap;

public final class Memtable {

    private final ConcurrentSkipListMap<String, ValueEntry> map = new ConcurrentSkipListMap<>();

    public void put(String key, String value) {
        map.put(key, ValueEntry.of(value));
    }

    public void delete(String key) {
        map.put(key, ValueEntry.deleted()); // tombstone, not removal
    }

    /** Returns the entry (which may be a tombstone), or null if the key was never written. */
    public ValueEntry get(String key) {
        return map.get(key);
    }

    public int size() { return map.size(); }

    public java.util.NavigableMap<String, ValueEntry> entries() { return map; }
}