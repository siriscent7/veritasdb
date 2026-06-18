package com.veritasdb.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public final class LsmEngine implements AutoCloseable {

    private final Memtable memtable = new Memtable();
    private final WriteAheadLog wal;

    public LsmEngine(Path dataDir) throws IOException {
        this.wal = new WriteAheadLog(dataDir.resolve("wal.log"));
        recover();
    }

    private void recover() throws IOException {
        for (WriteAheadLog.Record r : wal.replay()) {
            if (r.value().tombstone()) {
                memtable.delete(r.key());
            } else {
                memtable.put(r.key(), r.value().value());
            }
        }
    }

    public void put(String key, String value) throws IOException {
        wal.logPut(key, value);   // durability first
        memtable.put(key, value);
    }

    public void delete(String key) throws IOException {
        wal.logDelete(key);
        memtable.delete(key);
    }

    public Optional<String> get(String key) {
        ValueEntry e = memtable.get(key);
        if (e == null || e.tombstone()) return Optional.empty();
        return Optional.of(e.value());
    }

    public int size() { return memtable.size(); }

    @Override public void close() throws IOException {
        wal.close();
    }
}