package com.veritasdb.storage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * LSM engine: memtable + WAL + immutable SSTables.
 *   - Writes go to WAL (durability) then the memtable.
 *   - When the memtable exceeds a threshold, it is flushed to an SSTable
 *     and the WAL is truncated.
 *   - Reads check the memtable first, then SSTables newest -> oldest.
 */
public final class LsmEngine implements AutoCloseable {

    private static final int FLUSH_THRESHOLD = 3; // small, so demos trigger flushes

    private final Path dataDir;
    private Memtable memtable = new Memtable();
    private WriteAheadLog wal;

    // SSTables ordered oldest -> newest (we read in reverse for newest-first)
    private final List<SSTable> ssTables = new ArrayList<>();
    private int ssTableCounter = 0;

    public LsmEngine(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        discoverSSTables();
        this.wal = new WriteAheadLog(dataDir.resolve("wal.log"));
        recoverFromWal();
    }

    private void discoverSSTables() throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dataDir, "*.sst")) {
            ds.forEach(files::add);
        }
        files.sort(java.util.Comparator.comparing(p -> p.getFileName().toString()));
        for (Path p : files) {
            ssTables.add(SSTable.open(p));
            ssTableCounter++;
        }
    }

    private void recoverFromWal() throws IOException {
        for (WriteAheadLog.Record r : wal.replay()) {
            if (r.value().tombstone()) memtable.delete(r.key());
            else memtable.put(r.key(), r.value().value());
        }
    }

    public void put(String key, String value) throws IOException {
        wal.logPut(key, value);
        memtable.put(key, value);
        maybeFlush();
    }

    public void delete(String key) throws IOException {
        wal.logDelete(key);
        memtable.delete(key);
        maybeFlush();
    }

    public Optional<String> get(String key) throws IOException {
        // 1. memtable (newest data)
        ValueEntry e = memtable.get(key);
        if (e != null) return e.tombstone() ? Optional.empty() : Optional.of(e.value());

        // 2. SSTables, newest -> oldest
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            ValueEntry se = ssTables.get(i).get(key);
            if (se != null) return se.tombstone() ? Optional.empty() : Optional.of(se.value());
        }
        return Optional.empty();
    }

    private void maybeFlush() throws IOException {
        if (memtable.size() >= FLUSH_THRESHOLD) {
            flush();
        }
    }

    /** Flush the memtable to a new SSTable, then reset the memtable and WAL. */
    public void flush() throws IOException {
        if (memtable.size() == 0) return;
        Path ssPath = dataDir.resolve(String.format("sstable-%05d.sst", ssTableCounter++));
        SSTable table = SSTable.write(ssPath, memtable.entries());
        ssTables.add(table);

        // reset memtable + WAL
        memtable = new Memtable();
        wal.truncate();
        wal = new WriteAheadLog(dataDir.resolve("wal.log"));
    }

    public int memtableSize() { return memtable.size(); }
    public int ssTableCount() { return ssTables.size(); }

    @Override public void close() throws IOException {
        wal.close();
    }
}