package com.veritasdb.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An immutable, sorted on-disk table.
 * Format (one entry per line):  key|P|value   or   key|D   (tombstone)
 * Keys are written in sorted order so the whole table can be loaded
 * into a sorted map for fast lookups.
 */
public final class SSTable {

    private static final String SEP = "|";
    private final Path path;

    private SSTable(Path path) {
        this.path = path;
    }

    /** Write a sorted snapshot of entries to a new SSTable file. */
    public static SSTable write(Path path, NavigableMap<String, ValueEntry> entries)
            throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            for (var e : entries.entrySet()) {
                ValueEntry v = e.getValue();
                if (v.tombstone()) {
                    w.write(e.getKey() + SEP + "D");
                } else {
                    w.write(e.getKey() + SEP + "P" + SEP + v.value());
                }
                w.newLine();
            }
        }
        return new SSTable(path);
    }

    /** Open an existing SSTable file (does not load it yet). */
    public static SSTable open(Path path) {
        return new SSTable(path);
    }

    /** Load the full table into a sorted map (Phase 2 keeps it simple). */
    public NavigableMap<String, ValueEntry> load() throws IOException {
        NavigableMap<String, ValueEntry> map = new TreeMap<>();
        for (String line : Files.readAllLines(path)) {
            if (line.isBlank()) continue;
            String[] parts = line.split(java.util.regex.Pattern.quote(SEP), -1);
            String key = parts[0];
            if (parts[1].equals("D")) {
                map.put(key, ValueEntry.deleted());
            } else {
                map.put(key, ValueEntry.of(parts[2]));
            }
        }
        return map;
    }

    /** Look up a single key (loads the table; Phase 2c adds Bloom-filter skipping). */
    public ValueEntry get(String key) throws IOException {
        return load().get(key);
    }

    public Path path() { return path; }
}