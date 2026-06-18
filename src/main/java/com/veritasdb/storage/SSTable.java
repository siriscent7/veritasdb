package com.veritasdb.storage;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class SSTable {

    private static final String SEP = "|";

    private final Path path;
    private BloomFilter bloom;
    private NavigableMap<String, ValueEntry> cache; // lazily loaded

    private SSTable(Path path) {
        this.path = path;
    }

    public static SSTable write(Path path, NavigableMap<String, ValueEntry> entries)
            throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            for (var e : entries.entrySet()) {
                ValueEntry v = e.getValue();
                if (v.tombstone()) w.write(e.getKey() + SEP + "D");
                else w.write(e.getKey() + SEP + "P" + SEP + v.value());
                w.newLine();
            }
        }
        SSTable t = new SSTable(path);
        t.buildBloom(entries.keySet());
        return t;
    }

    public static SSTable open(Path path) throws IOException {
        SSTable t = new SSTable(path);
        t.buildBloom(t.load().keySet()); // build bloom from disk on open
        return t;
    }

    private void buildBloom(java.util.Set<String> keys) {
        bloom = new BloomFilter(Math.max(16, keys.size()));
        for (String k : keys) bloom.add(k);
    }

    public NavigableMap<String, ValueEntry> load() throws IOException {
        if (cache != null) return cache;
        NavigableMap<String, ValueEntry> map = new TreeMap<>();
        for (String line : Files.readAllLines(path)) {
            if (line.isBlank()) continue;
            String[] parts = line.split(java.util.regex.Pattern.quote(SEP), -1);
            String key = parts[0];
            if (parts[1].equals("D")) map.put(key, ValueEntry.deleted());
            else map.put(key, ValueEntry.of(parts[2]));
        }
        cache = map;
        return map;
    }

    /** Fast pre-check: false => key is definitely not in this table. */
    public boolean mightContain(String key) {
        return bloom.mightContain(key);
    }

    public ValueEntry get(String key) throws IOException {
        if (!mightContain(key)) return null; // skip disk read entirely
        return load().get(key);
    }

    public Path path() { return path; }
}