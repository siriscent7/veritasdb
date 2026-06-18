package com.veritasdb.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class WriteAheadLog {

    private static final String SEP = "|";
    private static final Pattern SEP_PATTERN = Pattern.compile(Pattern.quote(SEP));

    private final Path path;
    private final BufferedWriter writer;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path;
        Files.createDirectories(path.toAbsolutePath().getParent());
        this.writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized void logPut(String key, String value) throws IOException {
        writer.write("PUT" + SEP + key + SEP + value);
        writer.newLine();
        writer.flush();
    }

    public synchronized void logDelete(String key) throws IOException {
        writer.write("DEL" + SEP + key);
        writer.newLine();
        writer.flush();
    }

    public List<Record> replay() throws IOException {
        List<Record> records = new ArrayList<>();
        if (!Files.exists(path)) return records;
        for (String line : Files.readAllLines(path)) {
            if (line.isBlank()) continue;
            String[] parts = SEP_PATTERN.split(line, -1);
            switch (parts[0]) {
                case "PUT" -> records.add(new Record(parts[1], ValueEntry.of(parts[2])));
                case "DEL" -> records.add(new Record(parts[1], ValueEntry.deleted()));
                default -> throw new IOException("Corrupt WAL record: " + line);
            }
        }
        return records;
    }

    public synchronized void close() throws IOException {
        writer.close();
    }

    public synchronized void truncate() throws IOException {
        writer.close();
        Files.deleteIfExists(path);
    }

    public record Record(String key, ValueEntry value) {}
}
