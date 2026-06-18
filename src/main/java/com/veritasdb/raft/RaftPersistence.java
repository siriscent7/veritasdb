package com.veritasdb.raft;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Persists Raft state to disk so it survives restarts:
 *   - meta file:  "currentTerm|votedFor"   (votedFor = -1 means null)
 *   - log file:   one entry per line "term|index|command"
 *
 * Raft safety requires currentTerm, votedFor, and the log to be durable
 * before responding to RPCs, so a crashed node never votes twice in a term
 * or loses committed entries.
 */
public final class RaftPersistence {

    private static final String SEP = "|";
    private static final Pattern SEP_PATTERN = Pattern.compile(Pattern.quote(SEP));

    private final Path metaFile;
    private final Path logFile;

    public RaftPersistence(Path dir, int nodeId) throws IOException {
        Files.createDirectories(dir);
        this.metaFile = dir.resolve("raft-" + nodeId + ".meta");
        this.logFile = dir.resolve("raft-" + nodeId + ".log");
    }

    // ---- meta (term + votedFor) ----

    public void saveMeta(long currentTerm, Integer votedFor) throws IOException {
        int vf = (votedFor == null) ? -1 : votedFor;
        try (BufferedWriter w = Files.newBufferedWriter(metaFile)) {
            w.write(currentTerm + SEP + vf);
        }
    }

    /** Returns {currentTerm, votedFor} where votedFor == -1 means null. */
    public long[] loadMeta() throws IOException {
        if (!Files.exists(metaFile)) return new long[]{0, -1};
        String line = Files.readString(metaFile).trim();
        if (line.isEmpty()) return new long[]{0, -1};
        String[] parts = SEP_PATTERN.split(line, -1);
        return new long[]{Long.parseLong(parts[0]), Long.parseLong(parts[1])};
    }

    // ---- log ----

    public void saveLog(List<LogEntry> log) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(logFile)) {
            for (LogEntry e : log) {
                w.write(e.term() + SEP + e.index() + SEP + e.command());
                w.newLine();
            }
        }
    }

    public List<LogEntry> loadLog() throws IOException {
        List<LogEntry> log = new ArrayList<>();
        if (!Files.exists(logFile)) return log;
        for (String line : Files.readAllLines(logFile)) {
            if (line.isBlank()) continue;
            String[] parts = SEP_PATTERN.split(line, -1);
            log.add(new LogEntry(
                    Long.parseLong(parts[0]),
                    Integer.parseInt(parts[1]),
                    parts.length > 2 ? parts[2] : ""));
        }
        return log;
    }
}