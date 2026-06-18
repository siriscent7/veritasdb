package com.veritasdb.raft;

/**
 * A single Raft log entry: the term it was created in, its index,
 * and the command (an opaque string, e.g. "PUT key value").
 */
public record LogEntry(long term, int index, String command) {}
