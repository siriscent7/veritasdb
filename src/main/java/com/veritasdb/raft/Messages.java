package com.veritasdb.raft;

import java.util.List;

/** Raft RPC request/response message types. */
public final class Messages {

    // --- RequestVote (election) ---
    public record RequestVote(long term, int candidateId,
                              int lastLogIndex, long lastLogTerm) {}

    public record VoteResponse(long term, boolean voteGranted) {}

    // --- AppendEntries (replication + heartbeat) ---
    public record AppendEntries(long term, int leaderId,
                                int prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, int leaderCommit) {}

    public record AppendResponse(long term, boolean success, int matchIndex) {}

    private Messages() {}
}
