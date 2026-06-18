package com.veritasdb.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * A single Raft node implementing leader election and log replication.
 *
 * This is transport-agnostic: a RaftCluster (or, later, a network layer)
 * delivers RPCs by calling handleRequestVote / handleAppendEntries, and
 * sends out RPCs via the RaftTransport interface.
 */
public final class RaftNode {

    public interface Transport {
        Messages.VoteResponse sendRequestVote(int toNode, Messages.RequestVote rpc);
        Messages.AppendResponse sendAppendEntries(int toNode, Messages.AppendEntries rpc);
    }

    private final int id;
    private final int[] peers;          // other node ids
    private final Transport transport;

    // --- persistent state ---
    private long currentTerm = 0;
    private Integer votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    // --- volatile state ---
    private RaftState state = RaftState.FOLLOWER;
    private int commitIndex = -1;
    private Integer currentLeader = null;

    public RaftNode(int id, int[] peers, Transport transport) {
        this.id = id;
        this.peers = peers;
        this.transport = transport;
    }

    public synchronized int id() { return id; }
    public synchronized RaftState state() { return state; }
    public synchronized long currentTerm() { return currentTerm; }
    public synchronized Integer currentLeader() { return currentLeader; }
    public synchronized int logSize() { return log.size(); }
    public synchronized int commitIndex() { return commitIndex; }

    public synchronized String committedValueAt(int index) {
        if (index < 0 || index >= log.size()) return null;
        return log.get(index).command();
    }

    // ---------------- Leader Election ----------------

    /** Called when the election timeout fires: become candidate and request votes. */
    public synchronized void startElection() {
        state = RaftState.CANDIDATE;
        currentTerm++;
        votedFor = id;
        currentLeader = null;

        long termForElection = currentTerm;
        int votes = 1; // vote for self

        int lastIndex = log.size() - 1;
        long lastTerm = lastIndex >= 0 ? log.get(lastIndex).term() : 0;

        for (int peer : peers) {
            Messages.RequestVote rpc =
                new Messages.RequestVote(termForElection, id, lastIndex, lastTerm);
            Messages.VoteResponse resp = transport.sendRequestVote(peer, rpc);
            if (resp == null) continue;

            if (resp.term() > currentTerm) {
                stepDown(resp.term());
                return;
            }
            if (resp.voteGranted()) votes++;
        }

        // majority?
        if (state == RaftState.CANDIDATE && votes > (peers.length + 1) / 2) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        state = RaftState.LEADER;
        currentLeader = id;
        // send initial heartbeats
        sendHeartbeats();
    }

    /** RPC handler: another node is requesting our vote. */
    public synchronized Messages.VoteResponse handleRequestVote(Messages.RequestVote rpc) {
        if (rpc.term() > currentTerm) stepDown(rpc.term());

        boolean grant = false;
        if (rpc.term() == currentTerm
                && (votedFor == null || votedFor == rpc.candidateId())
                && isCandidateLogUpToDate(rpc.lastLogIndex(), rpc.lastLogTerm())) {
            grant = true;
            votedFor = rpc.candidateId();
        }
        return new Messages.VoteResponse(currentTerm, grant);
    }

    private boolean isCandidateLogUpToDate(int candLastIndex, long candLastTerm) {
        int lastIndex = log.size() - 1;
        long lastTerm = lastIndex >= 0 ? log.get(lastIndex).term() : 0;
        if (candLastTerm != lastTerm) return candLastTerm > lastTerm;
        return candLastIndex >= lastIndex;
    }

    // ---------------- Log Replication ----------------

    /** Leader API: append a client command and replicate it. */
    public synchronized boolean replicate(String command) {
        if (state != RaftState.LEADER) return false;

        int index = log.size();
        log.add(new LogEntry(currentTerm, index, command));

        int replicatedCount = 1; // leader has it
        for (int peer : peers) {
            if (sendAppendTo(peer)) replicatedCount++;
        }

        // commit if a majority have it
        if (replicatedCount > (peers.length + 1) / 2) {
            commitIndex = index;
            return true;
        }
        return false;
    }

    private void sendHeartbeats() {
        for (int peer : peers) sendAppendTo(peer);
    }

    private boolean sendAppendTo(int peer) {
        int prevLogIndex = log.size() - 2;
        long prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).term() : 0;
        List<LogEntry> entries = new ArrayList<>();
        if (!log.isEmpty()) entries.add(log.get(log.size() - 1));

        Messages.AppendEntries rpc = new Messages.AppendEntries(
                currentTerm, id, prevLogIndex, prevLogTerm, entries, commitIndex);
        Messages.AppendResponse resp = transport.sendAppendEntries(peer, rpc);
        if (resp == null) return false;
        if (resp.term() > currentTerm) { stepDown(resp.term()); return false; }
        return resp.success();
    }

    /** RPC handler: leader is sending entries (or a heartbeat). */
    public synchronized Messages.AppendResponse handleAppendEntries(Messages.AppendEntries rpc) {
        if (rpc.term() < currentTerm) {
            return new Messages.AppendResponse(currentTerm, false, -1);
        }
        if (rpc.term() > currentTerm) stepDown(rpc.term());

        // valid leader for this term
        state = RaftState.FOLLOWER;
        currentLeader = rpc.leaderId();

        // append entries (simplified: append the leader's latest entry if new)
        for (LogEntry e : rpc.entries()) {
            if (e.index() == log.size()) {
                log.add(e);
            } else if (e.index() < log.size()) {
                log.set(e.index(), e); // overwrite (conflict resolution, simplified)
            }
        }

        if (rpc.leaderCommit() > commitIndex) {
            commitIndex = Math.min(rpc.leaderCommit(), log.size() - 1);
        }
        return new Messages.AppendResponse(currentTerm, true, log.size() - 1);
    }

    // ---------------- Common ----------------

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        state = RaftState.FOLLOWER;
        votedFor = null;
        currentLeader = null;
    }

    public synchronized void receiveHeartbeatTick() {
        // used by tests/cluster to drive heartbeats from the leader
        if (state == RaftState.LEADER) sendHeartbeats();
    }
}
