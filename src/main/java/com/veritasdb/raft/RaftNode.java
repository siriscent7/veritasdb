package com.veritasdb.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Raft node with leader election, log replication (with full conflict
 * resolution via nextIndex/matchIndex backtracking), durable persistence,
 * and async election/heartbeat timers.
 */
public final class RaftNode {

    public interface Transport {
        Messages.VoteResponse sendRequestVote(int toNode, Messages.RequestVote rpc);
        Messages.AppendResponse sendAppendEntries(int toNode, Messages.AppendEntries rpc);
    }

    private final int id;
    private final int[] peers;
    private final Transport transport;
    private final RaftPersistence persistence; // may be null

    // --- persistent state ---
    private long currentTerm = 0;
    private Integer votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    // --- volatile state ---
    private RaftState state = RaftState.FOLLOWER;
    private int commitIndex = -1;
    private Integer currentLeader = null;

    // --- leader-only per-follower state ---
    private final Map<Integer, Integer> nextIndex = new HashMap<>();
    private final Map<Integer, Integer> matchIndex = new HashMap<>();

    // --- async timer state ---
    private volatile long lastHeartbeatTime = System.currentTimeMillis();
    private volatile boolean tickerRunning = false;
    private Thread tickerThread;
    private final long electionTimeoutMin = 1500;
    private final long electionTimeoutMax = 3000;
    private final long heartbeatInterval = 500;
    private long currentElectionTimeout = randomTimeout();

    public RaftNode(int id, int[] peers, Transport transport) {
        this(id, peers, transport, null);
    }

    public RaftNode(int id, int[] peers, Transport transport, RaftPersistence persistence) {
        this.id = id;
        this.peers = peers;
        this.transport = transport;
        this.persistence = persistence;
        loadPersistentState();
    }

    private void loadPersistentState() {
        if (persistence == null) return;
        try {
            long[] meta = persistence.loadMeta();
            currentTerm = meta[0];
            votedFor = (meta[1] == -1) ? null : (int) meta[1];
            log.addAll(persistence.loadLog());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load Raft state", e);
        }
    }

    private void persistMeta() {
        if (persistence == null) return;
        try { persistence.saveMeta(currentTerm, votedFor); }
        catch (IOException e) { throw new UncheckedIOException("persist meta failed", e); }
    }

    private void persistLog() {
        if (persistence == null) return;
        try { persistence.saveLog(log); }
        catch (IOException e) { throw new UncheckedIOException("persist log failed", e); }
    }

    // --- accessors ---
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

    public synchronized void startElection() {
        state = RaftState.CANDIDATE;
        currentTerm++;
        votedFor = id;
        currentLeader = null;
        persistMeta();

        long termForElection = currentTerm;
        int votes = 1;

        int lastIndex = log.size() - 1;
        long lastTerm = lastIndex >= 0 ? log.get(lastIndex).term() : 0;

        for (int peer : peers) {
            Messages.RequestVote rpc =
                new Messages.RequestVote(termForElection, id, lastIndex, lastTerm);
            Messages.VoteResponse resp = transport.sendRequestVote(peer, rpc);
            if (resp == null) continue;
            if (resp.term() > currentTerm) { stepDown(resp.term()); return; }
            if (resp.voteGranted()) votes++;
        }

        if (state == RaftState.CANDIDATE && votes > (peers.length + 1) / 2) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        state = RaftState.LEADER;
        currentLeader = id;
        // initialize per-follower indices
        for (int peer : peers) {
            nextIndex.put(peer, log.size());   // optimistically next slot
            matchIndex.put(peer, -1);
        }
        sendHeartbeats();
    }

    public synchronized Messages.VoteResponse handleRequestVote(Messages.RequestVote rpc) {
        if (rpc.term() > currentTerm) stepDown(rpc.term());

        boolean grant = false;
        if (rpc.term() == currentTerm
                && (votedFor == null || votedFor == rpc.candidateId())
                && isCandidateLogUpToDate(rpc.lastLogIndex(), rpc.lastLogTerm())) {
            grant = true;
            votedFor = rpc.candidateId();
            persistMeta();
            resetElectionTimer();
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
        persistLog();
        matchIndex.put(id, index); // leader has it

        for (int peer : peers) {
            replicateTo(peer);
        }
        updateCommitIndex();
        return commitIndex >= index;
    }

    private void sendHeartbeats() {
        for (int peer : peers) replicateTo(peer);
        updateCommitIndex();
    }

    /**
     * Replicate to a single follower, backtracking nextIndex on mismatch
     * until the logs converge, then shipping all entries from that point.
     */
    private void replicateTo(int peer) {
        int ni = nextIndex.getOrDefault(peer, log.size());

        // Build entries from nextIndex onward.
        while (true) {
            int prevLogIndex = ni - 1;
            long prevLogTerm = (prevLogIndex >= 0 && prevLogIndex < log.size())
                    ? log.get(prevLogIndex).term() : 0;

            List<LogEntry> entries = new ArrayList<>();
            for (int i = ni; i < log.size(); i++) entries.add(log.get(i));

            Messages.AppendEntries rpc = new Messages.AppendEntries(
                    currentTerm, id, prevLogIndex, prevLogTerm, entries, commitIndex);
            Messages.AppendResponse resp = transport.sendAppendEntries(peer, rpc);

            if (resp == null) return;                 // unreachable; try next round
            if (resp.term() > currentTerm) { stepDown(resp.term()); return; }

            if (resp.success()) {
                int newMatch = log.size() - 1;
                matchIndex.put(peer, newMatch);
                nextIndex.put(peer, log.size());
                return;
            } else {
                // log mismatch: backtrack and retry
                ni = Math.max(0, ni - 1);
                nextIndex.put(peer, ni);
                if (ni == 0 && entries.size() == log.size()) {
                    // already sending everything from the start; follower must accept
                    // (its prevLogIndex check at -1 succeeds). Avoid infinite loop.
                    return;
                }
            }
        }
    }

    /** Advance commitIndex to the highest index replicated on a majority. */
    private void updateCommitIndex() {
        for (int idx = log.size() - 1; idx > commitIndex; idx--) {
            // only commit entries from the current term (Raft safety rule)
            if (log.get(idx).term() != currentTerm) continue;
            int count = 1; // leader
            for (int peer : peers) {
                if (matchIndex.getOrDefault(peer, -1) >= idx) count++;
            }
            if (count > (peers.length + 1) / 2) {
                commitIndex = idx;
                break;
            }
        }
    }

    /** RPC handler: leader is sending entries (or a heartbeat). */
    public synchronized Messages.AppendResponse handleAppendEntries(Messages.AppendEntries rpc) {
        if (rpc.term() < currentTerm) {
            return new Messages.AppendResponse(currentTerm, false, -1);
        }
        if (rpc.term() > currentTerm) stepDown(rpc.term());

        state = RaftState.FOLLOWER;
        currentLeader = rpc.leaderId();
        resetElectionTimer();

        // Consistency check: our log must contain prevLogIndex with prevLogTerm.
        int prevIdx = rpc.prevLogIndex();
        if (prevIdx >= 0) {
            if (prevIdx >= log.size() || log.get(prevIdx).term() != rpc.prevLogTerm()) {
                return new Messages.AppendResponse(currentTerm, false, -1); // mismatch
            }
        }

        // Append/overwrite entries starting right after prevLogIndex.
        boolean changed = false;
        int insertAt = prevIdx + 1;
        for (int i = 0; i < rpc.entries().size(); i++) {
            LogEntry e = rpc.entries().get(i);
            int pos = insertAt + i;
            if (pos < log.size()) {
                if (log.get(pos).term() != e.term()) {
                    // conflict: truncate everything from here and append
                    while (log.size() > pos) log.remove(log.size() - 1);
                    log.add(e);
                    changed = true;
                }
            } else {
                log.add(e);
                changed = true;
            }
        }
        if (changed) persistLog();

        if (rpc.leaderCommit() > commitIndex) {
            commitIndex = Math.min(rpc.leaderCommit(), log.size() - 1);
        }
        return new Messages.AppendResponse(currentTerm, true, log.size() - 1);
    }

    // ---------------- Async timers ----------------

    public void startTicker() {
        if (tickerRunning) return;
        tickerRunning = true;
        tickerThread = new Thread(this::tickLoop, "raft-ticker-" + id);
        tickerThread.setDaemon(true);
        tickerThread.start();
    }

    public void stopTicker() {
        tickerRunning = false;
        if (tickerThread != null) tickerThread.interrupt();
    }

    private void tickLoop() {
        while (tickerRunning) {
            try {
                RaftState s;
                synchronized (this) { s = state; }
                if (s == RaftState.LEADER) {
                    synchronized (this) { if (state == RaftState.LEADER) sendHeartbeats(); }
                    Thread.sleep(heartbeatInterval);
                } else {
                    long since = System.currentTimeMillis() - lastHeartbeatTime;
                    if (since >= currentElectionTimeout) {
                        startElection();
                        synchronized (this) { currentElectionTimeout = randomTimeout(); }
                    }
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception ignored) { }
        }
    }

    private long randomTimeout() {
        return electionTimeoutMin
                + (long) (Math.random() * (electionTimeoutMax - electionTimeoutMin));
    }

    private void resetElectionTimer() {
        lastHeartbeatTime = System.currentTimeMillis();
    }

    // ---------------- Common ----------------

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        state = RaftState.FOLLOWER;
        votedFor = null;
        currentLeader = null;
        persistMeta();
    }

    public synchronized void receiveHeartbeatTick() {
        if (state == RaftState.LEADER) sendHeartbeats();
    }
}