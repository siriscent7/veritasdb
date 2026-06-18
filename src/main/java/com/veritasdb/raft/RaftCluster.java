package com.veritasdb.raft;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-process Raft cluster: nodes communicate via direct method calls
 * (synchronous in-memory transport). Models message loss with a
 * simple "alive" flag per node so we can test partitions/failures.
 */
public final class RaftCluster {

    private final Map<Integer, RaftNode> nodes = new HashMap<>();
    private final Map<Integer, Boolean> alive = new HashMap<>();

    public RaftCluster(int nodeCount) {
        for (int i = 0; i < nodeCount; i++) {
            final int id = i;
            int[] peers = peersOf(id, nodeCount);
            RaftNode.Transport transport = new RaftNode.Transport() {
                @Override public Messages.VoteResponse sendRequestVote(int to, Messages.RequestVote rpc) {
                    if (!isAlive(id) || !isAlive(to)) return null; // can't send/receive
                    return nodes.get(to).handleRequestVote(rpc);
                }
                @Override public Messages.AppendResponse sendAppendEntries(int to, Messages.AppendEntries rpc) {
                    if (!isAlive(id) || !isAlive(to)) return null;
                    return nodes.get(to).handleAppendEntries(rpc);
                }
            };
            nodes.put(id, new RaftNode(id, peers, transport));
            alive.put(id, true);
        }
    }

    private int[] peersOf(int id, int n) {
        int[] peers = new int[n - 1];
        int idx = 0;
        for (int i = 0; i < n; i++) if (i != id) peers[idx++] = i;
        return peers;
    }

    public RaftNode node(int id) { return nodes.get(id); }
    public boolean isAlive(int id) { return Boolean.TRUE.equals(alive.get(id)); }
    public void kill(int id) { alive.put(id, false); }
    public void revive(int id) { alive.put(id, true); }

    /** Find the current leader, if any. */
    public RaftNode leader() {
        for (RaftNode n : nodes.values()) {
            if (isAlive(n.id()) && n.state() == RaftState.LEADER) return n;
        }
        return null;
    }
}
