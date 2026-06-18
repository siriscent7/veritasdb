package com.veritasdb.raft;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RaftTimerTest {

    @Test
    void autoElectsLeaderViaTicker() throws Exception {
        RaftCluster cluster = new RaftCluster(3);

        // start tickers on all nodes
        cluster.node(0).startTicker();
        cluster.node(1).startTicker();
        cluster.node(2).startTicker();

        // wait long enough for an election to fire (timeout is 1.5-3s)
        RaftNode leader = waitForLeader(cluster, 6000);

        assertNotNull(leader, "a leader should be elected automatically");
        assertEquals(RaftState.LEADER, leader.state());

        // cleanup
        cluster.node(0).stopTicker();
        cluster.node(1).stopTicker();
        cluster.node(2).stopTicker();
    }

    @Test
    void autoFailoverAfterLeaderDies() throws Exception {
        RaftCluster cluster = new RaftCluster(3);
        for (int i = 0; i < 3; i++) cluster.node(i).startTicker();

        RaftNode leader = waitForLeader(cluster, 6000);
        assertNotNull(leader);
        int oldLeaderId = leader.id();

        // kill the leader and stop its ticker
        cluster.node(oldLeaderId).stopTicker();
        cluster.kill(oldLeaderId);

        // a new leader should emerge among the survivors
        RaftNode newLeader = waitForNewLeader(cluster, oldLeaderId, 8000);
        assertNotNull(newLeader, "a new leader should be elected after failover");
        assertNotEquals(oldLeaderId, newLeader.id());

        for (int i = 0; i < 3; i++) cluster.node(i).stopTicker();
    }

    private RaftNode waitForLeader(RaftCluster cluster, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            RaftNode l = cluster.leader();
            if (l != null) return l;
            Thread.sleep(100);
        }
        return null;
    }

    private RaftNode waitForNewLeader(RaftCluster cluster, int excludeId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            RaftNode l = cluster.leader();
            if (l != null && l.id() != excludeId && cluster.isAlive(l.id())) return l;
            Thread.sleep(100);
        }
        return null;
    }
}