package com.veritasdb.raft;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogConflictTest {

    @Test
    void laggingFollowerCatchesUp() {
        // 3-node cluster, node 0 becomes leader
        RaftCluster cluster = new RaftCluster(3);
        cluster.node(0).startElection();
        RaftNode leader = cluster.leader();
        assertNotNull(leader);

        // replicate a few entries with all nodes alive
        leader.replicate("PUT a 1");
        leader.replicate("PUT b 2");

        // node 2 goes offline and misses entries
        cluster.kill(2);
        leader.replicate("PUT c 3");
        leader.replicate("PUT d 4");

        // node 2 comes back
        cluster.revive(2);

        // leader heartbeats -> node 2 should backtrack + catch up
        leader.receiveHeartbeatTick();
        leader.receiveHeartbeatTick(); // a couple of rounds to converge

        assertEquals(leader.logSize(), cluster.node(2).logSize(),
                "lagging follower should catch up to the leader's log");
    }

    @Test
    void allFollowersConvergeAfterReplication() {
        RaftCluster cluster = new RaftCluster(3);
        cluster.node(0).startElection();
        RaftNode leader = cluster.leader();

        for (int i = 0; i < 5; i++) leader.replicate("PUT k" + i + " " + i);

        assertEquals(5, cluster.node(1).logSize());
        assertEquals(5, cluster.node(2).logSize());
        assertEquals("PUT k0 0", cluster.node(1).committedValueAt(0));
        assertEquals("PUT k4 4", cluster.node(2).committedValueAt(4));
    }
}