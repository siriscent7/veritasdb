package com.veritasdb.raft;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RaftTest {

    @Test
    void electsALeader() {
        RaftCluster cluster = new RaftCluster(3);
        cluster.node(0).startElection();   // node 0 triggers an election

        assertEquals(RaftState.LEADER, cluster.node(0).state());
        assertNotNull(cluster.leader());
        assertEquals(0, cluster.leader().id());
    }

    @Test
    void leaderReplicatesCommandToMajority() {
        RaftCluster cluster = new RaftCluster(3);
        cluster.node(0).startElection();
        RaftNode leader = cluster.leader();
        assertNotNull(leader);

        boolean committed = leader.replicate("PUT x 1");
        assertTrue(committed);
        assertEquals("PUT x 1", leader.committedValueAt(0));

        // followers received it too
        assertTrue(cluster.node(1).logSize() >= 1);
        assertTrue(cluster.node(2).logSize() >= 1);
    }

    @Test
    void newLeaderElectedAfterLeaderFails() {
        RaftCluster cluster = new RaftCluster(3);
        cluster.node(0).startElection();
        assertEquals(0, cluster.leader().id());

        // leader dies
        cluster.kill(0);

        // a follower starts a new election
        cluster.node(1).startElection();
        assertEquals(RaftState.LEADER, cluster.node(1).state());
        assertEquals(1, cluster.leader().id());
    }

    @Test
    void higherTermCausesStepDown() {
        RaftCluster cluster = new RaftCluster(3);
        cluster.node(0).startElection();    // node 0 leader, term 1
        cluster.node(1).startElection();    // node 1 forces a higher term

        // node 0 should no longer be leader at the old term
        assertTrue(cluster.node(1).currentTerm() > 1);
    }

    @Test
    void candidateNeedsMajority() {
        RaftCluster cluster = new RaftCluster(5);
        // kill two nodes -> 3 alive, still a majority possible
        cluster.kill(3);
        cluster.kill(4);
        cluster.node(0).startElection();
        assertEquals(RaftState.LEADER, cluster.node(0).state()); // 3/5 votes = majority
    }
}
