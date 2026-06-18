package com.veritasdb.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class RaftPersistenceTest {

    /** A no-op transport: this test only checks local persistence, not RPCs. */
    private RaftNode.Transport noopTransport() {
        return new RaftNode.Transport() {
            public Messages.VoteResponse sendRequestVote(int to, Messages.RequestVote rpc) { return null; }
            public Messages.AppendResponse sendAppendEntries(int to, Messages.AppendEntries rpc) { return null; }
        };
    }

    @Test
    void persistsTermAndVoteAcrossRestart(@TempDir Path dir) throws Exception {
        RaftPersistence p1 = new RaftPersistence(dir, 0);
        RaftNode node1 = new RaftNode(0, new int[]{1, 2}, noopTransport(), p1);

        // startElection bumps term to 1 and votes for self; peers unreachable so stays candidate
        node1.startElection();
        long term = node1.currentTerm();
        assertEquals(1, term);

        // "restart": new node, same dir + id, reloads persisted state
        RaftPersistence p2 = new RaftPersistence(dir, 0);
        RaftNode node2 = new RaftNode(0, new int[]{1, 2}, noopTransport(), p2);
        assertEquals(term, node2.currentTerm()); // term survived restart
    }

    @Test
    void persistsLogAcrossRestart(@TempDir Path dir) throws Exception {
        RaftPersistence p1 = new RaftPersistence(dir, 0);
        // single-node "cluster" (no peers) so replicate() commits immediately
        RaftNode leader = new RaftNode(0, new int[]{}, noopTransport(), p1);
        leader.startElection();           // becomes leader (majority of 1)
        assertEquals(RaftState.LEADER, leader.state());

        leader.replicate("PUT a 1");
        leader.replicate("PUT b 2");
        assertEquals(2, leader.logSize());

        // restart
        RaftPersistence p2 = new RaftPersistence(dir, 0);
        RaftNode restarted = new RaftNode(0, new int[]{}, noopTransport(), p2);
        assertEquals(2, restarted.logSize());                 // log survived
        assertEquals("PUT a 1", restarted.committedValueAt(0));
        assertEquals("PUT b 2", restarted.committedValueAt(1));
    }
}