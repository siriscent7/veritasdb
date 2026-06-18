package com.veritasdb.raft;

/** The three roles a Raft node can be in. */
public enum RaftState {
    FOLLOWER, CANDIDATE, LEADER
}
