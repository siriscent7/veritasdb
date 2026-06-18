package com.veritasdb.raft;

import com.veritasdb.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Network transport for Raft using gRPC. Maps peer node id -> host:port,
 * lazily opens channels, and translates internal Messages <-> proto.
 * Returns null on RPC failure (treated as "no response" by RaftNode).
 */
public final class GrpcTransport implements RaftNode.Transport {

    private final Map<Integer, String> addresses;   // nodeId -> "host:port"
    private final Map<Integer, RaftServiceGrpc.RaftServiceBlockingStub> stubs = new HashMap<>();

    public GrpcTransport(Map<Integer, String> addresses) {
        this.addresses = addresses;
    }

    private RaftServiceGrpc.RaftServiceBlockingStub stub(int nodeId) {
        return stubs.computeIfAbsent(nodeId, id -> {
            String[] hp = addresses.get(id).split(":");
            ManagedChannel ch = ManagedChannelBuilder
                    .forAddress(hp[0], Integer.parseInt(hp[1]))
                    .usePlaintext()
                    .build();
            return RaftServiceGrpc.newBlockingStub(ch);
        });
    }

    @Override
    public Messages.VoteResponse sendRequestVote(int toNode, Messages.RequestVote rpc) {
        try {
            VoteReply r = stub(toNode).requestVote(VoteRequest.newBuilder()
                    .setTerm(rpc.term()).setCandidateId(rpc.candidateId())
                    .setLastLogIndex(rpc.lastLogIndex()).setLastLogTerm(rpc.lastLogTerm())
                    .build());
            return new Messages.VoteResponse(r.getTerm(), r.getVoteGranted());
        } catch (Exception e) {
            return null; // peer unreachable
        }
    }

    @Override
    public Messages.AppendResponse sendAppendEntries(int toNode, Messages.AppendEntries rpc) {
        try {
            AppendRequest.Builder b = AppendRequest.newBuilder()
                    .setTerm(rpc.term()).setLeaderId(rpc.leaderId())
                    .setPrevLogIndex(rpc.prevLogIndex()).setPrevLogTerm(rpc.prevLogTerm())
                    .setLeaderCommit(rpc.leaderCommit());
            for (LogEntry e : rpc.entries()) {
                b.addEntries(LogEntryProto.newBuilder()
                        .setTerm(e.term()).setIndex(e.index()).setCommand(e.command()).build());
            }
            AppendReply r = stub(toNode).appendEntries(b.build());
            return new Messages.AppendResponse(r.getTerm(), r.getSuccess(), r.getMatchIndex());
        } catch (Exception e) {
            return null;
        }
    }
}
