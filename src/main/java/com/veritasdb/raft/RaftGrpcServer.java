package com.veritasdb.raft;

import com.veritasdb.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Exposes a RaftNode over gRPC. Translates proto messages <-> internal Messages. */
public final class RaftGrpcServer {

    private final int port;
    private final RaftNode node;
    private Server server;

    public RaftGrpcServer(int port, RaftNode node) {
        this.port = port;
        this.node = node;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new ServiceImpl(node))
                .build()
                .start();
        System.out.println("Raft gRPC server (node " + node.id() + ") listening on :" + port);
    }

    public void awaitTermination() throws InterruptedException {
        if (server != null) server.awaitTermination();
    }

    public void stop() {
        if (server != null) server.shutdownNow();
    }

    private static final class ServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
        private final RaftNode node;
        ServiceImpl(RaftNode node) { this.node = node; }

        @Override
        public void requestVote(VoteRequest req, StreamObserver<VoteReply> obs) {
            Messages.VoteResponse r = node.handleRequestVote(new Messages.RequestVote(
                    req.getTerm(), req.getCandidateId(),
                    req.getLastLogIndex(), req.getLastLogTerm()));
            obs.onNext(VoteReply.newBuilder()
                    .setTerm(r.term()).setVoteGranted(r.voteGranted()).build());
            obs.onCompleted();
        }

        @Override
        public void appendEntries(AppendRequest req, StreamObserver<AppendReply> obs) {
            List<LogEntry> entries = new ArrayList<>();
            for (LogEntryProto e : req.getEntriesList()) {
                entries.add(new LogEntry(e.getTerm(), e.getIndex(), e.getCommand()));
            }
            Messages.AppendResponse r = node.handleAppendEntries(new Messages.AppendEntries(
                    req.getTerm(), req.getLeaderId(), req.getPrevLogIndex(),
                    req.getPrevLogTerm(), entries, req.getLeaderCommit()));
            obs.onNext(AppendReply.newBuilder()
                    .setTerm(r.term()).setSuccess(r.success())
                    .setMatchIndex(r.matchIndex()).build());
            obs.onCompleted();
        }
    }
}
