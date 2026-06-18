package com.veritasdb;

import com.veritasdb.storage.LsmEngine;
import com.veritasdb.txn.MvccStore;
import com.veritasdb.txn.Transaction;
import com.veritasdb.txn.TransactionManager;
import com.veritasdb.raft.RaftCluster;
import com.veritasdb.raft.RaftNode;
import com.veritasdb.raft.GrpcTransport;
import com.veritasdb.raft.RaftGrpcServer;
import java.util.HashMap;
import java.util.Map;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equalsIgnoreCase("txn")) {
            runTxnDemo();
            return;
        }

         if (args.length >= 2 && args[0].equalsIgnoreCase("node")) {
            runNode(args);
            return;
        }

        if (args.length >= 1 && args[0].equalsIgnoreCase("raft")) {
            runRaftDemo();
            return;
        }

        Path dataDir = Path.of("data");
        try (LsmEngine engine = new LsmEngine(dataDir)) {
            System.out.println("Recovered: memtable=" + engine.memtableSize()
                    + " keys, SSTables=" + engine.ssTableCount());

            if (args.length >= 3 && args[0].equalsIgnoreCase("put")) {
                engine.put(args[1], args[2]);
                System.out.println("PUT " + args[1] + " = " + args[2]);
            } else if (args.length >= 2 && args[0].equalsIgnoreCase("get")) {
                System.out.println("GET " + args[1] + " -> " +
                        engine.get(args[1]).orElse("<not found>"));
            } else if (args.length >= 2 && args[0].equalsIgnoreCase("del")) {
                engine.delete(args[1]);
                System.out.println("DELETE " + args[1]);
            } else if (args.length >= 1 && args[0].equalsIgnoreCase("compact")) {
                engine.compact();
                System.out.println("Compacted into " + engine.ssTableCount() + " SSTable(s)");
            } else {
                System.out.println("Usage: put <k> <v> | get <k> | del <k> | compact | txn");
            }
        }
    }

    private static void runTxnDemo() {
        TransactionManager tm = new TransactionManager(new MvccStore());

        System.out.println("=== MVCC Transaction Demo ===\n");

        Transaction setup = tm.begin();
        tm.put(setup, "balance", "100");
        tm.commit(setup);
        System.out.println("Committed: balance = 100");

        Transaction reader = tm.begin();
        System.out.println("Reader begins. Sees balance = " + tm.get(reader, "balance").orElseThrow());

        Transaction writer = tm.begin();
        tm.put(writer, "balance", "200");
        tm.commit(writer);
        System.out.println("Writer committed balance = 200 (after reader's snapshot)");

        System.out.println("Reader STILL sees balance = " + tm.get(reader, "balance").orElseThrow()
                + "  (snapshot isolation)");
        tm.commit(reader);

        System.out.println("\n--- Write-write conflict ---");
        Transaction t1 = tm.begin();
        Transaction t2 = tm.begin();
        tm.get(t1, "balance");
        tm.get(t2, "balance");
        tm.put(t1, "balance", "300");
        tm.put(t2, "balance", "400");
        System.out.println("t1 commit: " + (tm.commit(t1) ? "SUCCESS" : "ABORT"));
        System.out.println("t2 commit: " + (tm.commit(t2) ? "SUCCESS" : "ABORT (conflict)"));
    }

    private static void runRaftDemo() {
        com.veritasdb.raft.RaftCluster cluster = new com.veritasdb.raft.RaftCluster(3);
        System.out.println("=== Raft Demo (3 nodes) ===\n");

        System.out.println("Node 0 starts an election...");
        cluster.node(0).startElection();
        System.out.println("Leader is now node " + cluster.leader().id()
                + " (term " + cluster.leader().currentTerm() + ")\n");

        System.out.println("Leader replicates 'PUT x 1'...");
        boolean ok = cluster.leader().replicate("PUT x 1");
        System.out.println("Committed to majority: " + ok);
        System.out.println("Followers' log sizes: node1=" + cluster.node(1).logSize()
                + ", node2=" + cluster.node(2).logSize() + "\n");

        System.out.println("Leader (node 0) fails...");
        cluster.kill(0);
        System.out.println("Node 1 starts an election...");
        cluster.node(1).startElection();
        System.out.println("New leader is node " + cluster.leader().id()
                + " (term " + cluster.leader().currentTerm() + ")");
    }

    private static void runNode(String[] args) throws Exception {
        // Usage: node <id> <port> <peerId>:<host>:<port> [<peerId>:<host>:<port> ...]
        int id = Integer.parseInt(args[1]);
        int port = Integer.parseInt(args[2]);

        Map<Integer, String> addresses = new HashMap<>();
        java.util.List<Integer> peerIds = new java.util.ArrayList<>();
        for (int i = 3; i < args.length; i++) {
            String[] parts = args[i].split(":");
            int peerId = Integer.parseInt(parts[0]);
            addresses.put(peerId, parts[1] + ":" + parts[2]);
            peerIds.add(peerId);
        }
        int[] peers = peerIds.stream().mapToInt(Integer::intValue).toArray();

        GrpcTransport transport = new GrpcTransport(addresses);
        RaftNode node = new RaftNode(id, peers, transport);
        RaftGrpcServer server = new RaftGrpcServer(port, node);
        server.start();

        // Simple control: type 'elect' to start an election, 'put k v' to replicate, 'status', 'quit'
        java.io.BufferedReader in = new java.io.BufferedReader(
                new java.io.InputStreamReader(System.in));
        System.out.println("Commands: elect | put <k> <v> | status | quit");
        String line;
        while ((line = in.readLine()) != null) {
            String[] c = line.trim().split(" ");
            if (c.length == 0) continue;
            switch (c[0].toLowerCase()) {
                case "elect" -> { node.startElection();
                    System.out.println("state=" + node.state() + " term=" + node.currentTerm()); }
                case "put" -> { boolean ok = node.replicate("PUT " + c[1] + " " + c[2]);
                    System.out.println("replicated=" + ok); }
                case "status" -> System.out.println("id=" + node.id() + " state=" + node.state()
                        + " term=" + node.currentTerm() + " leader=" + node.currentLeader()
                        + " log=" + node.logSize());
                case "quit" -> { server.stop(); return; }
                default -> System.out.println("Unknown: " + line);
            }
        }
    }
}
