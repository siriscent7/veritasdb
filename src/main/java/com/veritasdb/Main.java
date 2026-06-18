package com.veritasdb;

import com.veritasdb.storage.LsmEngine;
import com.veritasdb.txn.MvccStore;
import com.veritasdb.txn.Transaction;
import com.veritasdb.txn.TransactionManager;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equalsIgnoreCase("txn")) {
            runTxnDemo();
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
}
