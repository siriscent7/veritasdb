package com.veritasdb;

import com.veritasdb.storage.LsmEngine;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of("data");

        try (LsmEngine engine = new LsmEngine(dataDir)) {
            System.out.println("Recovered " + engine.size() + " keys from WAL.");

            if (args.length >= 3 && args[0].equalsIgnoreCase("put")) {
                engine.put(args[1], args[2]);
                System.out.println("PUT " + args[1] + " = " + args[2]);
            } else if (args.length >= 2 && args[0].equalsIgnoreCase("get")) {
                System.out.println("GET " + args[1] + " -> " +
                        engine.get(args[1]).orElse("<not found>"));
            } else if (args.length >= 2 && args[0].equalsIgnoreCase("del")) {
                engine.delete(args[1]);
                System.out.println("DELETE " + args[1]);
            } else {
                System.out.println("Usage: put <key> <value> | get <key> | del <key>");
            }
        }
    }
}
