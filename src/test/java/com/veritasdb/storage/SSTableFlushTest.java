package com.veritasdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class SSTableFlushTest {

    @Test
    void flushesToSSTableAndReadsBack(@TempDir Path dir) throws Exception {
        try (LsmEngine e = new LsmEngine(dir)) {
            e.put("a", "1");
            e.put("b", "2");
            e.put("c", "3");
            assertEquals(1, e.ssTableCount());
            assertEquals(0, e.memtableSize());
            assertEquals("1", e.get("a").orElseThrow());
            assertEquals("3", e.get("c").orElseThrow());
        }
    }

    @Test
    void newerValueOverridesOlder(@TempDir Path dir) throws Exception {
        try (LsmEngine e = new LsmEngine(dir)) {
            e.put("k", "old");
            e.put("x", "1");
            e.put("y", "2");
            e.put("k", "new");
            e.put("z", "3");
            e.put("w", "4");
            assertEquals("new", e.get("k").orElseThrow());
        }
    }

    @Test
    void recoversSSTablesAfterRestart(@TempDir Path dir) throws Exception {
        try (LsmEngine e = new LsmEngine(dir)) {
            e.put("p", "10");
            e.put("q", "20");
            e.put("r", "30");
        }
        try (LsmEngine e2 = new LsmEngine(dir)) {
            assertEquals(1, e2.ssTableCount());
            assertEquals("10", e2.get("p").orElseThrow());
        }
    }
}