package com.veritasdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class LsmEngineTest {

    @Test
    void putGetDelete(@TempDir Path dir) throws Exception {
        try (LsmEngine e = new LsmEngine(dir)) {
            e.put("a", "1");
            e.put("b", "2");
            assertEquals("1", e.get("a").orElseThrow());
            assertEquals("2", e.get("b").orElseThrow());

            e.delete("a");
            assertTrue(e.get("a").isEmpty());
        }
    }

    @Test
    void recoversFromWalAfterRestart(@TempDir Path dir) throws Exception {
        try (LsmEngine e = new LsmEngine(dir)) {
            e.put("x", "100");
            e.put("y", "200");
            e.delete("y");
        }
        // simulate restart: new engine, same dir
        try (LsmEngine e2 = new LsmEngine(dir)) {
            assertEquals("100", e2.get("x").orElseThrow());
            assertTrue(e2.get("y").isEmpty()); // tombstone recovered
        }
    }
}