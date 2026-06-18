package com.veritasdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class BloomAndCompactionTest {

    @Test
    void bloomFilterNoFalseNegatives(@TempDir Path dir) throws Exception {
        BloomFilter bf = new BloomFilter(100);
        bf.add("alice");
        bf.add("bob");
        assertTrue(bf.mightContain("alice"));
        assertTrue(bf.mightContain("bob"));
        assertFalse(bf.mightContain("definitely-not-present-xyz"));
    }

    @Test
    void compactionMergesSSTables(@TempDir Path dir) throws Exception {
        try (LsmEngine e = new LsmEngine(dir)) {
            e.put("a", "1"); e.put("b", "2"); e.put("c", "3"); // flush 1
            e.put("a", "10"); e.put("d", "4"); e.put("e", "5"); // flush 2
            assertTrue(e.ssTableCount() >= 2);

            e.compact();
            assertEquals(1, e.ssTableCount());

            // newest value wins after compaction
            assertEquals("10", e.get("a").orElseThrow());
            assertEquals("2", e.get("b").orElseThrow());
            assertEquals("4", e.get("d").orElseThrow());
        }
    }
}
