package com.veritasdb.txn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransactionTest {

    private TransactionManager tm;

    @BeforeEach
    void setup() {
        tm = new TransactionManager(new MvccStore());
    }

    @Test
    void basicCommitAndRead() {
        Transaction t1 = tm.begin();
        tm.put(t1, "a", "1");
        assertTrue(tm.commit(t1));

        Transaction t2 = tm.begin();
        assertEquals("1", tm.get(t2, "a").orElseThrow());
        assertTrue(tm.commit(t2));
    }

    @Test
    void readsOwnWritesBeforeCommit() {
        Transaction t = tm.begin();
        tm.put(t, "x", "buffered");
        // sees its own uncommitted write
        assertEquals("buffered", tm.get(t, "x").orElseThrow());
        assertTrue(tm.commit(t));
    }

    @Test
    void snapshotIsolation_readerDoesNotSeeLaterCommit() {
        // commit a=1
        Transaction setup = tm.begin();
        tm.put(setup, "a", "1");
        assertTrue(tm.commit(setup));

        // reader begins (snapshot now)
        Transaction reader = tm.begin();
        assertEquals("1", tm.get(reader, "a").orElseThrow());

        // another txn commits a=2 AFTER reader's snapshot
        Transaction writer = tm.begin();
        tm.put(writer, "a", "2");
        assertTrue(tm.commit(writer));

        // reader still sees its snapshot value (1), not 2
        assertEquals("1", tm.get(reader, "a").orElseThrow());
    }

    @Test
    void writeWriteConflictAborts() {
        // commit a=1
        Transaction setup = tm.begin();
        tm.put(setup, "a", "1");
        assertTrue(tm.commit(setup));

        // two concurrent transactions both read a, then both try to write a
        Transaction t1 = tm.begin();
        Transaction t2 = tm.begin();

        tm.get(t1, "a"); // t1 reads a
        tm.get(t2, "a"); // t2 reads a

        tm.put(t1, "a", "t1-value");
        tm.put(t2, "a", "t2-value");

        assertTrue(tm.commit(t1));   // first commit wins
        assertFalse(tm.commit(t2));  // second sees a newer version of a -> abort
    }

    @Test
    void deleteIsVisibleToLaterTransactions() {
        Transaction t1 = tm.begin();
        tm.put(t1, "k", "v");
        assertTrue(tm.commit(t1));

        Transaction t2 = tm.begin();
        tm.delete(t2, "k");
        assertTrue(tm.commit(t2));

        Transaction t3 = tm.begin();
        assertTrue(tm.get(t3, "k").isEmpty()); // deleted
    }
}
