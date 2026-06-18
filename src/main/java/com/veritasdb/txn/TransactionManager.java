package com.veritasdb.txn;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages transactions over an MvccStore with snapshot isolation + OCC.
 *
 * A single monotonic clock issues timestamps. begin() takes a fresh snapshot
 * timestamp; commit() takes a fresh (larger) commit timestamp and applies
 * buffered writes at it. Reads at a snapshot see versions with commitTs <= snapshot.
 */
public final class TransactionManager {

    private final MvccStore store;
    private final AtomicLong clock = new AtomicLong(0);

    public TransactionManager(MvccStore store) {
        this.store = store;
    }

    public Transaction begin() {
        long snapshot = clock.incrementAndGet();
        return new Transaction(snapshot);
    }

    public Optional<String> get(Transaction txn, String key) {
        ensureActive(txn);
        txn.recordRead(key);
        if (txn.hasLocalWrite(key)) {
            String v = txn.localWrite(key);
            return v == null ? Optional.empty() : Optional.of(v);
        }
        return store.read(key, txn.snapshotTs());
    }

    public void put(Transaction txn, String key, String value) {
        ensureActive(txn);
        txn.bufferPut(key, value);
    }

    public void delete(Transaction txn, String key) {
        ensureActive(txn);
        txn.bufferDelete(key);
    }

    /**
     * Commit with OCC validation. Aborts if any key in the read set has a
     * committed version newer than this transaction's snapshot.
     */
    public synchronized boolean commit(Transaction txn) {
        ensureActive(txn);

        for (String key : txn.readSet()) {
            long latest = store.latestCommitTs(key);
            if (latest > txn.snapshotTs()) {
                txn.close();
                return false; // write-write / read-write conflict -> abort
            }
        }

        long commitTs = clock.incrementAndGet();
        for (var e : txn.writes().entrySet()) {
            if (e.getValue() == null) {
                store.commitVersion(e.getKey(), VersionedValue.deleted(commitTs));
            } else {
                store.commitVersion(e.getKey(), VersionedValue.of(commitTs, e.getValue()));
            }
        }
        txn.close();
        return true;
    }

    public void abort(Transaction txn) {
        txn.close();
    }

    private void ensureActive(Transaction txn) {
        if (!txn.isActive()) throw new IllegalStateException("Transaction is not active");
    }
}
