package com.veritasdb.txn;

/**
 * One version of a value, tagged with the commit timestamp at which it became visible.
 * A tombstone version represents a delete.
 */
public record VersionedValue(long commitTs, String value, boolean tombstone) {

    public static VersionedValue of(long ts, String value) {
        return new VersionedValue(ts, value, false);
    }

    public static VersionedValue deleted(long ts) {
        return new VersionedValue(ts, null, true);
    }
}
