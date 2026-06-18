package com.veritasdb.storage;

public record ValueEntry(String value, boolean tombstone) {

    public static ValueEntry of(String value) {
        return new ValueEntry(value, false);
    }

    public static ValueEntry deleted() {
        return new ValueEntry(null, true);
    }

    @Override public String toString() {
        return tombstone ? "<deleted>" : value;
    }
}