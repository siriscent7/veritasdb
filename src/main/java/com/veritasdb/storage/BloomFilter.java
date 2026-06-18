package com.veritasdb.storage;
import java.util.BitSet;

public final class BloomFilter {

    private final BitSet bits;
    private final int size;
    private final int hashCount;

    public BloomFilter(int expectedItems) {
        // ~10 bits per item gives a low false-positive rate
        this.size = Math.max(64, expectedItems * 10);
        this.hashCount = 4;
        this.bits = new BitSet(size);
    }

    public void add(String key) {
        for (int i = 0; i < hashCount; i++) {
            bits.set(hash(key, i));
        }
    }

    public boolean mightContain(String key) {
        for (int i = 0; i < hashCount; i++) {
            if (!bits.get(hash(key, i))) return false; // definitely absent
        }
        return true; // probably present
    }

    private int hash(String key, int seed) {
        int h = 17 + seed * 31;
        for (int i = 0; i < key.length(); i++) {
            h = h * 31 + key.charAt(i);
        }
        return Math.floorMod(h, size);
    }
}