package com.spring2go.okcache;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheStatsCounter {
    final AtomicLong memoryHitCount = new AtomicLong(0);
    final AtomicLong memoryMissCount = new AtomicLong(0);

    final AtomicLong createCount = new AtomicLong(0);
    final AtomicLong updateCount = new AtomicLong(0);
    final AtomicLong removeCount = new AtomicLong(0);

    final AtomicLong evictCount = new AtomicLong(0);
    final AtomicLong expireCount = new AtomicLong(0);

    final AtomicLong evictStoreHitCount = new AtomicLong(0);
    final AtomicLong evictStoreMissCount = new AtomicLong(0);

    final AtomicLong behindStoreHitCount = new AtomicLong(0);
    final AtomicLong behindStoreMissCount = new AtomicLong(0);

    final AtomicLong size = new AtomicLong(0);
    final AtomicLong memorySize = new AtomicLong(0);

    public void memoryHits(int count) {
        memoryHitCount.addAndGet(count);
    }

    public void memoryMisses(int count) {
        memoryMissCount.addAndGet(count);
    }

    public void recordCreates(int count) {
        createCount.addAndGet(count);
    }

    public void recordUpdates(int count) {
        updateCount.addAndGet(count);
    }

    public void recordRemoves(int count) {
        removeCount.addAndGet(count);
    }

    public void recordEvicts(int count) {
        evictCount.addAndGet(count);
    }

    public void recordExpires(int count) {
        expireCount.addAndGet(count);
    }

    public void evictStoreHits(int count) {
        evictStoreHitCount.addAndGet(count);
    }

    public void evictStoreMisses(int count) {
        evictStoreMissCount.addAndGet(count);
    }

    public void behindStoreHits(int count) {
        behindStoreHitCount.addAndGet(count);
    }

    public void behindStoreMisses(int count) {
        behindStoreMissCount.addAndGet(count);
    }

    public void sizeIncrement() {
        size.incrementAndGet();
    }

    public void sizeDecrement() {
        size.decrementAndGet();
    }

    public void memorySizeIncrement() {
        memorySize.incrementAndGet();
    }

    public void memorySizeDecrement() {
        memorySize.decrementAndGet();
    }

    public CacheStats snapshot() {
        return new CacheStats(
                memoryHitCount.get() + evictStoreHitCount.get() + behindStoreHitCount.get(),
                memoryMissCount.get() - evictStoreHitCount.get() - behindStoreHitCount.get(),
                memoryHitCount.get(),
                memoryMissCount.get(),
                createCount.get(),
                updateCount.get(),
                removeCount.get(),
                evictCount.get(),
                expireCount.get(),
                evictStoreHitCount.get(),
                evictStoreMissCount.get(),
                behindStoreHitCount.get(),
                behindStoreMissCount.get(),
                size.get(),
                memorySize.get()
        );
    }
}
