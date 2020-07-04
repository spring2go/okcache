package com.spring2go.okcache.impl;

import com.spring2go.okcache.CacheStats;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class SegmentStatsCounter {
    volatile long hitCount;
    volatile long missCount;

    volatile long memoryHitCount = 0l;
    volatile long memoryMissCount = 0l;

    volatile long createCount = 0l;
    volatile long updateCount = 0l;
    volatile long removeCount = 0l;

    volatile long evictCount = 0l;
    volatile long expireCount = 0l;

    volatile long evictStoreHitCount = 0l;
    volatile long evictStoreMissCount = 0l;

    volatile long behindStoreHitCount = 0l;
    volatile long behindStoreMissCount = 0l;

    volatile long size = 0l;
    volatile long memorySize = 0l;

    volatile long evictStoreExceptionCount = 0l;
    volatile long behindStoreExceptionCount = 0l;

    public void hits(int count) {
        hitCount += count;
    }

    public void misses(int count) {
        missCount += count;
    }

    public void memoryHits(int count) {
        memoryHitCount += count;
    }

    public void memoryMisses(int count) {
        memoryMissCount += count;
    }

    public void recordCreates(int count) {
        createCount += count;
    }

    public void recordUpdates(int count) {
        updateCount += count;
    }

    public void recordRemoves(int count) {
        removeCount+=count;
    }

    public void recordEvicts(int count) {
        evictCount += count;
    }

    public void recordExpires(int count) {
        expireCount+=count;
    }

    public void evictStoreHits(int count) {
        evictStoreHitCount+=count;
    }

    public void evictStoreMisses(int count) {
        evictStoreMissCount+=count;
    }

    public void behindStoreHits(int count) {
        behindStoreHitCount+=count;
    }

    public void behindStoreMisses(int count) {
        behindStoreMissCount+=count;
    }

    public void sizeIncrement() {
        size++;
    }

    public void sizeDecrement() {
        size--;
    }

    public void memorySizeIncrement() {
        memorySize++;
    }

    public void memorySizeDecrement() {
        memorySize--;
    }

    public void evictStoreException(int count){
        evictStoreExceptionCount += count;
    }

    public void behindStoreException(int count){
        behindStoreExceptionCount += count;
    }

    public CacheStats snapshot() {
        return new CacheStats(
                hitCount,
                missCount,
                memoryHitCount,
                memoryMissCount,
                createCount,
                updateCount,
                removeCount,
                evictCount,
                expireCount,
                evictStoreHitCount,
                evictStoreMissCount,
                behindStoreHitCount,
                behindStoreMissCount,
                size,
                memorySize
        );
    }
}
