package com.spring2go.bigcache;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class BigCacheStats {
    private final long cacheHit;
    private final long cacheMiss;

    private final long cacheGet;
    private final long cachePut;
    private final long cacheDelete;

    private final long cacheExpire;
    private final long cacheMove;
    private final long cacheTotalEntries;

    private final long storageUsed;
    private final long storageDirty;
    private final long storageCapacity;
    private final long storageUsedBlocks;
    private final long storageFreeBlocks;
    private final long storageTotalBlocks;

    public BigCacheStats(long cacheHit, long cacheMiss, long cacheGet,
                         long cachePut, long cacheDelete, long cacheExpire,
                         long cacheMove, long cacheTotalEntries, long storageUsed,
                         long storageDirty, long storageCapacity,
                         long storageUsedBlocks, long storageFreeBlocks, long storageTotalBlocks) {
        this.cacheHit = cacheHit;
        this.cacheMiss = cacheMiss;

        this.cacheGet = cacheGet;
        this.cachePut = cachePut;
        this.cacheDelete = cacheDelete;

        this.cacheExpire = cacheExpire;
        this.cacheMove = cacheMove;

        this.cacheTotalEntries = cacheTotalEntries;

        this.storageUsed = storageUsed;
        this.storageDirty = storageDirty;
        this.storageCapacity = storageCapacity;

        this.storageUsedBlocks = storageUsedBlocks;
        this.storageFreeBlocks = storageFreeBlocks;
        this.storageTotalBlocks = storageTotalBlocks;
    }

    public BigCacheStats() {
        this(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public BigCacheStats getDeltaStats(BigCacheStats previousStats) {
        if (previousStats == null) {
            return this;
        }
        return new BigCacheStats(
                /*$(current value - previous value): the delta between two adjacent stats*/
                this.cacheHit - previousStats.cacheHit,
                this.cacheMiss - previousStats.cacheMiss,
                this.cacheGet - previousStats.cacheGet,
                this.cachePut - previousStats.cachePut,
                this.cacheDelete - previousStats.cacheDelete,
                this.cacheExpire - previousStats.cacheExpire,
                this.cacheMove - previousStats.cacheMove,

                /*$(current value): latest value which is more meaningful*/
                this.cacheTotalEntries,
                this.storageUsed,
                this.storageDirty,
                this.storageCapacity,
                this.storageUsedBlocks,
                this.storageFreeBlocks,
                this.storageTotalBlocks
        );
    }

    public long getCacheHit() {
        return cacheHit;
    }

    public long getCacheMiss() {
        return cacheMiss;
    }

    public long getCacheGet() {
        return cacheGet;
    }

    public long getCachePut() {
        return cachePut;
    }

    public long getCacheDelete() {
        return cacheDelete;
    }

    public long getCacheExpire() {
        return cacheExpire;
    }

    public long getCacheMove() {
        return cacheMove;
    }

    public long getCacheTotalEntries() {
        return cacheTotalEntries;
    }

    public long getStorageUsed() {
        return storageUsed;
    }

    public long getStorageDirty() {
        return storageDirty;
    }

    public long getStorageCapacity() {
        return storageCapacity;
    }

    public long getStorageUsedBlocks() {
        return storageUsedBlocks;
    }

    public long getStorageFreeBlocks() {
        return storageFreeBlocks;
    }

    public long getStorageTotalBlocks() {
        return storageTotalBlocks;
    }
}
