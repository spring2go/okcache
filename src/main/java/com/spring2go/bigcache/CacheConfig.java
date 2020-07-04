package com.spring2go.bigcache;

import com.spring2go.bigcache.storage.StorageManager;


/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheConfig {
    private int concurrencyLevel = BigCache.DEFAULT_CONCURRENCY_LEVEL;
    private int capacityPerBlock = StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
    private int initialNumberOfBlocks = StorageManager.DEFAULT_INITIAL_NUMBER_OF_BLOCKS;
    private long purgeInterval = BigCache.DEFAULT_PURGE_INTERVAL;
    private long mergeInterval = BigCache.DEFAULT_MERGE_INTERVAL;
    private double dirtyRatioThreshold = BigCache.DEFAULT_DIRTY_RATIO_THRESHOLD;
    private long maxOffHeapMemorySize = StorageManager.DEFAULT_MAX_OFFHEAP_MEMORY_SIZE;
    private StorageMode storageMode = StorageMode.PureFile;

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public CacheConfig setConcurrencyLevel(int concurrencyLevel) {
        if(concurrencyLevel > 11 || concurrencyLevel < 0){
            throw new IllegalArgumentException("concurrencyLevel must be between 0 and 11 inclusive!");
        }

        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    public int getCapacityPerBlock() {
        return capacityPerBlock;
    }

    public CacheConfig setCapacityPerBlock(int capacityPerBlock) {
        if(capacityPerBlock < 16 * 1024 * 1024){
            throw new IllegalArgumentException("capacityPerBlock must be bigger than 16MB!");
        }

        this.capacityPerBlock = capacityPerBlock;
        return this;
    }

    public int getInitialNumberOfBlocks() {
        return initialNumberOfBlocks;
    }

    public CacheConfig setInitialNumberOfBlocks(int initialNumberOfBlocks) {

        if(initialNumberOfBlocks <= 0){
            throw new IllegalArgumentException("initialNumberOfBlocks must be > 0!");
        }

        this.initialNumberOfBlocks = initialNumberOfBlocks;
        return this;
    }

    public long getPurgeInterval() {
        return purgeInterval;
    }

    public CacheConfig setPurgeInterval(long purgeInterval) {
        this.purgeInterval = purgeInterval;
        return this;
    }

    public long getMergeInterval() {
        return mergeInterval;
    }

    public CacheConfig setMergeInterval(long mergeInterval) {
        this.mergeInterval = mergeInterval;
        return this;
    }

    public double getDirtyRatioThreshold() {
        return dirtyRatioThreshold;
    }

    public CacheConfig setDirtyRatioLimit(double dirtyRatioThreshold) {
        this.dirtyRatioThreshold = dirtyRatioThreshold;
        return this;
    }

    public StorageMode getStorageMode() {
        return storageMode;
    }

    public CacheConfig setStorageMode(StorageMode storageMode) {
        this.storageMode = storageMode;
        return this;
    }

    /**
     * Limiting Offheap memory usage.
     *
     * Only takes effect when the {@link StorageMode} is set to MemoryMappedPlusFile or OffHeapPlusFile mode,
     * in these cases, this setting limits the max offheap memory size.
     *
     * @param maxOffHeapMemorySize max offheap memory size allowed, unit : byte.
     * @return CacheConfig
     */
    public CacheConfig setMaxOffHeapMemorySize(long maxOffHeapMemorySize) {
        if (maxOffHeapMemorySize < this.capacityPerBlock) {
            throw new IllegalArgumentException("maxOffHeapMemorySize must be equal to or larger than capacityPerBlock" + this.getCapacityPerBlock());
        }
        this.maxOffHeapMemorySize = maxOffHeapMemorySize;
        return this;
    }

    public long getMaxOffHeapMemorySize() {
        return this.maxOffHeapMemorySize;
    }

    public enum StorageMode {
        PureFile,
        MemoryMappedPlusFile,
        OffHeapPlusFile,
    }
}
