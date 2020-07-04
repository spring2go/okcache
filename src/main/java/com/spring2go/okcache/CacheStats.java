package com.spring2go.okcache;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheStats {
    private final long hitCount;
    private final long missCount;

    private final long memoryHitCount;
    private final long memoryMissCount;

    private final long createCount;
    private final long updateCount;
    private final long removeCount;

    private final long evictCount;
    private final long expireCount;

    private final long evictStoreHitCount;
    private final long evictStoreMissCount;

    private final long behindStoreHitCount;
    private final long behindStoreMissCount;

    private final long size;
    private final long memorySize;

    public CacheStats(long hitCount, long missCount, long memoryHitCount, long memoryMissCount, long createCount, long updateCount, long removeCount, long evictCount, long expireCount, long evictStoreHitCount, long evictStoreMissCount, long behindStoreHitCount, long behindStoreMissCount, long size, long memorySize) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.memoryHitCount = memoryHitCount;
        this.memoryMissCount = memoryMissCount;
        this.createCount = createCount;
        this.updateCount = updateCount;
        this.removeCount = removeCount;
        this.evictCount = evictCount;
        this.expireCount = expireCount;
        this.evictStoreHitCount = evictStoreHitCount;
        this.evictStoreMissCount = evictStoreMissCount;
        this.behindStoreHitCount = behindStoreHitCount;
        this.behindStoreMissCount = behindStoreMissCount;
        this.size = size;
        this.memorySize = memorySize;
    }

    public CacheStats() {
        this(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
    }

    public long requestCount() {
        return hitCount + missCount;
    }

    public double hitRate() {
        double r = requestCount();
        return r > 0 ? hitCount / r : 1.0;
    }

    public double memoryHitRate() {
        double r = memoryHitCount + memoryMissCount;
        return r > 0 ? memoryHitCount / r : 1.0;
    }

    public double evictStoreHitRate() {
        double r = evictStoreHitCount + evictStoreMissCount;
        return r > 0 ? evictStoreHitCount / r : 1.0;
    }

    public double behindStoreHitRate() {
        double r = behindStoreHitCount + behindStoreMissCount;
        return r > 0 ? behindStoreHitCount / r : 1.0;
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public long getMemoryHitCount() {
        return memoryHitCount;
    }

    public long getMemoryMissCount() {
        return memoryMissCount;
    }

    public long getCreateCount() {
        return createCount;
    }

    public long getUpdateCount() {
        return updateCount;
    }

    public long getRemoveCount() {
        return removeCount;
    }

    public long getEvictCount() {
        return evictCount;
    }

    public long getExpireCount() {
        return expireCount;
    }

    public long getEvictStoreHitCount() {
        return evictStoreHitCount;
    }

    public long getEvictStoreMissCount() {
        return evictStoreMissCount;
    }

    public long getBehindStoreHitCount() {
        return behindStoreHitCount;
    }

    public long getBehindStoreMissCount() {
        return behindStoreMissCount;
    }

    public long getSize() {
        return size;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public CacheStats minus(CacheStats other) {
        return new CacheStats(
                Math.max(0, hitCount - other.hitCount),
                Math.max(0, missCount - other.missCount),
                Math.max(0, memoryHitCount - other.memoryHitCount),
                Math.max(0, memoryMissCount - other.memoryMissCount),
                Math.max(0, createCount - other.createCount),
                Math.max(0, updateCount - other.updateCount),
                Math.max(0, removeCount - other.removeCount),
                Math.max(0, evictCount - other.evictCount),
                Math.max(0, expireCount - other.expireCount),
                Math.max(0, evictStoreHitCount - other.evictStoreHitCount),
                Math.max(0, evictStoreMissCount - other.evictStoreMissCount),
                Math.max(0, behindStoreHitCount - other.behindStoreHitCount),
                Math.max(0, behindStoreMissCount - other.behindStoreMissCount),
                Math.max(0, size - other.size),
                Math.max(0, memorySize - other.memorySize)
        );
    }

    public CacheStats plus(CacheStats other) {
        return new CacheStats(
                Math.max(0, hitCount + other.hitCount),
                Math.max(0, missCount + other.missCount),
                Math.max(0, memoryHitCount + other.memoryHitCount),
                Math.max(0, memoryMissCount + other.memoryMissCount),
                Math.max(0, createCount + other.createCount),
                Math.max(0, updateCount + other.updateCount),
                Math.max(0, removeCount + other.removeCount),
                Math.max(0, evictCount + other.evictCount),
                Math.max(0, expireCount + other.expireCount),
                Math.max(0, evictStoreHitCount + other.evictStoreHitCount),
                Math.max(0, evictStoreMissCount + other.evictStoreMissCount),
                Math.max(0, behindStoreHitCount + other.behindStoreHitCount),
                Math.max(0, behindStoreMissCount + other.behindStoreMissCount),
                Math.max(0, size + other.size),
                Math.max(0, memorySize + other.memorySize)
        );
    }

    @Override
    public String toString() {
        return new StringBuilder(this.getClass().getSimpleName()).append(":\n")
                .append("\t").append("hitCount").append(":").append(hitCount).append("\n")
                .append("\t").append("missCount").append(":").append(missCount).append("\n")
                .append("\t").append("memoryHitCount").append(":").append(memoryHitCount).append("\n")
                .append("\t").append("memoryMissCount").append(":").append(memoryMissCount).append("\n")
                .append("\t").append("createCount").append(":").append(createCount).append("\n")
                .append("\t").append("updateCount").append(":").append(updateCount).append("\n")
                .append("\t").append("removeCount").append(":").append(removeCount).append("\n")
                .append("\t").append("evictCount").append(":").append(evictCount).append("\n")
                .append("\t").append("expireCount").append(":").append(expireCount).append("\n")
                .append("\t").append("evictStoreHitCount").append(":").append(evictStoreHitCount).append("\n")
                .append("\t").append("evictStoreMissCount").append(":").append(evictStoreMissCount).append("\n")
                .append("\t").append("behindStoreHitCount").append(":").append(behindStoreHitCount).append("\n")
                .append("\t").append("behindStoreMissCount").append(":").append(behindStoreMissCount).append("\n")
                .append("\t").append("size").append(":").append(size).append("\n")
                .append("\t").append("memorySize").append(":").append(memorySize).append("\n")
                .toString();
    }
}
