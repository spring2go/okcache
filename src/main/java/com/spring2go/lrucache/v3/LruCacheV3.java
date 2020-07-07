package com.spring2go.lrucache.v3;

import com.spring2go.lrucache.v2.LruCacheV2;

/**
 *  LruCache实现，V3版本，线程安全+高并发
 *
 * Created on Jul, 2020 by @author bobo
 */
public class LruCacheV3<K, V> {

    private LruCacheV2<K, V>[] cacheSegments;

    public LruCacheV3(final int maxCapacity) {
        int cores = Runtime.getRuntime().availableProcessors();
        int concurrency = cores < 2 ? 2 : cores;
        cacheSegments = new LruCacheV2[concurrency];
        int segmentCapacity = maxCapacity / concurrency;
        if (maxCapacity % concurrency == 1) segmentCapacity++;
        for (int index = 0; index < cacheSegments.length; index++) {
            cacheSegments[index] = new LruCacheV2<>(segmentCapacity);
        }
    }

    public LruCacheV3(final int concurrency, final int maxCapacity) {
        cacheSegments = new LruCacheV2[concurrency];
        int segmentCapacity = maxCapacity / concurrency;
        if (maxCapacity % concurrency == 1) segmentCapacity++;
        for (int index = 0; index < cacheSegments.length; index++) {
            cacheSegments[index] = new LruCacheV2<>(segmentCapacity);
        }
    }

    private int stripeIndex(K key) {
        int hashCode = Math.abs(key.hashCode() * 31);
        return hashCode % cacheSegments.length;
    }

    private LruCacheV2<K, V> cache(K key) {
        return cacheSegments[stripeIndex(key)];
    }

    public void put(K key, V value) {
        cache(key).put(key, value);
    }

    public V get(K key) {
        return cache(key).get(key);
    }

    public int size() {
        int size = 0;
        for (LruCacheV2<K, V> cache : cacheSegments) {
            size += cache.size();
        }
        return size;
    }
}
