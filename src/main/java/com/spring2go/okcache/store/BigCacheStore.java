package com.spring2go.okcache.store;

import com.spring2go.bigcache.BigCache;
import com.spring2go.okcache.*;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class BigCacheStore<K extends Serializable, V extends Serializable> implements CacheStore<K, V> {
    private BigCache<K> cache;
    private Serializer serializer = new JavaSerializer();

    public BigCacheStore(BigCache<K> cache) {
        this.cache = cache;
    }

    @Override
    public void store(CacheEntry<K, V> entry) throws Exception {
        if (entry == null) {
            return;
        }

        K key = entry.getKey();
        if (key == null) {
            return;
        }

        byte[] value= serializer.serialize(entry);
        if (value == null || value.length == 0) {
            return;
        }

        long ttl = CacheEntryHelper.calculateTTL(entry, TimeHelper.nowMs());
        cache.put(key, value, ttl);
    }

    @Override
    public CacheEntry load(K key) throws Exception {
        return remove(key);
    }

    @Override
    public CacheEntry remove(K key) throws Exception {
        if (key == null) {
            return null;
        }

        byte[] value = cache.delete(key);
        if (value == null) {
            return null;
        }

        return serializer.deserialize(value, CacheEntry.class);

    }

    @Override
    public void close() {
        try {
            cache.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
