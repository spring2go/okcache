package com.spring2go.okcache.impl;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import sun.misc.Unsafe;

import com.spring2go.okcache.BehindStore;
import com.spring2go.okcache.Cache;
import com.spring2go.okcache.CacheStats;
import com.spring2go.okcache.CacheStore;
import com.spring2go.okcache.ExpirationPolicy;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheImpl<K extends Serializable, V extends Serializable> implements Cache<K, V> {
    private final List<CacheStore<K, V>> evictStores = new ArrayList<CacheStore<K, V>>();
    private final List<BehindStore<K, V>> writeBehindStores = new ArrayList<BehindStore<K, V>>();

    private int initCapacity = 2000;
    private long maximumSize = 2000;
    private int concurrencyLevel = 1;

    private long expireAfterAccess = -1l;
    private long expireAfterWrite = -1l;
    private long expireAfterCreate = -1l;

    private float evictionStartFactor = 0.85f;
    private float evictionStopFactor = 0.70f;

    private int segmentCount = 1;
    private int segmentShift;
    private int segmentMask = 0;
    private final CacheSegment[] segments;

    public CacheImpl(int initCapacity, long maximumSize, int concurrencyLevel, long expireAfterAccess, long expireAfterWrite, long expireAfterCreate, float evictionStartFactor, float evictionStopFactor, List<CacheStore<K, V>> evictStores, List<BehindStore<K, V>> writeBehindStores) {
        this.initCapacity = Math.min(initCapacity, (int) maximumSize);
        this.maximumSize = Math.max(initCapacity, maximumSize);
        this.concurrencyLevel = Math.max(concurrencyLevel, 1);
        this.expireAfterAccess = expireAfterAccess;
        this.expireAfterWrite = expireAfterWrite;
        this.expireAfterCreate = expireAfterCreate;
        this.evictionStartFactor = Math.max(evictionStartFactor, evictionStopFactor);
        this.evictionStopFactor = Math.min(evictionStopFactor, evictionStartFactor);
        this.evictStores.addAll(evictStores);
        this.writeBehindStores.addAll(writeBehindStores);

        int scount=1;
        int sshift=0;
        while (scount < this.concurrencyLevel) {
            ++sshift;
            scount<<=1;
        }
        this.segmentCount = scount;
        this.segmentShift = sshift;

        int init = this.initCapacity / scount + 1;
        long max = this.maximumSize / scount + 1;

        this.segmentMask = this.segmentCount - 1;
        segments = new CacheSegment[scount];

        CacheSegment<K,V> s;
        for (int i = 0; i < scount; i++) {
            s = new CacheSegment<K, V>(init, max, evictionStartFactor, evictionStopFactor, evictStores, writeBehindStores);
            UNSAFE.putOrderedObject(segments, SBASE + (i << SSHIFT), s);
        }
    }

    @Override
    public V get(K key) {
        return get(key, AccessLevel.EVICT_STORE, UpdateTimestamp.Access);
    }

    @Override
    public V get(K key, AccessLevel level) {
        return get(key, level, UpdateTimestamp.Access);
    }

    @Override
    public V get(K key, UpdateTimestamp strategy) {
        return get(key, AccessLevel.EVICT_STORE, strategy);
    }

    @Override
    public V get(K key, AccessLevel level, UpdateTimestamp strategy) {
        int hash = hash(key);
        CacheSegment<K,V> s = segmentForHash(hash);
        return s.get(hash, key , level, strategy);
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, ExpirationPolicy.after(expireAfterAccess, expireAfterWrite, expireAfterCreate));
    }

    @Override
    public V put(K key, V value, ExpirationPolicy expirationPolicy) {
        int hash = hash(key);
        if (value == null)
            throw new NullPointerException();
        CacheSegment<K,V> s = segmentForHash(hash);
        return s.put(hash, key, value, expirationPolicy, false);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, ExpirationPolicy.after(expireAfterAccess, expireAfterWrite, expireAfterCreate));
    }

    @Override
    public V putIfAbsent(K key, V value, ExpirationPolicy expirationPolicy) {
        int hash = hash(key);
        if (value == null)
            throw new NullPointerException();
        CacheSegment<K,V> s = segmentForHash(hash);
        return s.put(hash, key, value, expirationPolicy, true);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpirationPolicy expirationPolicy) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue(), expirationPolicy);
        }
    }

    @Override
    public V replace(K key, V value) {
        int hash = hash(key);
        if (value == null)
            throw new NullPointerException();
        CacheSegment<K,V> s = segmentForHash(hash);
        return s == null ? null : s.replace(key, hash, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        int hash = hash(key);
        if (oldValue == null || newValue == null)
            throw new NullPointerException();
        CacheSegment<K,V> s = segmentForHash(hash);
        return s != null && s.replace(key, hash, oldValue, newValue);
    }

    @Override
    public V remove(K key) {
        int hash = hash(key);
        CacheSegment<K,V> s = segmentForHash(hash);
        return s == null ? null : s.remove(key, hash, null);
    }

    @Override
    public boolean remove(K key, V value) {
        int hash = hash(key);
        CacheSegment<K,V> s;
        return value != null && (s = segmentForHash(hash)) != null &&
                s.remove(key, hash, value) != null;
    }

    @Override
    public boolean contains(K key) {
        int hash = hash(key);
        if (key == null)
            return false;
        CacheSegment<K,V> s = segmentForHash(hash);
        return s.contains(hash, key);
    }

    @Override
    public CacheStats stats() {
        CacheStats stats = new CacheStats();
        for (CacheSegment<K, V> segament : segments) {
            stats = stats.plus(segament.stats());
        }
        return stats;
    }

    @Override
    public void close() {
        for(CacheStore cs : evictStores) {
            cs.close();
        }
        for(CacheStore cs : writeBehindStores) {
            cs.close();
        }
    }

    private int hash(Object k) {
        int h = 0;
        h ^= k.hashCode();

        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    @SuppressWarnings("unchecked")
    static final <K extends Serializable,V extends Serializable> CacheSegment<K,V> segmentAt(CacheSegment<K,V>[] ss, int j) {
        long u = (j << SSHIFT) + SBASE;
        return ss == null ? null :
                (CacheSegment<K,V>) UNSAFE.getObjectVolatile(ss, u);
    }


    // Hash-based segment and entry accesses
    /**
     * Get the segment for the given hash
     */
    @SuppressWarnings("unchecked")
    private CacheSegment<K,V> segmentForHash(int h) {
        long u = (((h >>> segmentShift) & segmentMask) << SSHIFT) + SBASE;
        return (CacheSegment<K,V>) UNSAFE.getObjectVolatile(segments, u);
    }

    // Unsafe mechanics
    private static final Unsafe UNSAFE;
    private static final long SBASE;
    private static final int SSHIFT;

    static {
        int ss;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);

            Class sc = CacheSegment[].class;
            SBASE = UNSAFE.arrayBaseOffset(sc);
            ss = UNSAFE.arrayIndexScale(sc);
        } catch (Exception e) {
            throw new Error(e);
        }
        if ((ss & (ss-1)) != 0)
            throw new Error("data type scale not a power of two");
        SSHIFT = 31 - Integer.numberOfLeadingZeros(ss);
    }
}
