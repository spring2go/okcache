package com.spring2go.okcache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.spring2go.okcache.impl.CacheImpl;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheBuilder<K extends Serializable, V extends Serializable> {
    private int initCapacity = 1;
    private long maximumSize = 2000;
    private int concurrencyLevel = 1;

    private long expireAfterAccess = -1l;
    private long expireAfterWrite = -1l;
    private long expireAfterCreate = -1l;

    private float evictionStartFactor = 0.85f;
    private float evictionStopFactor = 0.70f;

    private final List<CacheStore<K, V>> evictStores = new ArrayList<CacheStore<K, V>>();
    private final List<BehindStore<K, V>> writeBehindStores = new ArrayList<BehindStore<K, V>>();

    public CacheBuilder<K, V> initialCapacity(int initialCapacity) {
        checkArgument(initialCapacity >= 0);
        this.initCapacity = initialCapacity;
        return this;
    }

    public CacheBuilder<K, V> maximumSize(long size) {
        checkArgument(size >= 0, "maximum size must not be negative");
        this.maximumSize = size;
        return this;
    }

    public CacheBuilder<K, V> concurrencyLevel(int concurrencyLevel) {
        checkArgument(concurrencyLevel > 0);
        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    public CacheBuilder<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        checkArgument(duration >= 0, "duration cannot be negative:" + duration + unit);
        this.expireAfterAccess = unit.toMillis(duration);
        return this;
    }

    public CacheBuilder<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        checkArgument(duration >= 0, "duration cannot be negative:" + duration + unit);
        this.expireAfterWrite = unit.toMillis(duration);
        return this;
    }

    public CacheBuilder<K, V> expireAfterCreate(long duration, TimeUnit unit) {
        checkArgument(duration >= 0, "duration cannot be negative:" + duration + unit);
        this.expireAfterCreate = unit.toMillis(duration);
        return this;
    }

    public CacheBuilder<K, V> evictionFactors(float startFactor, float stopFactor) {
        checkArgument(startFactor > 0 && startFactor < 1 && stopFactor > 0 && stopFactor < 1, "eviction factors must be between 0 an 1");
        checkArgument(startFactor > stopFactor, "startFactor must be great than stopFactor");
        this.evictionStartFactor = startFactor;
        this.evictionStopFactor = stopFactor;
        return this;
    }

    public CacheBuilder<K, V> addEvictStore(CacheStore<K,V> store) {
        checkArgument(store!=null, "evictStore must not be null.");
        checkArgument(!evictStores.contains(store), "the evictStore has already added");

        evictStores.add(store);
        return this;
    }

    public CacheBuilder<K, V> addWriteBehindStore(BehindStore<K,V> store) {
        checkArgument(store!=null, "writeBehindStore must not be null.");
        checkArgument(!writeBehindStores.contains(store), "the writeBehindStore has already added");

        writeBehindStores.add(store);
        return this;
    }

    public Cache<K,V> build() {
        return new CacheImpl<K, V>(initCapacity, maximumSize, concurrencyLevel, expireAfterAccess, expireAfterWrite, expireAfterCreate, evictionStartFactor, evictionStopFactor, evictStores, writeBehindStores);
    }

    public static CacheBuilder<? extends Serializable, ? extends Serializable> newBuilder(){
        return new CacheBuilder<Serializable, Serializable>();
    }

    public static <K extends Serializable,V extends Serializable> CacheBuilder<K, V> newBuilder(Class<K> key, Class<V> value){
        return new CacheBuilder<K, V>();
    }


    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
}
