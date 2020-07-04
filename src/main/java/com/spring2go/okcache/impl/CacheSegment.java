package com.spring2go.okcache.impl;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import sun.misc.Unsafe;

import com.spring2go.okcache.BehindStore;
import com.spring2go.okcache.Cache.AccessLevel;
import com.spring2go.okcache.Cache.UpdateTimestamp;
import com.spring2go.okcache.CacheStats;
import com.spring2go.okcache.CacheStore;
import com.spring2go.okcache.ExpirationPolicy;

/**
 *
 * Created on Jul, 2020 by @author bobo
 */
public class CacheSegment<K extends Serializable, V extends Serializable> extends ReentrantLock {
    static final int MAX_SCAN_RETRIES = Runtime.getRuntime().availableProcessors() > 1 ? 64 : 1;
    static final int MAXIMUM_CAPACITY = Integer.MAX_VALUE;

    private volatile HashCacheEntry[] table;
    private volatile SegmentAccessQueue accessQueue = new SegmentAccessQueue();
    private volatile SegmentStatsCounter statsCounter = new SegmentStatsCounter();

    private final List<CacheStore<K, V>> evictStores = new ArrayList<CacheStore<K, V>>();
    private final List<BehindStore<K, V>> writeBehindStores = new ArrayList<BehindStore<K, V>>();

    private int initCapacity = 32;
    private long maximumSize = 2000;

    private float evictionStartFactor = 0.85f;
    private float evictionStopFactor = 0.70f;

    transient int count;
    transient int modCount;
    transient int threshold;
    final float loadFactor;

    public CacheSegment(int initCap, long maximumSize, float evictionStartFactor, float evictionStopFactor, List<CacheStore<K, V>> evictStores, List<BehindStore<K, V>> writeBehindStores) {
        this.initCapacity = Math.min(initCap, (int) maximumSize);
        this.maximumSize = Math.max(initCap, maximumSize);

        this.evictionStartFactor = Math.max(evictionStartFactor, evictionStopFactor);
        this.evictionStopFactor = Math.min(evictionStopFactor, evictionStartFactor);

        this.evictStores.addAll(evictStores);
        this.writeBehindStores.addAll(writeBehindStores);

        int cap = 2;
        while (cap < initCapacity) cap = cap << 1;

        table = new HashCacheEntry[cap];

        this.loadFactor = 0.7f;
        this.threshold = (int) (cap * this.loadFactor);
    }

    public V get(int hash, K key, AccessLevel level, UpdateTimestamp strategy) {

        if (!tryLock()) scanAndLock(key, hash);
        long now = now();
        V value = null;
        HashCacheEntry<K, V> e = null, pred = null;
        try {
            HashCacheEntry<K, V>[] tab = table;
            int index = (tab.length - 1) & hash;
            HashCacheEntry<K, V> first = entryAt(tab, index);

            //Look up the entry in the memory table
            for (e = first; e != null; pred = e, e = e.next) {
                K k;
                if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                    break;
                }
            }

            if (e != null) {
                //The entry is in memory
                if (isExpired(e, now)) {
                    //Remove it
                    accessQueue.remove(e);
                    if (pred == null) {
                        setEntryAt(tab, index, e.next);
                    } else {
                        pred.setNext(e.next);
                    }

                    //Update counters
                    ++modCount;
                    --count;

                    statsCounter.recordExpires(1);
                    statsCounter.memorySizeDecrement();
                    statsCounter.sizeDecrement();
                    statsCounter.misses(1);
                    statsCounter.memoryMisses(1);
                } else {
                    value = e.value;

                    e.setAccessTime(now);
                    if (strategy.updateWriteTime()) e.setWriteTime(now);
                    if (strategy.updateCreateTime()) e.setCreateTime(now);

                    accessQueue.addLast(e);
                    statsCounter.memoryHits(1);
                    statsCounter.hits(1);
                }
            } else {
                //The entry isn't in memory
                //Try to activate from evict store.
                HashCacheEntry<K, V> activation = activateEntry(key, now, level);
                if (activation != null) {
                    e = activation;
                    value = e.value;

                    //Push it into memory
                    e.setNext(first);
                    e.setAccessTime(now);
                    if (strategy.updateWriteTime()) e.setWriteTime(now);
                    if (strategy.updateCreateTime()) e.setCreateTime(now);

                    setEntryAt(tab, index, e);
                    accessQueue.addLast(e);

                    statsCounter.memorySizeIncrement();
                    statsCounter.hits(1);
                }else{
                    statsCounter.misses(1);
                }
                statsCounter.memoryMisses(1);
            }
        } finally {
            if (value != null) updateToWBStores(e);
            tryClean();
            unlock();
        }
        return value;
    }

    public V put(int hash, K key, V value, ExpirationPolicy expirationPolicy, boolean onlyIfAbsent) {
        HashCacheEntry<K, V> node = tryLock() ? null : scanAndLockForPut(key, hash, value, expirationPolicy);
        long now = now();
        V oldValue = null;
        try {
            HashCacheEntry<K, V>[] tab = table;
            int index = (tab.length - 1) & hash;
            HashCacheEntry<K, V> first = entryAt(tab, index);
            HashCacheEntry<K, V> e;

            //Look up the entry in the memory table
            for (e = first; e != null; e = e.next) {
                K k;
                if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                    break;
                }
            }

            if (e != null) {
                //The entry is in memory
                if (isExpired(e, now)) {
                    reincarnate(now, e, value, expirationPolicy);
                    accessQueue.addLast(e);

                    statsCounter.recordExpires(1);
                    statsCounter.recordCreates(1);
                    ++modCount;
                } else {
                    oldValue = e.value;
                    if (!onlyIfAbsent) {
                        reincarnate(now, e, value, expirationPolicy);
                        accessQueue.addLast(e);

                        ++modCount;
                        statsCounter.recordCreates(1);
                    }
                }
            } else {
                HashCacheEntry<K, V> activation = activateEntry(key, now);
                if (onlyIfAbsent) {
                    //The entry isn't in memory
                    //Try to activate from evict store.
                    //Check if the key exists.
                    e = activation;
                    if (e != null) {
                        oldValue = e.value;
                        statsCounter.memorySizeIncrement();
                    }
                }

                if (e == null) {
                    if (node != null) {
                        e = node;
                    } else {
                        e = new HashCacheEntry<K, V>(hash, key, value, expirationPolicy);
                    }
                    statsCounter.recordCreates(1);
                    statsCounter.memorySizeIncrement();
                    statsCounter.sizeIncrement();
                }

                e.setNext(first);
                e.setNextInAccessQueue(null);
                e.setPreviousInAccessQueue(null);
                accessQueue.addLast(e);

                int c = count + 1;
                if (c > threshold && tab.length < MAXIMUM_CAPACITY)
                    rehash(e);
                else
                    setEntryAt(tab, index, e);

                ++modCount;
                count = c;
            }
            updateToWBStores(e);
        } finally {
            tryClean();
            unlock();
        }
        return oldValue;
    }

    /**
     * Doubles size of table and repacks entries, also adding the
     * given node to new table
     */
    @SuppressWarnings("unchecked")
    private void rehash(HashCacheEntry<K, V> node) {
        /*
         * Reclassify nodes in each list to new table.  Because we
         * are using power-of-two expansion, the elements from
         * each bin must either stay at same index, or move with a
         * power of two offset. We eliminate unnecessary node
         * creation by catching cases where old nodes can be
         * reused because their next fields won't change.
         * Statistically, at the default threshold, only about
         * one-sixth of them need cloning when a table
         * doubles. The nodes they replace will be garbage
         * collectable as soon as they are no longer referenced by
         * any reader thread that may be in the midst of
         * concurrently traversing table. Entry accesses use plain
         * array indexing because they are followed by volatile
         * table write.
         */
        HashCacheEntry<K, V>[] oldTable = table;
        int oldCapacity = oldTable.length;
        int newCapacity = oldCapacity << 1;
        threshold = (int) (newCapacity * loadFactor);
        HashCacheEntry<K, V>[] newTable = (HashCacheEntry<K, V>[]) new HashCacheEntry[newCapacity];
        int sizeMask = newCapacity - 1;
        for (int i = 0; i < oldCapacity; i++) {
            HashCacheEntry<K, V> e = oldTable[i];
            if (e != null) {
                HashCacheEntry<K, V> next = e.next;
                int idx = e.hash & sizeMask;
                if (next == null)   //  Single node on list
                    newTable[idx] = e;
                else { // Reuse consecutive sequence at same slot
                    HashCacheEntry<K, V> lastRun = e;
                    int lastIdx = idx;
                    for (HashCacheEntry<K, V> last = next; last != null; last = last.next) {
                        int k = last.hash & sizeMask;
                        if (k != lastIdx) {
                            lastIdx = k;
                            lastRun = last;
                        }
                    }
                    newTable[lastIdx] = lastRun;
                    // Clone remaining nodes
                    for (HashCacheEntry<K, V> p = e; p != lastRun;) {

                        HashCacheEntry<K, V> entry = p;
                        p = p.next;

                        int h = entry.hash;
                        int k = h & sizeMask;
                        HashCacheEntry<K, V> n = newTable[k];
                        entry.setNext(n);
                        newTable[k] = entry;
                    }
                }
            }
        }
        int nodeIndex = node.hash & sizeMask; // add the new node
        node.setNext(newTable[nodeIndex]);
        newTable[nodeIndex] = node;
        table = newTable;
    }

    /**
     * Warm up lookup process and try to build a new node if there has not one, before lock.
     *
     * @param key
     * @param hash
     * @param value
     * @param expirationPolicy @return
     */
    private HashCacheEntry<K, V> scanAndLockForPut(K key, int hash, V value, ExpirationPolicy expirationPolicy) {
        HashCacheEntry<K, V> first = entryForHash(this, hash);
        HashCacheEntry<K, V> e = first;
        HashCacheEntry<K, V> node = null;
        int retries = -1; // negative while locating node
        while (!tryLock()) {
            HashCacheEntry<K, V> f; // to recheck first below
            if (retries < 0) {
                if (e == null) {
                    if (node == null) // speculatively create node
                        node = new HashCacheEntry<K, V>(hash, key, value, expirationPolicy);
                    retries = 0;
                } else if (Objects.deepEquals(key, e.key))
                    retries = 0;
                else
                    e = e.next;
            } else if (++retries > MAX_SCAN_RETRIES) {
                lock();
                break;
            } else if ((retries & 1) == 0 &&
                    (f = entryForHash(this, hash)) != first) {
                e = first = f; // re-traverse if entry changed
                retries = -1;
            }
        }
        return node;
    }

    /**
     * Scans for a node containing the given key while trying to
     * acquire lock for a remove or replace operation. Upon
     * return, guarantees that lock is held.  Note that we must
     * lock even if the key is not found, to ensure sequential
     * consistency of updates.
     */
    private void scanAndLock(Object key, int hash) {
        // similar to but simpler than scanAndLockForPut
        HashCacheEntry<K, V> first = entryForHash(this, hash);
        HashCacheEntry<K, V> e = first;
        int retries = -1;
        while (!tryLock()) {
            HashCacheEntry<K, V> f;
            if (retries < 0) {
                if (e == null || Objects.deepEquals(key, e.key))
                    retries = 0;
                else
                    e = e.next;
            } else if (++retries > MAX_SCAN_RETRIES) {
                lock();
                break;
            } else if ((retries & 1) == 0 &&
                    (f = entryForHash(this, hash)) != first) {
                e = first = f;
                retries = -1;
            }
        }
    }

    /**
     * Remove; match on key only if value null, else match both.
     */
    public V remove(K key, int hash, V value) {
        if (!tryLock()) scanAndLock(key, hash);
        long now = now();
        V oldValue = null;
        try {
            HashCacheEntry<K, V>[] tab = table;
            int index = (tab.length - 1) & hash;
            HashCacheEntry<K, V> first = entryAt(tab, index);
            HashCacheEntry<K, V> e, pred = null;

            //Look up the entry in the memory table
            for (e = first; e != null; pred = e, e = e.next) {
                K k;
                if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                    break;
                }
            }

            if (e != null) {
                //The entry is in memory
                if (isExpired(e, now)) {
                    //Remove it
                    accessQueue.remove(e);
                    if (pred == null) {
                        setEntryAt(tab, index, e.next);
                    } else {
                        pred.setNext(e.next);
                    }

                    //Update counters
                    ++modCount;
                    --count;
                    statsCounter.recordExpires(1);
                    statsCounter.memorySizeDecrement();
                    statsCounter.sizeDecrement();
                } else {
                    V v = e.value;
                    if (value == null || value == v || Objects.deepEquals(value, v)) {
                        oldValue = v;
                        //Remove it
                        accessQueue.remove(e);
                        if (pred == null) {
                            setEntryAt(tab, index, e.next);
                        } else {
                            pred.setNext(e.next);
                        }

                        //Update counters
                        ++modCount;
                        --count;
                        statsCounter.recordRemoves(1);
                        statsCounter.memorySizeDecrement();
                        statsCounter.sizeDecrement();

                    } else {
                        //Fresh the entry
                        accessQueue.addLast(e);
                    }
                }
            } else {
                //The entry isn't in memory
                //Try to activate from evict store.
                HashCacheEntry<K, V> activation = activateEntry(key, now);
                if (activation != null) {
                    e = activation;
                    V v = e.value;
                    if (value == null || value == v || Objects.deepEquals(value, v)) {
                        oldValue = v;
                        statsCounter.recordRemoves(1);
                        statsCounter.sizeDecrement();
                    } else {
                        //Push it into memory
                        e.setNext(first);
                        setEntryAt(tab, index, e);
                        accessQueue.addLast(e);

                        ++modCount;
                        ++count;

                        statsCounter.memorySizeIncrement();
                    }
                }
            }
        } finally {
            if (oldValue != null) removeFromWBStores(key);
            tryClean();
            unlock();
        }
        return oldValue;
    }

    public boolean replace(K key, int hash, V oldValue, V newValue) {
        if (!tryLock()) scanAndLock(key, hash);

        long now = now();
        boolean replaced = false;
        HashCacheEntry<K, V> e = null, pred = null;
        try {
            HashCacheEntry<K, V>[] tab = table;
            int index = (tab.length - 1) & hash;
            HashCacheEntry<K, V> first = entryAt(tab, index);

            //Look up the entry in the memory table
            for (e = first; e != null; pred = e, e = e.next) {
                K k;
                if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                    break;
                }
            }

            if (e != null) {
                if (isExpired(e, now)) {
                    //Remove it
                    accessQueue.remove(e);
                    if (pred == null) {
                        setEntryAt(tab, index, e.next);
                    } else {
                        pred.setNext(e.next);
                    }

                    //Update counters
                    ++modCount;
                    --count;
                    statsCounter.recordExpires(1);
                    statsCounter.memorySizeDecrement();
                    statsCounter.sizeDecrement();
                } else {
                    if (Objects.deepEquals(oldValue, e.value)) {
                        e.setValue(newValue);
                        ++modCount;
                        e.setWriteTime(now);
                        replaced = true;

                        statsCounter.recordUpdates(1);
                    }
                    accessQueue.addLast(e);
                }
            } else {
                //The entry isn't in memory
                //Try to activate from evict store.
                HashCacheEntry<K, V> activation = activateEntry(key, now);
                if (activation != null) {
                    e = activation;
                    V v = e.value;
                    if (Objects.deepEquals(oldValue, v)) {
                        e.setValue(newValue);
                        e.setWriteTime(now);
                        replaced = true;

                        statsCounter.recordUpdates(1);
                    }
                    //Push it into memory
                    e.setNext(first);
                    setEntryAt(tab, index, e);
                    accessQueue.addLast(e);

                    ++modCount;
                    ++count;

                    statsCounter.memorySizeIncrement();
                }
            }
        } finally {
            if (replaced) updateToWBStores(e);
            tryClean();
            unlock();
        }
        return replaced;
    }

    public V replace(K key, int hash, V value) {
        if (!tryLock()) scanAndLock(key, hash);

        long now = now();
        V oldValue = null;
        HashCacheEntry<K, V> e = null, pred = null;
        try {
            HashCacheEntry<K, V>[] tab = table;
            int index = (tab.length - 1) & hash;
            HashCacheEntry<K, V> first = entryAt(tab, index);

            //Look up the entry in the memory table
            for (e = first; e != null; pred = e, e = e.next) {
                K k;
                if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                    break;
                }
            }

            if (e != null) {
                if (isExpired(e, now)) {
                    //Remove it
                    accessQueue.remove(e);
                    if (pred == null) {
                        setEntryAt(tab, index, e.next);
                    } else {
                        pred.setNext(e.next);
                    }

                    //Update counters
                    ++modCount;
                    --count;
                    statsCounter.recordExpires(1);
                    statsCounter.memorySizeDecrement();
                    statsCounter.sizeDecrement();
                } else {
                    oldValue = e.value;
                    e.setValue(value);
                    e.setWriteTime(now);

                    accessQueue.addLast(e);

                    ++modCount;
                    statsCounter.recordUpdates(1);
                }
            } else {
                //The entry isn't in memory
                //Try to activate from evict store.
                HashCacheEntry<K, V> activation = activateEntry(key, now);
                if (activation != null) {
                    e = activation;

                    oldValue = e.value;
                    e.setValue(value);
                    e.setWriteTime(now);

                    //Push it into memory
                    e.setNext(first);
                    setEntryAt(tab, index, e);

                    accessQueue.addLast(e);

                    ++modCount;
                    ++count;
                    statsCounter.recordUpdates(1);
                    statsCounter.memorySizeIncrement();
                }
            }
        } finally {
            if (oldValue != null) updateToWBStores(e);
            tryClean();
            unlock();
        }
        return oldValue;
    }

    public boolean contains(int hash, K key) {
        int maxRetries = MAX_SCAN_RETRIES;
        int retries = 0;
        do {
            HashCacheEntry<K, V> e = null;
            HashCacheEntry<K, V>[] tab = table;
            int index = (tab.length - 1) & hash;
            HashCacheEntry<K, V> first = entryAt(tab, index);

            //Look up the entry in the memory table
            for (e = first; e != null; e = e.next) {
                K k;
                if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                    if(!isExpired(e,now())){
                        return true;
                    }
                }
            }
        } while (!tryLock() && retries++ < maxRetries);

        if (!isLocked()) lock();

        long now = now();
        boolean contained = false;
        HashCacheEntry<K, V> e = null, pred = null;
        try {
            HashCacheEntry<K, V>[] tab = table;
            int index = (tab.length - 1) & hash;
            HashCacheEntry<K, V> first = entryAt(tab, index);

            //Look up the entry in the memory table
            for (e = first; e != null; pred = e, e = e.next) {
                K k;
                if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                    break;
                }
            }

            if (e != null) {
                if (isExpired(e, now)) {
                    //Remove it
                    accessQueue.remove(e);
                    if (pred == null) {
                        setEntryAt(tab, index, e.next);
                    } else {
                        pred.setNext(e.next);
                    }

                    //Update counters
                    ++modCount;
                    --count;
                    statsCounter.recordExpires(1);
                    statsCounter.memorySizeDecrement();
                    statsCounter.sizeDecrement();
                } else {
                    contained = true;
                    accessQueue.addLast(e);
                }
            } else {
                //The entry isn't in memory
                //Try to activate from evict store.
                HashCacheEntry<K, V> activation = activateEntry(key, now);
                if (activation != null) {
                    e = activation;
                    contained = true;

                    //Push it into memory
                    e.setNext(first);
                    setEntryAt(tab, index, e);
                    accessQueue.addLast(e);

                    ++modCount;
                    ++count;

                    statsCounter.memorySizeIncrement();
                }
            }
        } finally {
            tryClean();
            unlock();
        }
        return contained;
    }


    public CacheStats stats() {
        return statsCounter.snapshot();
    }

    private void removeFromWBStores(K key) {
        for (CacheStore<K, V> store : writeBehindStores) {
            try {
                store.remove(key);
            } catch (Exception e) {
                statsCounter.behindStoreException(1);
            }
        }
    }

    private HashCacheEntry<K, V> activateEntry(K key, long now) {
        return activateEntry(key, now, AccessLevel.EVICT_STORE);
    }

    private HashCacheEntry<K, V> activateEntry(K key, long now, AccessLevel accessLevel) {
        HashCacheEntry<K, V> storeEntry = loadEntryFromStores(key, accessLevel);
        if (storeEntry == null || isExpired(storeEntry, now)) {
            return null;
        }
        return storeEntry;
    }

    //It should not load from stores too frequent in a  healthy situation
    private HashCacheEntry<K, V> loadEntryFromStores(K key, AccessLevel accessLevel) {
        HashCacheEntry<K, V> entry = null;

        if (accessLevel.accessEvictStore()) {
            for (CacheStore<K, V> store : evictStores) {
                try {
                    entry = (HashCacheEntry<K, V>) store.remove(key);
                } catch (Exception e) {
                    statsCounter.evictStoreException(1);
                }
                if (entry != null) {
                    statsCounter.evictStoreHits(1);
                    return entry;
                }
            }
            statsCounter.evictStoreMisses(1);
        }

        if (accessLevel.accessBehindStore()) {
            for (CacheStore<K, V> store : writeBehindStores) {
                try {
                    entry = (HashCacheEntry<K, V>) store.load(key);
                } catch (Exception e) {
                    statsCounter.behindStoreException(1);
                }
                if (entry != null) {
                    statsCounter.behindStoreHits(1);
                    return entry;
                }
            }
            statsCounter.behindStoreMisses(1);
        }
        return null;
    }

    private void updateToWBStores(HashCacheEntry<K, V> entry) {
        for (CacheStore<K, V> store : writeBehindStores) {
            try {
                store.store(entry);
            } catch (Exception e) {
                statsCounter.behindStoreException(1);
            }
        }
    }

    void tryClean() {
        tryExpire();
        tryEvict();
    }

    private void tryExpire() {
        long now = now();
        HashCacheEntry<K, V> entry = null;
        entry = accessQueue.peek();
        while (entry != null && isExpired(entry, now)) {
            _remove(entry.key, entry.hash);
            removeFromEvictStores(entry);

            statsCounter.recordExpires(1);
            statsCounter.sizeDecrement();

            entry = accessQueue.peek();
        }
    }

    private void tryEvict() {
        long s = statsCounter.memorySize;
        long evictionCount = s > maximumSize * evictionStartFactor ? (long) (s - maximumSize * evictionStopFactor) : 0l;

        if (evictionCount <= 0) return;

        HashCacheEntry<K, V> entry = null;

        entry = accessQueue.peek();
        do {
            _remove(entry.key, entry.hash);

            addToEvictStores(entry);
            statsCounter.recordEvicts(1);

            evictionCount--;
            entry = accessQueue.peek();
        } while (evictionCount > 0 && entry != null);
    }

    //Remove from memory. Assume there has been locked.
    private void _remove(K key, int hash) {
        HashCacheEntry<K, V>[] tab = table;
        int index = (tab.length - 1) & hash;
        HashCacheEntry<K, V> first = entryAt(tab, index);
        HashCacheEntry<K, V> e, pred = null;

        //Look up the entry in the memory table
        for (e = first; e != null; pred = e, e = e.next) {
            K k;
            if ((k = e.key) == key || (e.hash == hash && Objects.deepEquals(key, k))) {
                accessQueue.remove(e);
                if (pred == null) {
                    setEntryAt(tab, index, e.next);
                } else {
                    pred.setNext(e.next);
                }
                //Update counters
                ++modCount;
                --count;
                statsCounter.memorySizeDecrement();
                break;
            }
        }
    }


    private void removeFromEvictStores(HashCacheEntry<K, V> entry) {
        for (CacheStore<K, V> evictStore : evictStores) {
            try {
                evictStore.remove(entry.getKey());
            } catch (Exception e) {
                statsCounter.evictStoreException(1);
            }
        }
    }

    private void addToEvictStores(HashCacheEntry<K, V> entry) {
        for (CacheStore<K, V> evictStore : evictStores) {
            try {
                evictStore.store(entry);
            } catch (Exception e) {
                statsCounter.evictStoreException(1);
            }
        }
    }

    /**
     * Gets the table entry for the given hash
     */
    static final <K extends Serializable, V extends Serializable> HashCacheEntry<K, V> entryForHash(CacheSegment<K, V> seg, int h) {
        HashCacheEntry<K, V>[] tab;
        return (seg == null || (tab = seg.table) == null) ? null :
                (HashCacheEntry<K, V>) UNSAFE.getObjectVolatile
                        (tab, ((long) (((tab.length - 1) & h)) << TSHIFT) + TBASE);
    }

    /**
     * Gets the ith element of given table (if nonnull) with volatile
     * read semantics. Note: This is manually integrated into a few
     * performance-sensitive methods to reduce call overhead.
     */
    @SuppressWarnings("unchecked")
    static final <K extends Serializable, V extends Serializable> HashCacheEntry<K, V> entryAt(HashCacheEntry<K, V>[] tab, int i) {
        return (tab == null) ? null :
                (HashCacheEntry<K, V>) UNSAFE.getObjectVolatile
                        (tab, ((long) i << TSHIFT) + TBASE);
    }

    /**
     * Sets the ith element of given table, with volatile write
     * semantics. (See above about use of putOrderedObject.)
     */
    static final <K extends Serializable, V extends Serializable> void setEntryAt(HashCacheEntry<K, V>[] tab, int i,
                                                                                  HashCacheEntry<K, V> e) {
        UNSAFE.putOrderedObject(tab, ((long) i << TSHIFT) + TBASE, e);
    }

    static final boolean isExpired(HashCacheEntry entry, long msTime) {
        long t;
        if ((t = entry.getExpireAfterAccess()) > 0 && (msTime - entry.getAccessTime() > t)) return true;
        if ((t = entry.getExpireAfterWrite()) > 0 && (msTime - entry.getWriteTime() > t)) return true;
        if ((t = entry.getExpireAfterCreate()) > 0 && (msTime - entry.getCreateTime() > t)) return true;
        return false;
    }

    static final <K extends Serializable, V extends Serializable> void reincarnate(long time, HashCacheEntry<K, V> entry, V value, ExpirationPolicy expirationPolicy) {
        if (entry != null) {
            entry.setCreateTime(time);
            entry.setValue(value);

            entry.setExpireAfterAccess(expirationPolicy.getAfterAccess());
            entry.setExpireAfterWrite(expirationPolicy.getAfterWrite());
            entry.setExpireAfterCreate(expirationPolicy.getAfterCreate());
        }

    }

    static final long now() {
        return System.currentTimeMillis();
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe UNSAFE;
    private static final long TBASE;
    private static final int TSHIFT;

    static {
        int ts;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);

            Class sc = HashCacheEntry[].class;
            TBASE = UNSAFE.arrayBaseOffset(sc);
            ts = UNSAFE.arrayIndexScale(sc);
        } catch (Exception e) {
            throw new Error(e);
        }
        if ((ts & (ts - 1)) != 0)
            throw new Error("data type scale not a power of two");
        TSHIFT = 31 - Integer.numberOfLeadingZeros(ts);
    }
}
