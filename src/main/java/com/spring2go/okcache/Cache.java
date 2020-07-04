package com.spring2go.okcache;

import java.io.Serializable;
import java.util.Map;

/**
 * Created on Jul, 2020 by @author bobo
 */
public interface Cache<K extends Serializable, V extends Serializable> {
    V get(K key);

    V get(K key, AccessLevel level);

    V get(K key, UpdateTimestamp strategy);

    V get(K key, AccessLevel level, UpdateTimestamp strategy);

    V put(K key, V value);

    V put(K key, V value, ExpirationPolicy expirationPolicy);

    V putIfAbsent(K key, V value);

    V putIfAbsent(K key, V value, ExpirationPolicy expirationPolicy);

    void putAll(Map<? extends K, ? extends V> map);

    void putAll(Map<? extends K, ? extends V> map, ExpirationPolicy expirationPolicy);

    V replace(K key, V value);

    boolean replace(K key, V oldValue, V newValue);

    V remove(K key);

    boolean remove(K key, V value);

    boolean contains(K key);

    CacheStats stats();

    void close();

    enum AccessLevel{
        MEMORY(0),
        EVICT_STORE(1),
        BEHIND_STORE(2);

        private final int code;
        AccessLevel(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public boolean accessEvictStore(){
            return this.code >= EVICT_STORE.getCode();
        }

        public boolean accessBehindStore(){
            return this.code >= BEHIND_STORE.getCode();
        }
    }

    enum UpdateTimestamp {
        Access(0),
        Write(1),
        Create(2);

        private final int code;
        UpdateTimestamp(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public boolean updateAccessTime(){
            return this.code >= Access.getCode();
        }

        public boolean updateWriteTime(){
            return this.code >= Write.getCode();
        }

        public boolean updateCreateTime(){
            return this.code >= Create.getCode();
        }
    }
}
