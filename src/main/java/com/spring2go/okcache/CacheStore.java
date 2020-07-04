package com.spring2go.okcache;

import java.io.Serializable;

/**
 * Created on Jul, 2020 by @author bobo
 */
public interface CacheStore<K extends Serializable, V extends Serializable> {
    void store(CacheEntry<K, V> entry) throws Exception;

    CacheEntry<K, V> load(K key) throws Exception;

    CacheEntry<K, V> remove(K key) throws Exception;

    void close();
}
