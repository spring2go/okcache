package com.spring2go.okcache;

import java.io.Serializable;

/**
 * Created on Jul, 2020 by @author bobo
 */
public interface CacheEntry<K extends Serializable, V extends Serializable> extends Serializable {
    K getKey();

    V getValue();

    void setValue(V value);

    /**
     * Returns the time that this entry was last accessed, in ms.
     */
    long getAccessTime();

    /**
     * Sets the entry access time in ns.
     */
    void setAccessTime(long time);

    /**
     * Returns the time that this entry was last written, in ms.
     */
    long getWriteTime();

    /**
     * Sets the entry write time in ms.
     */
    void setWriteTime(long time);

    /**
     * Returns the time that this entry was created, in ms.
     */
    long getCreateTime();

    /**
     * Sets the entry created time in ms.
     */
    void setCreateTime(long time);

    long getExpireAfterAccess();
    void setExpireAfterAccess(long time);
    long getExpireAfterWrite();
    void setExpireAfterWrite(long time);
    long getExpireAfterCreate();
    void setExpireAfterCreate(long time);
}
