package com.spring2go.bigcache;

import com.spring2go.bigcache.storage.Pointer;

/**
 * Created on Jul, 2020 by @author bobo
 *
 * Wrapper class in BigCache, which contains info on access time, ttl and storage.
 *
 * The {@link BigCache} will protect the r/w operation on this object by two means:
 * 1. use a striped write lock in its write operations.
 * 2. synchronize the CacheValueWrapper object in the read operations of Cache, as there may be
 * multiple threads working on this simultaneously.
 */
public class CacheValueWrapper {
    /**
     * The backend storage info of this entry.
     *
     */
    protected Pointer pointer;

    /**
     * The access time in milliseconds.
     *
     * Read/write of this field should be guarded by locks.
     */
    protected long lastAccessTime = -1; // -1 means for not initialized.

    /** Time to idle in milliseconds */
    protected long timeToIdle = -1L;

    /**
     * Gets the last access time.
     *
     * @return the access time
     */
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public CacheValueWrapper(Pointer pointer, long lastAccessTime, long timeToIdle) {
        this.pointer = pointer;
        this.lastAccessTime = lastAccessTime;
        this.timeToIdle = timeToIdle;
    }

    public CacheValueWrapper() {
    }

    /**
     * Sets the last access time.
     *
     * we need to put the following restriction:
     * 1. We will always set the time to a bigger value than the current one.
     * 2. If the pointer has expired, don't set the value, so there won't be <em>expire</em> to <em>non-expire</em>, which
     * is wrong
     *
     * @param accessTime the new access time
     * @return true if we has modified the time successfully.
     */
    public void setLastAccessTime(long accessTime) {
        if (lastAccessTime < 0) {
            // not initialized yet.
            lastAccessTime = accessTime;
            return;
        }

        // don't set it to an old value
        if (lastAccessTime >= accessTime) return;

        // can't update the access value if it has already expired.
        if (isExpired()) return;

        lastAccessTime = accessTime;
    }

    /**
     * Gets the time to idle in milliseconds
     *
     * @return time to idle
     */
    public long getTimeToIdle() {
        return timeToIdle;
    }

    /**
     * Sets the time to idle in milliseconds
     *
     * @param timeToIdle the new time to idle
     */
    public void setTimeToIdle(long timeToIdle) {
        this.timeToIdle = timeToIdle;
    }

    public Pointer getPointer() {
        return pointer;
    }

    public void setPointer(Pointer pointer) {
        this.pointer = pointer;
    }

    /**
     * Is the cached item expired
     *
     * @return expired or not
     */
    public boolean isExpired() {
        if (this.timeToIdle <= 0) return false; // never expire
        if (this.lastAccessTime < 0) return false; // not initialized
        return System.currentTimeMillis() - this.lastAccessTime > this.timeToIdle;
    }
}
