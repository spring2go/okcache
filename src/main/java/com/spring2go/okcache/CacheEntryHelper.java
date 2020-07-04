package com.spring2go.okcache;

import java.io.Serializable;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheEntryHelper {
    public static boolean isExpired(CacheEntry entry, long msTime){
        long t;
        if((t = entry.getExpireAfterAccess()) > 0 && (msTime - entry.getAccessTime() > t)) return true;
        if((t = entry.getExpireAfterWrite()) > 0 && (msTime - entry.getWriteTime() > t)) return true;
        if((t = entry.getExpireAfterCreate()) > 0 && (msTime - entry.getCreateTime() > t)) return true;
        return false;
    }

    public static <K extends Serializable, V extends Serializable> long calculateTTL(CacheEntry<K, V> entry, long msTime) {
        long ttl = Long.MAX_VALUE, t;
        if((t = entry.getExpireAfterAccess()) > 0) ttl = entry.getAccessTime() + t - msTime;
        if((t = entry.getExpireAfterWrite()) > 0) ttl = Math.min(ttl,entry.getWriteTime() + t - msTime);
        if((t = entry.getExpireAfterCreate()) > 0) ttl = Math.min(ttl,entry.getCreateTime() + t - msTime);
        return ttl;
    }

    public static <K extends Serializable, V extends Serializable> long calculateExpiredTimeInMS(CacheEntry<K, V> entry) {
        long now = TimeHelper.nowMs();
        long ttl = calculateTTL(entry, now);
        if(ttl == Long.MAX_VALUE) return ttl;
        return now + ttl;
    }
}
