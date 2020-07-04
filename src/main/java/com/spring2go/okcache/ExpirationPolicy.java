package com.spring2go.okcache;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class ExpirationPolicy {
    public static ExpirationPolicy never() {
        return new ExpirationPolicy(-1l, -1l, -1l);
    }

    public static ExpirationPolicy afterAccess(long ms) {
        return new ExpirationPolicy(ms, -1l, -1l);
    }

    public static ExpirationPolicy afterWrite(long ms) {
        return new ExpirationPolicy(-1l, ms, -1l);
    }

    public static ExpirationPolicy afterCreate(long ms) {
        return new ExpirationPolicy(-1l, -1l, ms);
    }

    public static ExpirationPolicy afterAccessOrWrite(long access, long write) {
        return new ExpirationPolicy(access, write, -1l);
    }

    public static ExpirationPolicy after(long access, long write, long create) {
        return new ExpirationPolicy(access, write, create);
    }

    long afterAccess = -1l;
    long afterWrite = -1l;
    long afterCreate = -1l;

    private ExpirationPolicy(long afterAccess, long afterWrite, long afterCreate) {
        this.afterAccess = afterAccess;
        this.afterWrite = afterWrite;
        this.afterCreate = afterCreate;
    }

    public long getAfterAccess() {
        return afterAccess;
    }

    public long getAfterWrite() {
        return afterWrite;
    }

    public long getAfterCreate() {
        return afterCreate;
    }
}
