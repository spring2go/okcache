package com.spring2go.okcache.impl;

import com.spring2go.okcache.AbstractDeque;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * Created on Jul, 2020 by @author bobo
 */
public class SegmentAccessQueue extends AbstractDeque<HashCacheEntry> {
    @Override
    public boolean offerFirst(HashCacheEntry entry) {
        if (entry == null) return false;

        if (entry.getNextInAccessQueue() != null)
            connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue());

        HashCacheEntry headNext = head.getNextInAccessQueue();
        connectAccessOrder(head, entry);
        connectAccessOrder(entry, headNext);

        return true;
    }

    @Override
    public boolean offerLast(HashCacheEntry entry) {
        if (entry == null) return false;

        if (entry.getNextInAccessQueue() != null)
            connectAccessOrder(entry.getPreviousInAccessQueue(), entry.getNextInAccessQueue());
        HashCacheEntry headPrevious = head.getPreviousInAccessQueue();
        connectAccessOrder(headPrevious, entry);
        connectAccessOrder(entry, head);

        return true;
    }

    @Override
    public HashCacheEntry pollFirst() {
        HashCacheEntry entry = head.getNextInAccessQueue();
        if (entry == head) return null;
        connectAccessOrder(head, entry.getNextInAccessQueue());
        nullifyAccessOrder(entry);
        return entry;
    }

    @Override
    public HashCacheEntry pollLast() {
        HashCacheEntry entry = head.getPreviousInAccessQueue();
        if (entry == head) return null;
        connectAccessOrder(entry.getPreviousInAccessQueue(), head);
        nullifyAccessOrder(entry);
        return entry;
    }

    @Override
    public HashCacheEntry peekFirst() {
        HashCacheEntry entry = head.getNextInAccessQueue();
        return entry == head ? null : entry;
    }

    @Override
    public HashCacheEntry peekLast() {
        HashCacheEntry entry = head.getPreviousInAccessQueue();
        return entry == head ? null : entry;
    }

    @Override
    public boolean remove(Object o) {
        HashCacheEntry entry = (HashCacheEntry) o;
        HashCacheEntry previous = entry.getPreviousInAccessQueue();
        HashCacheEntry next = entry.getNextInAccessQueue();

        if (previous == null) {
            return false;
        } else {
            connectAccessOrder(previous, next);
            nullifyAccessOrder(entry);
            return true;
        }

    }

    @Override
    public boolean contains(Object o) {
        HashCacheEntry entry = (HashCacheEntry) o;
        return entry.getNextInAccessQueue() != null;
    }

    @Override
    public boolean isEmpty() {
        return head.getNextInAccessQueue() == head;
    }

    @Override
    public void clear() {
        HashCacheEntry e, next;
        e = head.getNextInAccessQueue();
        while (e != head) {
            next = e.getNextInAccessQueue();
            nullifyAccessOrder(e);
            e = next;
        }
        connectAccessOrder(head, head);
    }

    @Override
    public Iterator<HashCacheEntry> iterator() {
        return new Iterator<HashCacheEntry>() {
            private HashCacheEntry next = peek();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public HashCacheEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                try {
                    return next;
                } finally {
                    HashCacheEntry n = next().getNextInAccessQueue();
                    next = n == head ? null : n;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public int size() {
        int size = 0;
        HashCacheEntry entry = head;
        while ((entry = entry.getNextInAccessQueue()) != head) size++;
        return size;
    }

    final HashCacheEntry head = new HeadHashCacheEntry();


    void connectAccessOrder(HashCacheEntry previous, HashCacheEntry next) {
        previous.setNextInAccessQueue(next);
        next.setPreviousInAccessQueue(previous);
    }

    void nullifyAccessOrder(HashCacheEntry nulled) {
        nulled.setNextInAccessQueue(null);
        nulled.setPreviousInAccessQueue(null);
    }

    final static class HeadHashCacheEntry extends HashCacheEntry {
        HeadHashCacheEntry() {
            super(0,null,null,0,0,0);
            previousAccess = this;
            nextAccess = this;
        }
    }
}
