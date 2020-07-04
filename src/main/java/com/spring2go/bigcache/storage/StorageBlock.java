package com.spring2go.bigcache.storage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.spring2go.bigcache.CacheConfig.StorageMode;

/**
 * Created on Jul, 2020 by @author bobo
 *
 * The Class StorageBlock.
 */
public class StorageBlock implements IStorageBlock {
    /** The index. */
    private final int index;

    /** The capacity. */
    private final int capacity;

    /** The underlying storage. */
    private IStorage underlyingStorage;

    /** The offset within the storage block. */
    private final AtomicInteger currentOffset = new AtomicInteger(0);

    /** The dirty storage. */
    private final AtomicInteger dirtyStorage = new AtomicInteger(0);

    /** The used storage. */
    private final AtomicInteger usedStorage = new AtomicInteger(0);

    /**
     * Instantiates a new storage block.
     *
     * @param dir the directory
     * @param index the index
     * @param capacity the capacity
     * @throws IOException exception throws when failing to create the storage block
     */
    public StorageBlock(String dir, int index, int capacity, StorageMode storageMode) throws IOException{
        this.index = index;
        this.capacity = capacity;
        switch (storageMode) {
            case PureFile:
                underlyingStorage = new FileChannelStorage(dir, index, capacity);
                break;
            case MemoryMappedPlusFile:
                underlyingStorage = new MemoryMappedStorage(dir, index, capacity);
                break;
            case OffHeapPlusFile:
                underlyingStorage = new OffHeapStorage(capacity);
                break;
        }
    }

    @Override
    public byte[] retrieve(Pointer pointer) throws IOException {
        byte [] payload = new byte[pointer.getLength()];
        underlyingStorage.get(pointer.getPosition(), payload);
        return payload;
    }

    @Override
    public byte[] remove(Pointer pointer) throws IOException {
        byte [] payload = retrieve(pointer);
        dirtyStorage.addAndGet(pointer.getLength());
        usedStorage.addAndGet(-1 * pointer.getLength());
        return payload;
    }


    @Override
    public void removeLight(Pointer pointer) throws IOException {
        dirtyStorage.addAndGet(pointer.getLength());
        usedStorage.addAndGet(-1 * pointer.getLength());
    }

    @Override
    public Pointer store(byte[] payload) throws IOException {
        Allocation allocation = allocate(payload);
        if (allocation == null) return null; // not enough storage available
        Pointer pointer = store(allocation, payload);
        return pointer;
    }

    /**
     * Allocates storage for the payload, return null if not enough storage available.
     *
     * @param payload the payload
     * @return the allocation
     */
    protected Allocation allocate(byte[] payload) {
        int payloadLength = payload.length;
        int allocationOffset = currentOffset.addAndGet(payloadLength);
        if(this.capacity < allocationOffset){
            return null;
        }
        Allocation allocation = new Allocation(allocationOffset - payloadLength, payloadLength);
        return allocation;
    }


    /**
     * Stores the payload by the help of allocation.
     *
     * @param allocation the allocation
     * @param payload the payload
     * @return the pointer
     * @throws IOException
     */
    public Pointer store(Allocation allocation, byte[] payload) throws IOException {
        Pointer pointer = new Pointer(allocation.getOffset(), allocation.getLength(), this);
        underlyingStorage.put(allocation.getOffset(), payload);
        usedStorage.addAndGet(payload.length);
        return pointer;
    }

    @Override
    public Pointer update(Pointer pointer, byte[] payload) throws IOException {
        if (pointer.getLength() >= payload.length) { // has enough space to reuse
            dirtyStorage.addAndGet(pointer.getLength() - payload.length);
            usedStorage.addAndGet(-1 * pointer.getLength());
            Allocation allocation = new Allocation(pointer.getPosition(), payload.length);
            return store(allocation, payload); // should always return a new pointer
        } else { // make a move
            dirtyStorage.addAndGet(pointer.getLength());
            usedStorage.addAndGet(-1 * pointer.getLength());
            return store(payload); // may return null because not enough space available
        }
    }

    @Override
    public long getDirty() {
        return this.dirtyStorage.get();
    }

    @Override
    public double getDirtyRatio() {
        return (this.getDirty() * 1.0) / this.getCapacity();
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    @Override
    public long getUsed() {
        return this.usedStorage.get();
    }

    @Override
    public void free() {

        currentOffset.set(0);
        dirtyStorage.set(0);
        usedStorage.set(0);

        underlyingStorage.free();
    }

    /**
     * The Class Allocation.
     */
    private static class Allocation {

        /** The offset. */
        private int offset;

        /** The length. */
        private int length;

        /**
         * Instantiates a new allocation.
         *
         * @param offset the offset
         * @param length the length
         */
        public Allocation(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }

        /**
         * Gets the offset.
         *
         * @return the offset
         */
        public int getOffset() {
            return offset;
        }

        /**
         * Gets the length.
         *
         * @return the length
         */
        public int getLength() {
            return length;
        }
    }

    /**
     * Gets the index.
     *
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    @Override
    public void close() throws IOException {
        if (this.underlyingStorage != null) {
            this.underlyingStorage.close();
        }
    }

    @Override
    public int compareTo(IStorageBlock o) {
        if (this.getIndex() < o.getIndex()) return -1;
        else if (this.getIndex() == o.getIndex()) return 0;
        else return 1;
    }

}
