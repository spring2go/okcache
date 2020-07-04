package com.spring2go.bigcache.storage;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class OffHeapStorage implements IStorage {
    protected final AtomicBoolean disposed = new AtomicBoolean(false);
    protected ByteBuffer byteBuffer;

    private static final Unsafe UNSAFE = getUnsafe();
    private static final long BYTE_ARRAY_OFFSET = (long) UNSAFE.arrayBaseOffset(byte[].class);

    private final long address;

    private static Unsafe getUnsafe() {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public OffHeapStorage(int capacity) {
        this.address = UNSAFE.allocateMemory(capacity);
    }

    public OffHeapStorage(int capacity, ByteBuffer buffer) {
        this.byteBuffer = ByteBuffer.allocateDirect(capacity);
        try {
            Method method = byteBuffer.getClass().getDeclaredMethod("address");
            method.setAccessible(true);
            this.address = (Long) method.invoke(byteBuffer);
        } catch (Exception e) {
            throw new RuntimeException("Unable to allocate offheap memory using sun.misc.Unsafe on your platform", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (!disposed.compareAndSet(false, true))
            return;
        UNSAFE.freeMemory(address);
    }

    @Override
    public void get(int position, byte[] dest) throws IOException {
        assert !disposed.get() : "disposed";
        assert position >= 0 : position;
        this.get(address + position, dest, BYTE_ARRAY_OFFSET, dest.length);
    }

    /**
     * Get bytes from the local buffer to a given byte array.
     *
     * @param baseAddress the absolute base address of the local buffer
     * @param dest the dest byte array
     * @param destOffset the offset of the dest byte array
     * @param length the length of bytes to get
     */
    private void get(long baseAddress, byte[] dest, long destOffset, long length) {
        UNSAFE.copyMemory(null, baseAddress, dest, destOffset, length);
    }

    @Override
    public void put(int position, byte[] source) throws IOException {
        assert !disposed.get() : "disposed";
        assert position >= 0 : position;
        this.put(BYTE_ARRAY_OFFSET, source, address + position, source.length);

    }

    /**
     * Put bytes from a given byte array to the local buffer.
     *
     * @param srcOffset the offset of the source byte array
     * @param source the source byte array
     * @param baseAddress the absolute base address of the local buffer
     * @param length the length of bytes to put
     */
    private void put(long srcOffset, byte[] source, long baseAddress, long length) {
        UNSAFE.copyMemory(source, srcOffset, null, baseAddress, length);
    }

    @Override
    public void free() {
        //do nothing
    }
}
