package com.spring2go.bigcache.storage;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Created on Jul, 2020 by @author bobo
 *
 * This class is called when buffers should be allocated aligned.
 *
 * It creates a block of memory aligned to memory page size, and limits the amount of buffer each processor can access.
 *
 */
public class AlignOffHeapStorage extends OffHeapStorage {
    public AlignOffHeapStorage(int capacity) {
        super(capacity, ByteBuffer.allocateDirect(capacity));
    }

    @Override
    public void close() throws IOException {
        if (!disposed.compareAndSet(false, true))
            return;
        if (byteBuffer == null)
            return;
        try {
            Field cleanerField = byteBuffer.getClass().getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            sun.misc.Cleaner cleaner = (sun.misc.Cleaner) cleanerField.get(byteBuffer);
            cleaner.clean();
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
