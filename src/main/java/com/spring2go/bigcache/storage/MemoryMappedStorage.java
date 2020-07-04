package com.spring2go.bigcache.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class MemoryMappedStorage implements IStorage {
    private RandomAccessFile raf;
    private ThreadLocalByteBuffer threadLocalBuffer;

    public MemoryMappedStorage(String dir, int index, int capacity) throws IOException {
        File backFile = new File(dir);
        if (!backFile.exists()) {
            backFile.mkdirs();
        }
        String backFileName = dir + index + "-" + System.currentTimeMillis() + DATA_FILE_SUFFIX;
        raf = new RandomAccessFile(backFileName, "rw");
        MappedByteBuffer mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.PRIVATE, 0, capacity);
        threadLocalBuffer = new ThreadLocalByteBuffer(mappedByteBuffer);
    }

    private ByteBuffer getLocal(int position) {
        ByteBuffer buffer = threadLocalBuffer.get();
        buffer.position(position);
        return buffer;
    }

    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.close();
        }
        //implies system GC
        threadLocalBuffer.set(null);
        threadLocalBuffer = null;
    }

    @Override
    public void get(int position, byte[] dest) throws IOException {
        ByteBuffer buffer = this.getLocal(position);
        buffer.get(dest);
    }

    @Override
    public void put(int position, byte[] source) throws IOException {
        ByteBuffer buffer = this.getLocal(position);
        buffer.put(source);
    }

    @Override
    public void free() {
        MappedByteBuffer buffer = (MappedByteBuffer) threadLocalBuffer.getSourceBuffer();
        buffer.clear();
    }

    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
        private ByteBuffer _src;

        public ThreadLocalByteBuffer(ByteBuffer src) {
            _src = src;
        }

        public ByteBuffer getSourceBuffer() {
            return _src;
        }

        @Override
        protected synchronized ByteBuffer initialValue() {
            ByteBuffer dup = _src.duplicate();
            return dup;
        }
    }
}
