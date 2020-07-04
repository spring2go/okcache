package com.spring2go.bigcache.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class FileChannelStorage implements IStorage {
    private FileChannel fileChannel;
    private RandomAccessFile raf;

    public FileChannelStorage(String dir, int index, int capacity) throws IOException {
        File dirFile = new File(dir);
        if (!dirFile.exists()) { dirFile.mkdirs(); }
        String fullFileName = dir + index + "-" + System.currentTimeMillis() + DATA_FILE_SUFFIX;
        raf = new RandomAccessFile(fullFileName, "rw");
        raf.setLength(capacity);
        fileChannel = raf.getChannel();
    }

    @Override
    public void get(int position, byte[] dest) throws IOException {
        fileChannel.read(ByteBuffer.wrap(dest), position);
    }

    @Override
    public void put(int position, byte[] source) throws IOException {
        fileChannel.write(ByteBuffer.wrap(source), position);
    }

    @Override
    public void free() {
        // nothing to do here
    }

    @Override
    public void close() throws IOException {
        if (this.fileChannel != null) {
            this.fileChannel.close();
        }
        if (this.raf != null) {
            this.raf.close();
        }

    }
}
