package com.spring2go.bigcache.storage;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created on Jul, 2020 by @author bobo
 *
 * The Interface IStorage for get/put cached data in bytes.
 */
public interface IStorage extends Closeable {
    public static final String DATA_FILE_SUFFIX = ".data";

    /**
     * Gets bytes from the specified location.
     *
     * @param position the position
     * @param dest the destination
     */
    void get(int position, byte[] dest) throws IOException;

    /**
     * Puts source to the specified location of the Storage.
     *
     * @param position the position
     * @param source the source
     */
    void put(int position, byte[] source) throws IOException;

    /**
     * Frees the storage.
     */
    void free();
}
