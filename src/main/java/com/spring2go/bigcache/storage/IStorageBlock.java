package com.spring2go.bigcache.storage;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created on Jul, 2020 by @author bobo
 *
 * The Interface IStorageBlock.
 *
 * A storage unit with fixed capacity.
 */
public interface IStorageBlock extends Comparable<IStorageBlock>, Closeable {
    /**
     * Retrieves the payload associated with the pointer and always update the access time.
     *
     * @param pointer the pointer
     * @return the byte[]
     * @throws IOException
     */
    byte[] retrieve(Pointer pointer) throws IOException;

    /**
     * Removes the payload and marks the used space as dirty.
     *
     * @param pointer the pointer
     * @return the byte[]
     * @throws IOException
     */
    byte[] remove(Pointer pointer) throws IOException;

    /**
     * Removes the payload without returning the payload
     *
     * @param pointer the pointer
     * @throws IOException
     */
    void removeLight(Pointer pointer) throws IOException;

    /**
     * Stores the payload.
     *
     * @param payload the payload
     * @return the pointer
     * @throws IOException
     */
    Pointer store(byte[] payload) throws IOException;

    /**
     * Updates the payload by marking exSpace as dirty.
     *
     * @param pointer the pointer
     * @param payload the payload
     * @return the pointer
     * @throws IOException
     */
    Pointer update(Pointer pointer, byte[] payload) throws IOException;

    /**
     * Calculates and returns total size of the dirty space.
     *
     * @return the total size of the dirty space.
     */
    long getDirty();

    /**
     * Calculates and returns total size of the used space.
     *
     * @return the total size of the used space.
     */
    long getUsed();

    /**
     * Calculates and returns total capacity of the block.
     *
     * @return the total capacity of the block.
     */
    long getCapacity();

    /**
     * Calculates and returns the dirty to capacity ratio
     *
     * @return dirty ratio
     */
    double getDirtyRatio();

    /**
     * Get the index of this storage block
     *
     * @return an index
     */
    int getIndex();

    /**
     * Frees the storage.
     */
    void free();
}
