package com.spring2go.bigcache.storage;

/**
 * Created on Jul, 2020 by @author bobo
 *
 * The Class Pointer is a pointer to the stored cache data, which keeps
 * position and length of the payload and associated StorageBlock.
 */
public class Pointer {
    /** The position. */
    protected int position;

    /** The length of the value. */
    protected int length;

    /** The associated storage block. */
    protected StorageBlock storageBlock;

    /**
     * Instantiates a new pointer.
     *
     * @param position the position
     * @param length the length of the value
     * @param storageBlock the persistent cache storage
     */
    public Pointer(int position, int length, StorageBlock storageBlock) {
        this.position = position;
        this.length = length;
        this.storageBlock = storageBlock;
    }

    /**
     * Gets the position.
     *
     * @return the position
     */
    public int getPosition() {
        return position;
    }

    /**
     * Sets the position.
     *
     * @param position the new position
     */
    public void setPosition(int position) {
        this.position = position;
    }

    /**
     * Gets the storage block.
     *
     * @return the storage block.
     */
    public StorageBlock getStorageBlock() {
        return storageBlock;
    }

    /**
     * Sets the storage block.
     *
     * @param storageBlock the new storage block.
     */
    public void setStorageBlock(StorageBlock storageBlock) {
        this.storageBlock = storageBlock;
    }

    /**
     * Gets the length of the value
     *
     * @return the length of the stored value
     */
    public int getLength() {
        return length;
    }

    /**
     * Sets the length of the stored value.
     *
     * @param length the length of the stored value.
     */
    public void setLength(int length) {
        this.length = length;
    }


    /**
     * Copies given pointer.
     *
     * @param pointer the pointer
     * @return the pointer
     */
    public Pointer copy(Pointer pointer) {
        this.position = pointer.position;
        this.length = pointer.length;
        this.storageBlock = pointer.storageBlock;
        return this;
    }
}
