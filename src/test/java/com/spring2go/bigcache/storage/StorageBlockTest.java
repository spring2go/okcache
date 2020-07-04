package com.spring2go.bigcache.storage;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.spring2go.bigcache.CacheConfig.StorageMode;
import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestUtil;

/**
 * Created on Jul, 2020 by @author bobo
 */
@RunWith(Parameterized.class)
public class StorageBlockTest {
    private static String testDir = TestUtil.TEST_BASE_DIR + "unit/storage_block_test/";

    private IStorageBlock block = null;

    @Parameterized.Parameter(value = 0)
    public StorageMode storageMode;

    @Parameterized.Parameters
    public static Collection<StorageMode[]> data() throws IOException {
        StorageMode[][] data = { { StorageMode.PureFile },
                { StorageMode.MemoryMappedPlusFile },
                { StorageMode.OffHeapPlusFile } };
        return Arrays.asList(data);
    }

    @Test
    public void testBasic() throws IOException {
        block = new StorageBlock(testDir, 1, StorageManager.DEFAULT_CAPACITY_PER_BLOCK, storageMode);

        String testString = "Test String";
        byte[] testBytes = testString.getBytes();

        assertTrue(StorageManager.DEFAULT_CAPACITY_PER_BLOCK == block.getCapacity());
        assertTrue(0L == block.getDirty());
        assertTrue(block.getDirtyRatio() <= 1e-6);
        assertTrue(1 == block.getIndex());

        // store
        Pointer pointer = block.store(testBytes);
        assertTrue(0L == block.getDirty());
        assertTrue(block.getDirtyRatio() <= 1e-6);
        assertTrue(0 == pointer.getPosition());
        assertTrue(testBytes.length == pointer.getLength());
        assertTrue(testBytes.length == block.getUsed());

        // retrieve
        byte[] resultBytes = block.retrieve(pointer);
        assertEquals(testString, new String(resultBytes));
        assertTrue(0L == block.getDirty());
        assertTrue(block.getDirtyRatio() <= 1e-6);
        assertTrue(0 == pointer.getPosition());
        assertTrue(testBytes.length == pointer.getLength());
        assertTrue(testBytes.length == block.getUsed());

        // update to small
        String smallTestString = "Test Str";
        byte[] smallTestBytes = smallTestString.getBytes();
        pointer = block.update(pointer, smallTestBytes);
        assertTrue((testBytes.length - smallTestBytes.length) == block.getDirty());
        double expectedRatio = (testBytes.length - smallTestBytes.length) * 1.0 / StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
        assertTrue(Math.abs(expectedRatio - block.getDirtyRatio()) <= 1e-6);
        assertTrue(0 == pointer.getPosition());
        assertTrue(smallTestBytes.length == pointer.getLength());
        assertTrue(smallTestBytes.length == block.getUsed());

        // update to bigger
        pointer = block.update(pointer, testBytes);
        assertTrue(testBytes.length== block.getDirty());
        expectedRatio = testBytes.length * 1.0 / StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
        assertTrue(Math.abs(expectedRatio - block.getDirtyRatio()) <= 1e-6);
        assertTrue(testBytes.length == pointer.getPosition());
        assertTrue(testBytes.length == pointer.getLength());
        assertTrue(testBytes.length == block.getUsed());

        // remove
        resultBytes = block.remove(pointer);
        assertEquals(testString, new String(resultBytes));
        expectedRatio = testBytes.length * 2 * 1.0 / StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
        assertTrue(Math.abs(expectedRatio - block.getDirtyRatio()) <= 1e-6);
        assertTrue(0L == block.getUsed());

        // free
        block.free();
        assertTrue(StorageManager.DEFAULT_CAPACITY_PER_BLOCK == block.getCapacity());
        assertTrue(0L == block.getDirty());
        assertTrue(block.getDirtyRatio() <= 1e-6);
        assertTrue(1 == block.getIndex());
    }

    @Test
    public void testlimitNunberOfItems() throws IOException {
        block = new StorageBlock(testDir, 2, StorageManager.DEFAULT_CAPACITY_PER_BLOCK, storageMode);

        int limit = 1000;

        String testString = "Test String";
        byte[] testBytes = testString.getBytes();

        assertTrue(StorageManager.DEFAULT_CAPACITY_PER_BLOCK == block.getCapacity());
        assertTrue(0L == block.getDirty());
        assertTrue(block.getDirtyRatio() <= 1e-6);
        assertTrue(2 == block.getIndex());

        // store
        Pointer[] pointers = new Pointer[limit];
        for(int i = 0; i < limit; i++) {
            Pointer pointer = block.store(testBytes);
            pointers[i] = pointer;
            assertTrue(0L == block.getDirty());
            assertTrue(block.getDirtyRatio() <= 1e-6);
            assertTrue(i * (testBytes.length) == pointer.getPosition());
            assertTrue(testBytes.length == pointer.getLength());
            assertTrue(testBytes.length * (i + 1) == block.getUsed());
        }

        // retrieve
        for(int i = 0; i < limit; i++) {
            byte[] resultBytes = block.retrieve(pointers[i]);
            assertEquals(testString, new String(resultBytes));
            assertTrue(0L == block.getDirty());
            assertTrue(block.getDirtyRatio() <= 1e-6);
            assertTrue(i * (testBytes.length) == pointers[i].getPosition());
            assertTrue(testBytes.length == pointers[i].getLength());
            assertTrue(testBytes.length * limit == block.getUsed());
        }

        // update to small
        String smallTestString = "Test Str";
        byte[] smallTestBytes = smallTestString.getBytes();
        for(int i = 0; i < limit; i++) {
            pointers[i] = block.update(pointers[i], smallTestBytes);
            assertTrue((i + 1) * (testBytes.length - smallTestBytes.length) == block.getDirty());
            double expectedRatio = (i + 1) * (testBytes.length - smallTestBytes.length) * 1.0 / StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
            assertTrue(Math.abs(expectedRatio - block.getDirtyRatio()) <= 1e-6);
            assertTrue(i * (testBytes.length) == pointers[i].getPosition());
            assertTrue(smallTestBytes.length == pointers[i].getLength());
            assertTrue(testBytes.length * limit - (testBytes.length - smallTestBytes.length) * (i + 1) == block.getUsed());
        }

        // update to bigger
        for(int i = 0; i < limit; i++) {
            pointers[i] = block.update(pointers[i], testBytes);
            assertTrue(((testBytes.length - smallTestBytes.length) * (limit - i - 1))  + (i + 1) * testBytes.length == block.getDirty());
            double expectedRatio = (((testBytes.length - smallTestBytes.length) * (limit - i - 1))  + (i + 1) * testBytes.length )* 1.0 / StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
            assertTrue(Math.abs(expectedRatio - block.getDirtyRatio()) <= 1e-6);
            assertTrue((limit + i) * testBytes.length == pointers[i].getPosition());
            assertTrue(testBytes.length == pointers[i].getLength());
        }
        assertTrue(testBytes.length * limit == block.getUsed());

        // remove
        for(int i = 0; i < limit; i++) {
            byte[] resultBytes = block.remove(pointers[i]);
            assertEquals(testString, new String(resultBytes));
            double expectedRatio = (testBytes.length * limit + testBytes.length * (i + 1)) * 1.0 / StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
            assertTrue(Math.abs(expectedRatio - block.getDirtyRatio()) <= 1e-6);
        }
        assertTrue(0L == block.getUsed());

        // free
        block.free();
        assertTrue(StorageManager.DEFAULT_CAPACITY_PER_BLOCK == block.getCapacity());
        assertTrue(0L == block.getDirty());
        assertTrue(block.getDirtyRatio() <= 1e-6);
        assertTrue(2 == block.getIndex());
    }

    @Test
    public void testStoreOverflow() throws IOException {
        block = new StorageBlock(testDir, 3, 1024 * 1024 + 1023, storageMode); // 1M + 1023

        byte[] sourceBytes = new byte[1024];
        // populate
        for(int i = 0; i < 1024; i++) {
            Pointer pointer = block.store(sourceBytes);
            assertNotNull(pointer);
            assertTrue(i * 1024 == pointer.getPosition());
            assertTrue(1024 == pointer.getLength());
        }
        assertTrue(1024 * 1024 == block.getUsed());

        Pointer pointer = block.store(sourceBytes);
        assertNull(pointer); // overflow
    }

    @Test
    public void testUpdateOverflow() throws IOException {
        block = new StorageBlock(testDir, 4, 1024 * 1024, storageMode); // 1M

        byte[] sourceBytes = new byte[1024];
        Pointer pointer = null;
        // populate
        for(int i = 0; i < 1024; i++) {
            pointer = block.store(sourceBytes);
            assertNotNull(pointer);
            assertTrue(i * 1024 == pointer.getPosition());
            assertTrue(1024 == pointer.getLength());
        }
        assertTrue(1024 * 1024 == block.getUsed());

        pointer = block.update(pointer, new byte[512]);
        assertTrue(1023 * 1024 == pointer.getPosition());
        assertTrue(512 == pointer.getLength());
        assertTrue(1024 * 1024 - 512 == block.getUsed());

        pointer = block.update(pointer, new byte[512]);
        assertTrue(1023 * 1024 == pointer.getPosition());
        assertTrue(512 == pointer.getLength());
        assertTrue(1024 * 1024 - 512 == block.getUsed());

        pointer = block.update(pointer, new byte[513]);
        assertNull(pointer); // overflow
    }

    @After
    public void clear() throws IOException {
        if (this.block != null) {
            this.block.close();
        }

        try {
            FileUtil.deleteDirectory(new File(testDir));
        } catch (IllegalStateException e) {
            System.gc();
            try {
                FileUtil.deleteDirectory(new File(testDir));
            } catch (IllegalStateException e1) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e2) {
                }
                FileUtil.deleteDirectory(new File(testDir));
            }
        }
    }
}
