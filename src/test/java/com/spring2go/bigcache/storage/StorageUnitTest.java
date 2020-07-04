package com.spring2go.bigcache.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestUtil;

/**
 * Created on Jul, 2020 by @author bobo
 */
@RunWith(Parameterized.class)
public class StorageUnitTest {
    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "unit/istorage_test/";
    private static IStorage[][] storageData;

    @Parameter(value = 0)
    public IStorage storage;

    @Parameters
    public static Collection<IStorage[]> data() throws IOException {
        storageData = new IStorage[][] { { new FileChannelStorage(TEST_DIR, 0, 16 * 1024 * 1024) },
                { new MemoryMappedStorage(TEST_DIR, 0, 16 * 1024 * 1024) },
                { new OffHeapStorage(16 * 1024 * 1024) } };
        return Arrays.asList(storageData);
    }

    public void storage6() throws IOException {
        storage.put(0, "A".getBytes());
        storage.put(1, "B".getBytes());
        storage.put(2, "C".getBytes());
        storage.put(3, "D".getBytes());
        storage.put(4, "E".getBytes());
        storage.put(5, "F".getBytes());
    }

    @Test
    public void testGet() throws IOException {
        storage6();
        byte[] dest = new byte["A".getBytes().length];
        storage.get(0, dest);
        assertEquals(new String(dest), "A");
        storage.get(1, dest);
        assertEquals(new String(dest), "B");
        storage.get(2, dest);
        assertEquals(new String(dest), "C");
        storage.get(3, dest);
        assertEquals(new String(dest), "D");
        storage.get(4, dest);
        assertEquals(new String(dest), "E");
        storage.get(5, dest);
        assertEquals(new String(dest), "F");
    }

    @Test
    public void testPut() throws IOException {
        storage6();
        //test put new
        storage.put(6, "G".getBytes());
        byte[] dest = new byte["A".getBytes().length];
        storage.get(6, dest);
        assertEquals(new String(dest), "G");
        //test replace old
        storage.put(0, "W".getBytes());
        storage.get(0, dest);
        assertEquals(new String(dest), "W");
    }

    @After
    public void clear() throws IOException {
        storage.free();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        for (IStorage[] storage : storageData) {
            storage[0].close();
        }
        try {
            FileUtil.deleteDirectory(new File(TEST_DIR));
        } catch (IllegalStateException e) {
            System.gc();
            try {
                FileUtil.deleteDirectory(new File(TEST_DIR));
            } catch (IllegalStateException e1) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e2) {
                }
                FileUtil.deleteDirectory(new File(TEST_DIR));
            }
        }
    }
}
