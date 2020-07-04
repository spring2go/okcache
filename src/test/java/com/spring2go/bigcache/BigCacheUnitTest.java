package com.spring2go.bigcache;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.spring2go.bigcache.CacheConfig.StorageMode;
import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestUtil;

/**
 * Created on Jul, 2020 by @author bobo
 */
@RunWith(Parameterized.class)
public class BigCacheUnitTest {
    private static String TEST_DIR = TestUtil.TEST_BASE_DIR + "unit/bigcache/";
    private static BigCache<Integer> cache;

    @Parameter(value = 0)
    public StorageMode storageMode;

    @Parameters
    public static Collection<StorageMode[]> data() throws IOException {
        StorageMode[][] data = { { StorageMode.PureFile },
                { StorageMode.MemoryMappedPlusFile },
                { StorageMode.OffHeapPlusFile } };
        return Arrays.asList(data);
    }

    public BigCache<Integer> cache6() throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode);
        BigCache<Integer> cache = new BigCache<Integer>(TEST_DIR, config);
        cache.put(0, "A".getBytes());
        cache.put(1, "B".getBytes());
        cache.put(2, "C".getBytes());
        cache.put(3, "D".getBytes());
        cache.put(4, "E".getBytes());
        cache.put(5, "F".getBytes());
        return cache;
    }

    @Test
    public void testGet() throws IOException {
        cache = cache6();
        assertEquals(new String(cache.get(0)), "A");
        assertEquals(new String(cache.get(1)), "B");
        assertEquals(new String(cache.get(2)), "C");
        assertEquals(new String(cache.get(3)), "D");
        assertEquals(new String(cache.get(4)), "E");
        assertEquals(new String(cache.get(5)), "F");
    }

    @Test
    public void testPut() throws IOException {
        cache = cache6();
        //test put new
        cache.put(6, "G".getBytes());
        assertEquals(new String(cache.get(6)), "G");
        //test replace old
        cache.put(0, "W".getBytes());
        assertEquals(new String(cache.get(0)), "W");
    }

    @Test
    public void testDelete() throws IOException {
        cache = cache6();
        byte[] old = cache.delete(0);
        assertEquals(new String(old), "A");
        assertNull(cache.get(0));

        old = cache.delete(6);
        assertNull(old);
        assertNull(cache.get(6));
    }

    @Test
    public void testContain() throws IOException {
        cache = cache6();
        assertTrue(cache.contains(0));
        assertFalse(cache.contains(6));
    }

    @After
    public void close() throws IOException {
        try {
            cache.close();
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
