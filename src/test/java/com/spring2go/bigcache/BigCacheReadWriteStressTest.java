package com.spring2go.bigcache;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.spring2go.bigcache.CacheConfig.StorageMode;
import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestSample;
import com.spring2go.bigcache.utils.TestUtil;

/**
 * Created on Jul, 2020 by @author bobo
 */
@RunWith(Parameterized.class)
public class BigCacheReadWriteStressTest {
    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/bigcache/";

    private static BigCache<String> cache;

    @Parameter(value = 0)
    public StorageMode storageMode;

    @Parameters
    public static Collection<StorageMode[]> data() throws IOException {
        StorageMode[][] data = { //{ StorageMode.PureFile },
                { StorageMode.MemoryMappedPlusFile },
                { StorageMode.OffHeapPlusFile } };
        return Arrays.asList(data);
    }

    public BigCache<String> cache(long count) throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode);
        BigCache<String> cache = new BigCache<String>(TEST_DIR, config);

        for (long i = 0; i < count; i++) {
            String key = "" + i;
            cache.put(key, key.getBytes());
        }
        return cache;
    }

    @Test
    public void testWrite_ten_million() throws IOException {
        final long item_count = 10 * 1000 * 1000;
        long elapsedTime = 0;
        long count = 0;
        cache = cache(0);
        long startTime = System.nanoTime();

        for (long i = 0; i < item_count; i++) {
            String key = "" + i;
            long start = System.nanoTime();
            cache.put(key, key.getBytes());

            if (i % (100 * 1000) == 0) {
                elapsedTime += (System.nanoTime() - start);
                count++;
            }
        }

        System.out.println("avg:" + elapsedTime / count + " ns");
        System.out.println("write " + item_count / (1000 * 1000) + " million times:"
                + (System.nanoTime() - startTime) / (1000 * 1000) + " ms");
    }

    @Test
    public void testRead_ten_million() throws IOException {
        final long item_count = 10 * 1000 * 1000;
        cache = cache(item_count);
        long startTime = System.nanoTime();

        for (long i = 0; i < item_count; i++) {
            String key = "" + i;
            cache.get(key);
        }
        System.out.println("read " + item_count / (1000 * 1000) + " million times:" + (System.nanoTime() - startTime)
                / (1000 * 1000)
                + " ms");
    }

    @Test
    public void testReadWrite_one_million() throws IOException {
        final long item_count = 1000 * 1000;
        final int keyLen = 8;
        final int valueLen = 128;
        this.executeReadWrite(item_count, keyLen, valueLen);
    }

    @Test
    public void testReadWrite_two_million() throws IOException {
        final long item_count = 2 * 1000 * 1000;
        final int keyLen = 8;
        final int valueLen = 1024;
        this.executeReadWrite(item_count, keyLen, valueLen);
    }

    private void executeReadWrite(final long count, int keyLen, int valueLen) throws IOException {
        final int defaultKeyLen = 8;
        final int defaultValueLen = 32;

        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode);
        cache = new BigCache<String>(TEST_DIR, config);
        List<String> keys = new ArrayList<String>();

        String key = "";
        String value = "";
        if (keyLen > 0) {
            key = TestUtil.randomString(keyLen);
        } else {
            key = TestUtil.randomString(defaultKeyLen);
        }
        if (valueLen > 0) {
            value = TestUtil.randomString(valueLen);
        } else {
            value = TestUtil.randomString(defaultValueLen);
        }
        for (int i = 0; i < count; i++) {
            cache.put(key + i, TestUtil.getBytes(value + i));
            keys.add(key + i);
        }
        assertEquals(cache.count(), count);
        for (String k : keys) {
            String v = new String(cache.get(k));
            String index = k.substring(keyLen > 0 ? keyLen : 8);
            assertEquals(value + index, v);
        }
        for (String k : keys) {
            cache.delete(k);
        }
        for (String k : keys) {
            assertNull(cache.get(k));
        }
    }

    @Test
    public void testMultiThreadWriteTtl_two_million() throws InterruptedException, ExecutionException, IOException {
        final int count = 2 * 1000 * 1000;
        final int threadCount = 16;
        ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode)
                .setCapacityPerBlock(20 * 1024 * 1024)
                .setMergeInterval(2 * 1000)
                .setPurgeInterval(2 * 1000);
        cache = new BigCache<String>(TEST_DIR, config);

        long start = System.nanoTime();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int i = 0; i < threadCount; i++) {
            final int finalI = i;
            futures.add(service.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        final TestSample sample = new TestSample();
                        StringBuilder user = new StringBuilder();
                        for (int j = finalI; j < count; j += threadCount) {
                            sample.intA = j;
                            sample.doubleA = j;
                            sample.longA = j;
                            cache.put(TestSample.users(user, j), sample.toBytes(), 2 * 1000);
                        }
                        Thread.sleep(10 * 1000);
                        for (int j = finalI; j < count; j += threadCount) {
                            assertNull(cache.get(TestSample.users(user, j)));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second%n",
                (int) (count * 4 * 1e6 / duration));
        service.shutdown();
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
                    Thread.sleep(10000);
                } catch (InterruptedException e2) {
                }
                FileUtil.deleteDirectory(new File(TEST_DIR));
            }
        }
    }
}
