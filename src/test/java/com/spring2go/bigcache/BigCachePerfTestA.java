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
public class BigCachePerfTestA {
    private static final int THREAD_COUNT = 128;
    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "performance/bigcache/";

    private static BigCache<String> cache;

    @Parameter(value = 0)
    public StorageMode storageMode;

    @Parameters
    public static Collection<StorageMode[]> data() throws IOException {
        StorageMode[][] data = { { StorageMode.PureFile },
                { StorageMode.MemoryMappedPlusFile },
                { StorageMode.OffHeapPlusFile } };
        return Arrays.asList(data);
    }

    private BigCache<String> cache() throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode)
                .setCapacityPerBlock(20 * 1024 * 1024)
                .setMergeInterval(2 * 1000)
                .setPurgeInterval(2 * 1000);
        BigCache<String> cache = new BigCache<String>(TEST_DIR, config);
        return cache;
    }

    @Test
    public void testSingleThreadReadWrite() throws IOException, ClassNotFoundException {
        final int count = 400 * 1000;
        final TestSample sample = new TestSample();

        cache = cache();

        long start = System.nanoTime();

        StringBuilder user = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sample.intA = i;
            sample.doubleA = i;
            sample.longA = i;
            cache.put(TestSample.users(user, i), sample.toBytes());
        }
        for (int i = 0; i < count; i++) {
            byte[] result = cache.get(TestSample.users(user, i));
            assertNotNull(result);
        }
        for (int i = 0; i < count; i++) {
            byte[] result = cache.get(TestSample.users(user, i));
            TestSample re = TestSample.fromBytes(result);
            assertEquals(i, re.intA);
            assertEquals(i, re.doubleA, 0.0);
            assertEquals(i, re.longA);
        }
        for (int i = 0; i < count; i++) {
            cache.delete(TestSample.users(user, i));
        }
        assertEquals(cache.count(), 0);
        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second%n",
                (int) (count * 4 * 1e6 / duration));
    }

    @Test
    public void testMultiThreadReadWrite() throws InterruptedException, ExecutionException, IOException {
        final int count = 2 * 1000 * 1000;
        ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        cache = cache();

        long start = System.nanoTime();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int finalI = i;
            futures.add(service.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        final TestSample sample = new TestSample();
                        StringBuilder user = new StringBuilder();
                        for (int j = finalI; j < count; j += THREAD_COUNT) {
                            sample.intA = j;
                            sample.doubleA = j;
                            sample.longA = j;
                            cache.put(TestSample.users(user, j), sample.toBytes());
                        }
                        for (int j = finalI; j < count; j += THREAD_COUNT) {
                            byte[] result = cache.get(TestSample.users(user, j));
                            assertNotNull(result);
                        }
                        for (int j = finalI; j < count; j += THREAD_COUNT) {
                            byte[] result = cache.get(TestSample.users(user, j));
                            TestSample re = TestSample.fromBytes(result);
                            assertEquals(j, re.intA);
                            assertEquals(j, re.doubleA, 0.0);
                            assertEquals(j, re.longA);
                        }
                        for (int j = finalI; j < count; j += THREAD_COUNT) {
                            cache.delete(TestSample.users(user, j));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e1) {
                        e1.printStackTrace();
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
                    Thread.sleep(3000);
                } catch (InterruptedException e2) {
                }
                FileUtil.deleteDirectory(new File(TEST_DIR));
            }
        }
    }
}
