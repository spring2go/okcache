package com.spring2go.okcache;

import com.spring2go.bigcache.BigCache;
import com.spring2go.bigcache.CacheConfig;
import com.spring2go.bigcache.utils.TestUtil;
import com.spring2go.okcache.store.BigCacheStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CachePerfTest {
    public Cache<Integer, String> cache2000() throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(CacheConfig.StorageMode.OffHeapPlusFile)
                .setCapacityPerBlock(16 * 1024 * 1024)
                .setMaxOffHeapMemorySize(16 * 1024 * 1024)
                .setMergeInterval(2 * 1000)
                .setPurgeInterval(2 * 1000);
        BigCache bigcache = new BigCache<Integer>(TestUtil.TEST_BASE_DIR, config);
        BigCacheStore<Integer, String> evictStore = new BigCacheStore<Integer, String>(bigcache);
        Cache<Integer, String> cache = CacheBuilder.newBuilder(Integer.class, String.class)
                .expireAfterAccess(1000*30, TimeUnit.MILLISECONDS)
                .maximumSize(2000)
                .addEvictStore(evictStore)
                .build();

        for (int i = 1; i <= 200; i++) {
            cache.put(i, "V" + i);
        }
        return cache;
    }

    @Test
    public void testPutAndGet() throws IOException, ExecutionException, InterruptedException {
        int count = 300000;
        int threadCount = 8;

        final Cache<Integer, String> cache = cache2000();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Future[] futures = new Future[count];

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int finalI = i;
            futures[i] = executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    cache.put(finalI,"V" + finalI);
                    return finalI;
                }
            });
        }

        for (Future future : futures) {
            future.get();
        }

        long end = System.nanoTime();

        System.out.println("Puts Cost:" + (end - start) + " ns, average:" + ((end - start)/count) + " ns");
        System.out.println(cache.stats());

        Thread.sleep(5000);
        System.out.println(cache.stats());

        start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int finalI = i;
            futures[i] = executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return cache.get(finalI);
                }
            });
        }

        String[] res = new String[count];
        for (int i = 0; i < count; i++) {
            res[i] = (String) futures[i].get();
        }

        end = System.nanoTime();

        System.out.println("Gets Cost:" + (end - start) + " ns, average:" + ((end - start)/count) + " ns");
        System.out.println(cache.stats());

        Thread.sleep(5000);
        System.out.println(cache.stats());

        for (int i = 0; i < count; i++) {
            Assert.assertEquals(res[i],"V" + i);
        }


    }
}
