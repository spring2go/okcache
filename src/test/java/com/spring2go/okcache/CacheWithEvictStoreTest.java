package com.spring2go.okcache;

import com.spring2go.bigcache.BigCache;
import com.spring2go.bigcache.CacheConfig;
import com.spring2go.bigcache.utils.TestUtil;
import com.spring2go.okcache.store.BigCacheStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheWithEvictStoreTest {
    private Cache<Integer, String> cache;
    private BigCache<Integer> bigcache;

    @Before
    public void setUp() {
        try {
            CacheConfig config = new CacheConfig();
            config.setStorageMode(CacheConfig.StorageMode.OffHeapPlusFile)
                    .setCapacityPerBlock(16 * 1024 * 1024)
                    .setMaxOffHeapMemorySize(16 * 1024 * 1024)
                    .setMergeInterval(2 * 1000)
                    .setPurgeInterval(2 * 1000);
            bigcache = new BigCache<Integer>(TestUtil.TEST_BASE_DIR, config);

            BigCacheStore<Integer, String> evictStore = new BigCacheStore<Integer, String>(bigcache);
            cache = CacheBuilder.newBuilder(Integer.class, String.class)
                    .expireAfterAccess(1000*30, TimeUnit.MILLISECONDS)
                    .maximumSize(100)
                    .addEvictStore(evictStore)
                    .build();

            for (int i = 1; i <= 2000000; i++) {
                cache.put(i, "V" + i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws IOException {
        if (bigcache != null) {
            bigcache.close();
        }
    }

    @Test
    public void testGet() throws IOException, InterruptedException {
        Thread.sleep(5000);
        Assert.assertEquals(cache.get(1), "V1");
        Assert.assertEquals(cache.get(200), "V200");
        System.out.println(cache.stats());
    }
    @Test
    public void testGet2() throws IOException, InterruptedException {
        Thread.sleep(5000);
        long start = System.nanoTime();
        for (int i = 1; i <= 130; i++) {
            Assert.assertEquals(cache.get(i), "V"+i);
        }
        System.out.println("AverageCost:" + (System.nanoTime() -start)/130);
        System.out.println(cache.stats());
        Thread.sleep(5000);
        System.out.println(cache.stats());
    }

    @Test
    public void testPut() throws IOException, InterruptedException {
        Thread.sleep(5000);
        cache.put(1,"V1M");
        Assert.assertEquals(cache.get(1), "V1M");
        Assert.assertEquals(cache.get(200), "V200");
        System.out.println(cache.stats());
    }

    @Test
    public void testPutIfAbsent() throws IOException, InterruptedException {
        Thread.sleep(5000);
        String old = cache.putIfAbsent(1, "V1M");
        Assert.assertEquals(old, "V1");
        Assert.assertEquals(cache.get(1), "V1");
        Assert.assertEquals(cache.get(200), "V200");
        System.out.println(cache.stats());
    }

    @Test
    public void testReplace() throws IOException, InterruptedException {
        Thread.sleep(5000);
        String old = cache.replace(100000000,"V1M");
        Assert.assertEquals(old, null);
        Assert.assertEquals(cache.get(100000000), null);

        old = cache.replace(1,"V1M");
        Assert.assertEquals(old, "V1");
        Assert.assertEquals(cache.get(1), "V1M");

        old = cache.replace(200,"V200M");
        Assert.assertEquals(old, "V200");
        Assert.assertEquals(cache.get(200), "V200M");

        System.out.println(cache.stats());
    }

    @Test
    public void testReplace2() throws IOException, InterruptedException {
        Thread.sleep(5000);
        boolean res = cache.replace(100000000,"V1000", "V1000M");
        Assert.assertFalse(res);
        Assert.assertEquals(cache.get(100000000), null);

        res = cache.replace(1,"V1W", "V1M");
        Assert.assertFalse(res);
        Assert.assertEquals(cache.get(1), "V1");

        res = cache.replace(2,"V2", "V2M");
        Assert.assertTrue(res);
        Assert.assertEquals(cache.get(2), "V2M");

        System.out.println(cache.stats());
    }

    @Test
    public void testRemove() throws IOException, InterruptedException {
        Thread.sleep(5000);
        String old = cache.remove(100000000);
        Assert.assertEquals(old, null);
        Assert.assertEquals(cache.get(100000000), null);

        old = cache.remove(1);
        Assert.assertEquals(old, "V1");
        Assert.assertEquals(cache.get(1), null);

        old = cache.remove(200);
        Assert.assertEquals(old, "V200");
        Assert.assertEquals(cache.get(200), null);

        System.out.println(cache.stats());
    }

    @Test
    public void testRemove2() throws IOException, InterruptedException {
        Thread.sleep(5000);
        boolean res = cache.remove(100000000,"V1000");
        Assert.assertFalse(res);
        Assert.assertEquals(cache.get(100000000), null);

        res = cache.remove(1,"V10");
        Assert.assertFalse(res);
        Assert.assertEquals(cache.get(1), "V1");

        res = cache.remove(2,"V2");
        Assert.assertTrue(res);
        Assert.assertEquals(cache.get(2), null);

        System.out.println(cache.stats());
    }
}
