package com.spring2go.bigcache;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import com.spring2go.bigcache.CacheConfig.StorageMode;
import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestUtil;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class BigCacheLimitTest {
    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/bigcache/";

    private static BigCache<String> cache;

    public static void main(String args[]) throws IOException {
        CacheConfig config = new CacheConfig()
                .setStorageMode(StorageMode.OffHeapPlusFile)
                .setPurgeInterval(2 * 1000)
                .setMergeInterval(2 * 1000)
                .setMaxOffHeapMemorySize(10 * 1000 * 1024 * 1024L);

        cache = new BigCache<String>(TEST_DIR, config);

        String rndString = TestUtil.randomString(10);

        System.out.println("Start from date " + new Date());
        long start = System.currentTimeMillis();
        for (long counter = 0;; counter++) {
            cache.put(Long.toString(counter), rndString.getBytes());
            if (counter % 1000000 == 0) {
                System.out.println("Current date " + new Date());
                System.out.println("" + counter);
                System.out.println(TestUtil.printMemoryFootprint());
                long end = System.currentTimeMillis();
                System.out.println("timeSpent = " + (end - start));
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
                start = System.currentTimeMillis();
            }
        }
    }

    public static void close() throws IOException {
        if (cache == null)
            return;
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
