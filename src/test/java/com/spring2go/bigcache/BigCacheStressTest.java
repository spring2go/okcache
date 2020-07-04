package com.spring2go.bigcache;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.spring2go.bigcache.CacheConfig.StorageMode;
import com.spring2go.bigcache.utils.TestUtil;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class BigCacheStressTest {
    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/bigcache/";

    private static BigCache<String> cache;

    public static void main(String[] args) throws Exception {
        int numKeyLimit = 1024 * 16;
        int valueLengthLimit = 1024 * 16;

        CacheConfig config = new CacheConfig();
        config.setStorageMode(StorageMode.OffHeapPlusFile)
                .setPurgeInterval(2 * 1000)
                .setMergeInterval(2 * 1000)
                //allocate half of the memory - leave space for keys and reference map.
                .setMaxOffHeapMemorySize(5 * 1000 * 1024 * 1024L);
        cache = new BigCache<String>(TEST_DIR, config);
        Map<String, byte[]> map = new HashMap<String, byte[]>();

        String[] rndStrings = { TestUtil.randomString(valueLengthLimit / 2),
                TestUtil.randomString(valueLengthLimit),
                TestUtil.randomString(valueLengthLimit + valueLengthLimit / 2) };
        byte[] rndBytes = rndStrings[1].getBytes();

        Random random = new Random();

        System.out.println("Start from date " + new Date());
        long start = System.currentTimeMillis();
        for (long counter = 0;; counter++) {
            int rndKey = random.nextInt(numKeyLimit);
            boolean put = random.nextDouble() < 0.5 ? true : false;
            if (put) {
                rndBytes = rndStrings[random.nextInt(3)].getBytes();
                map.put(String.valueOf(rndKey), rndBytes);
                cache.put(String.valueOf(rndKey), rndBytes);
            } else {
                map.remove(String.valueOf(rndKey));
                byte[] oldV = cache.delete(String.valueOf(rndKey));
                byte[] v = cache.get(String.valueOf(rndKey));
                if (v != null) {
                    System.out.println("should be null. Key:" + String.valueOf(rndKey) + "    Value:" + new String(v));
                    System.out.println("                Key:" + String.valueOf(rndKey) + " oldValue:"
                            + (oldV == null ? null : new String(oldV)));
                }
            }

            cache.put(counter + "-ttl", rndBytes, (long) 10 * 1000);

            if (counter % 1000000 == 0) {
                System.out.println("Current date: " + new Date());
                System.out.println("counter:      " + counter);
                System.out.println("purge:        " + cache.getStats().getCacheExpire());
                System.out.println("move:         " + cache.getStats().getCacheMove());
                System.out.println("size:        " + cache.count());
                long cacheUsed = cache.getStats().getStorageUsed();
                System.out.println("used:         " + cacheUsed);
                System.out.println();

                long storeUsed = cache.storageManager.getUsed();
                if (cacheUsed != storeUsed) {
                    System.out.println("!!!! Temporarily fail the test, this could seldom occur");
                    System.out.println("storage used: " + storeUsed + ", but cache used: " + cacheUsed);
                }

                System.out.println();
                System.out.println(TestUtil.printMemoryFootprint());
                long end = System.currentTimeMillis();
                System.out.println("timeSpent = " + (end - start));
                System.out.println("ttl count = " + (cache.count() - map.size()));
                System.out.println("used size = " + cache.getStats().getStorageUsed());

                // validation
                for (int i = 0; i < numKeyLimit; i++) {
                    String key = String.valueOf(i);
                    byte[] mapValue = map.get(key);
                    byte[] cacheValue = cache.get(key);

                    if (mapValue == null && cacheValue != null) {
                        System.out.println("Key:" + key);
                        System.out.println("Value:" + new String(cacheValue));
                        throw new RuntimeException("Validation exception, key exists in cache but not in map");
                    }
                    if (mapValue != null && cacheValue == null) {
                        throw new RuntimeException("Validation exception, key exists in map but not in cache");
                    }
                    if (mapValue != null && cacheValue != null) {
                        if (compare(mapValue, cacheValue) != 0) {
                            throw new RuntimeException("Validation exception, values in map and cache does not equal");
                        }
                    }
                }


                //exclude validation process from time count
                start = System.currentTimeMillis();
            }
        }
    }

    public static int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}
