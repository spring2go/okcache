package com.spring2go.lrucache.v2;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class LruCacheV2Test {
    @Test
    public void test1() {
        LruCacheV2<Integer, Integer> cache = new LruCacheV2<>(5);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.put(4, 4);
        cache.put(5, 5);

        Assert.assertTrue(cache.get(1) == 1 && cache.get(2) == 2 && cache.get(4) == 4);
    }

    @Test
    public void test2() {
        LruCacheV2<Integer, Integer> cache = new LruCacheV2<>(5);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.put(4, 4);
        cache.put(5, 5);

        cache.put(6, 6);

        Assert.assertEquals(cache.size(), 5);
    }

    @Test
    public void test3() {
        LruCacheV2<Integer, Integer> cache = new LruCacheV2<>(5);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.put(4, 4);
        cache.put(5, 5);

        cache.put(6, 6);

        cache.put(7, 7);

        Assert.assertTrue(cache.get(4) == 4 && cache.get(6) == 6 && cache.get(7) == 7);
    }

    @Test
    public void test4() {
        LruCacheV2<Integer, Integer> cache = new LruCacheV2<>(5);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.put(4, 4);
        cache.put(5, 5);

        cache.put(6, 6);
        cache.put(7, 7);

        Assert.assertTrue(cache.get(1) == null);
    }

    @Test
    public void test5() {
        LruCacheV2<Integer, Integer> cache = new LruCacheV2<>(3);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(1, 1);
        cache.put(3, 3);
        cache.put(4, 4);

        Assert.assertTrue(cache.get(1) == 1);
    }
}
