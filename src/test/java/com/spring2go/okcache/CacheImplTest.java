package com.spring2go.okcache;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CacheImplTest {
    public Cache<Integer, String> cache5() {
        Cache<Integer, String> cache = CacheBuilder.newBuilder(Integer.class, String.class)
                .expireAfterAccess(1000, TimeUnit.MILLISECONDS)
                .expireAfterWrite(1500, TimeUnit.MILLISECONDS)
                .build();
        cache.put(1, "A");
        cache.put(2, "B");
        cache.put(3, "C");
        cache.put(4, "D");
        cache.put(5, "E");
        return cache;
    }

    public Cache<Integer, String> cache5b() {
        Cache<Integer, String> cache = CacheBuilder.newBuilder(Integer.class, String.class)
                .expireAfterWrite(1500, TimeUnit.MILLISECONDS)
                .build();
        cache.put(1, "A");
        cache.put(2, "B");
        cache.put(3, "C");
        cache.put(4, "D");
        cache.put(5, "E");
        return cache;
    }

    @Test
    public void testGet() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        Assert.assertEquals(cache.get(1), "A");
        Assert.assertEquals(cache.get(2), "B");
        Assert.assertEquals(cache.get(3), "C");
        Assert.assertEquals(cache.get(4), "D");
        Assert.assertEquals(cache.get(5), "E");
        Thread.sleep(2000);
        Assert.assertEquals(cache.get(1), null);
        Assert.assertEquals(cache.get(2), null);
        Assert.assertEquals(cache.get(3), null);
        Assert.assertEquals(cache.get(4), null);
        Assert.assertEquals(cache.get(5), null);
    }

    @Test
    public void testGet2() {
        Cache<Integer, String> cache = cache5();
        Assert.assertEquals(cache.get(1), "A");
        cache.put(1, "W");
        Assert.assertEquals(cache.get(1), "W");
    }

    @Test
    public void testGet3() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        Assert.assertEquals(cache.get(1), "A");
        Assert.assertEquals(cache.get(2), "B");
        Thread.sleep(800);
        Assert.assertEquals(cache.get(1), "A");
        Assert.assertEquals(cache.get(2, Cache.UpdateTimestamp.Write), "B");
        Thread.sleep(800);
        Assert.assertEquals(cache.get(1), null);
        Assert.assertEquals(cache.get(2), "B");
    }

    @Test
    public void testGet4() throws InterruptedException {
        Cache<Integer, String> cache = cache5b();
        Assert.assertEquals(cache.get(1), "A");
        Assert.assertEquals(cache.get(2), "B");
        Thread.sleep(1000);
        Assert.assertEquals(cache.get(1), "A");
        Assert.assertEquals(cache.get(2, Cache.UpdateTimestamp.Write), "B");
        Thread.sleep(1000);
        Assert.assertEquals(cache.get(1), null);
        Assert.assertEquals(cache.get(2), "B");
        Thread.sleep(1000);
        Assert.assertEquals(cache.get(1), null);
        Assert.assertEquals(cache.get(2), null);
    }

    @Test
    public void testPut() {
        Cache<Integer, String> cache = cache5();
        cache.put(6, "F");
        Assert.assertEquals(cache.get(6), "F");
    }

    @Test
    public void testPut2() {
        Cache<Integer, String> cache = cache5();
        String old = cache.put(1, "W");
        Assert.assertEquals(old, "A");
        Assert.assertEquals(cache.get(1), "W");
    }

    @Test
    public void testPut3() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        cache.put(7, "7A", ExpirationPolicy.afterWrite(3000));
        cache.put(8,"8A",ExpirationPolicy.afterCreate(5000));
        cache.put(9,"9A",ExpirationPolicy.never());

        Assert.assertEquals(cache.get(1), "A");
        Assert.assertEquals(cache.get(7), "7A");
        Assert.assertEquals(cache.get(8), "8A");
        Assert.assertEquals(cache.get(9), "9A");

        Thread.sleep(1001);

        Assert.assertEquals(cache.get(1), null);
        Assert.assertEquals(cache.get(7), "7A");
        Assert.assertEquals(cache.get(8), "8A");
        Assert.assertEquals(cache.get(9), "9A");

        Thread.sleep(2000);

        Assert.assertEquals(cache.get(1), null);
        Assert.assertEquals(cache.get(7), null);
        Assert.assertEquals(cache.get(8), "8A");
        Assert.assertEquals(cache.get(9), "9A");

        Thread.sleep(2000);

        Assert.assertEquals(cache.get(1), null);
        Assert.assertEquals(cache.get(7), null);
        Assert.assertEquals(cache.get(8), null);
        Assert.assertEquals(cache.get(9), "9A");
    }

    @Test
    public void testPutIfAbsent() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        String current = cache.putIfAbsent(1, "W");
        Assert.assertEquals(current, "A");
        Assert.assertEquals(cache.get(1), "A");

        current = cache.putIfAbsent(6, "F");
        Assert.assertEquals(current, null);
        Assert.assertEquals(cache.get(6), "F");

        Thread.sleep(2000);
        current = cache.putIfAbsent(1, "W");
        Assert.assertEquals(current, null);
        Assert.assertEquals(cache.get(1), "W");

    }

    @Test
    public void testPutAll() {
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(11, "AA");
        map.put(12, "AB");
        map.put(13, "AC");
        map.put(14, "AD");
        map.put(15, "AE");

        Cache<Integer, String> cache = cache5();
        cache.putAll(map);

        Assert.assertEquals(cache.get(11), "AA");
        Assert.assertEquals(cache.get(12), "AB");
        Assert.assertEquals(cache.get(13), "AC");
        Assert.assertEquals(cache.get(14), "AD");
        Assert.assertEquals(cache.get(15), "AE");
    }

    @Test
    public void testReplace() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        String old = cache.replace(6, "F");
        Assert.assertEquals(old, null);
        Assert.assertEquals(cache.get(6), null);

        old = cache.replace(1, "W");
        Assert.assertEquals(old, "A");
        Assert.assertEquals(cache.get(1), "W");

        Thread.sleep(2000);

        old = cache.replace(1, "A");
        Assert.assertEquals(old, null);
        Assert.assertEquals(cache.get(1), null);
    }

    @Test
    public void testReplace2() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        boolean success = cache.replace(6, "E", "F");
        Assert.assertEquals(success, false);
        Assert.assertEquals(cache.get(6), null);

        success = cache.replace(1, "B", "W");
        Assert.assertEquals(success, false);
        Assert.assertEquals(cache.get(1), "A");

        success = cache.replace(1, "A", "W");
        Assert.assertEquals(success, true);
        Assert.assertEquals(cache.get(1), "W");

        Thread.sleep(2000);

        success = cache.replace(1, "W", "A");
        Assert.assertEquals(success, false);
        Assert.assertEquals(cache.get(1), null);
    }

    @Test
    public void testRemove() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        String old = cache.remove(1);
        Assert.assertEquals(old, "A");
        Assert.assertEquals(cache.get(1), null);

        old = cache.remove(6);
        Assert.assertEquals(old, null);
        Assert.assertEquals(cache.get(6), null);

        Thread.sleep(2000);

        old = cache.remove(2);
        Assert.assertEquals(old, null);
        Assert.assertEquals(cache.get(2), null);
    }

    @Test
    public void testRemove2() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        boolean success = cache.remove(6, "E");
        Assert.assertEquals(success, false);
        Assert.assertEquals(cache.get(6), null);

        success = cache.remove(1, "B");
        Assert.assertEquals(success, false);
        Assert.assertEquals(cache.get(1), "A");

        success = cache.remove(1, "A");
        Assert.assertEquals(success, true);
        Assert.assertEquals(cache.get(1), null);

        Thread.sleep(2000);

        success = cache.remove(2, "B");
        Assert.assertEquals(success, false);
        Assert.assertEquals(cache.get(2), null);
    }

    @Test
    public void testContains() throws InterruptedException {
        Cache<Integer, String> cache = cache5();
        Assert.assertEquals(cache.contains(1), true);
        Assert.assertEquals(cache.contains(6), false);

        Thread.sleep(2000);

        Assert.assertEquals(cache.contains(1), false);
    }
}
