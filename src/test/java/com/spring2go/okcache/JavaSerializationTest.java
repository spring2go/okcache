package com.spring2go.okcache;

import com.spring2go.okcache.impl.HashCacheEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class JavaSerializationTest {
    @Test
    public void perfTest() throws Exception {
        Serializer serializer = new JavaSerializer();
        Random random = new Random();

        int count = 10000;
        CacheEntry<String, byte[]>[] es = new CacheEntry[count];
        for (int i = 0; i < count; i++) {
            byte[] b = new byte[random.nextInt(1024) + 1024*5];
            Arrays.fill(b, (byte) random.nextInt(100));
            es[i] = new HashCacheEntry<String, byte[]>(0,"key-" + i, b, ExpirationPolicy.never());
        }

        byte[][] res = new byte[count][];
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            res[i] = serializer.serialize(es[i]);
        }

        System.out.println("Cost(ms): " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            serializer.deserialize(res[i]);
        }
        System.out.println("Cost(ms): " + (System.currentTimeMillis() - start));

    }

    @Test
    public void test2() throws Exception {
        Serializer serializer = new JavaSerializer();
        byte[] a1 = serializer.serialize(new Integer(1));
        byte[] a2 = serializer.serialize(new Integer(1));

        Assert.assertTrue(Arrays.equals(a1,a2));

    }
}
