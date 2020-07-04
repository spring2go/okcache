package com.spring2go.okcache;

import org.junit.Test;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class CachePerfGetsTest {
    @Test
    public void  stressGet(){
        final Cache<Serializable,Serializable> cache = CacheBuilder.newBuilder(Serializable.class,Serializable.class)
                .expireAfterWrite(1000, TimeUnit.MINUTES)
                .maximumSize(200000)
                .concurrencyLevel(25)
                .build();

        final int eCount = 20000;
        final Serializable[] ks = new Serializable[eCount];
        for (int i = 0; i < eCount; i++) {
            cache.put("k" + i, "v" + i);
            ks[i] = "k" + i;
        }


        final AtomicInteger totalCount = new AtomicInteger();

        int tCount = 25;
        final int time = 1000*60;
        Thread[] ts = new Thread[tCount];
        for (int i = 0; i < tCount; i++) {
            ts[i] = new Thread(){
                int count = 0;
                @Override
                public void run() {
                    Random random = new Random();
                    long start = System.currentTimeMillis();
                    while(true){
                        int i = random.nextInt(eCount);
                        cache.get(ks[i]);
                        count++;
                        if(System.currentTimeMillis() - start >= time) break;
                    }
                    totalCount.addAndGet(count);
                }
            };
        }

        for (Thread t : ts) {
            t.start();
        }

        for (Thread t : ts) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Total Count:" + totalCount.get());
        System.out.println("Ops/Sec:" + (totalCount.get()/(time/1000)));

    }

    @Test
    public void  stressConcurrentHashMap(){
        final Map<Serializable,Serializable> cache = new ConcurrentHashMap<Serializable, Serializable>(1000,0.7f, 4);

        final int eCount = 20000;
        final Serializable[] ks = new Serializable[eCount];
        for (int i = 0; i < eCount; i++) {
            cache.put("k" + i, "v" + i);
            ks[i] = "k" + i;
        }


        final AtomicInteger totalCount = new AtomicInteger();

        int tCount = 25;
        final int time = 1000*60;
        Thread[] ts = new Thread[tCount];
        for (int i = 0; i < tCount; i++) {
            ts[i] = new Thread(){
                int count = 0;
                @Override
                public void run() {
                    Random random = new Random();
                    long start = System.currentTimeMillis();
                    while(true){
                        int i = random.nextInt(eCount);
                        cache.put(ks[i], "v" + i);
                        //cache.get(ks[i]);
                        count++;
                        if(System.currentTimeMillis() - start >= time) break;
                    }
                    totalCount.addAndGet(count);
                }
            };
        }

        for (Thread t : ts) {
            t.start();
        }

        for (Thread t : ts) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Total Count:" + totalCount.get());
        System.out.println("Ops/Sec:" + (totalCount.get()/(time/1000)));

    }
}
