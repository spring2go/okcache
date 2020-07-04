package com.spring2go.bigcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.spring2go.bigcache.CacheConfig.StorageMode;
import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestUtil;

/**
 * Created on Jul, 2020 by @author bobo
 */
@RunWith(Parameterized.class)
public class BigCachePerfTestB {
    /********************* configurable parameters *********************/
    private static final int LOOP = 5;
    private static final int ITEM_COUNT = 1000000;
    private static final int PRODUCER_COUNT = 4;
    private static final int CONSUMER_COUNT = 4;
    private static final int STRING_LEN = 16;
    /******************************************************************/

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "performance/bigcache/";

    private final AtomicInteger producingItemCount = new AtomicInteger(0);
    private final AtomicInteger consumingItemCount = new AtomicInteger(0);
    private final BlockingQueue<String> keysInMemoryQueue = new LinkedBlockingQueue<String>();

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
        config.setStorageMode(storageMode);
        BigCache<String> cache = new BigCache<String>(TEST_DIR, config);
        return cache;
    }

    @Test
    public void testProduceThenConsume() throws IOException, InterruptedException {
        System.out.println("BigCache performance test begin ...");
        for (int i = 0; i < LOOP; i++) {
            cache = cache();
            System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + LOOP);
            this.doRunProduceThenConsume();
            producingItemCount.set(0);
            consumingItemCount.set(0);
            this.close();
        }
        System.out.println("[doRunProduceThenConsume] test ends");
    }

    @Test
    public void testProduceMixedConsume() throws IOException, InterruptedException {
        for (int i = 0; i < LOOP; i++) {
            cache = cache();
            System.out.println("[doRunMixed] round " + (i + 1) + " of " + LOOP);
            this.doRunProduceMixedConsume();
            producingItemCount.set(0);
            consumingItemCount.set(0);
            this.close();
        }
        System.out.println("[doRunMixed] test ends");
    }

    private void doRunProduceThenConsume() throws InterruptedException {
        CountDownLatch producerLatch = new CountDownLatch(PRODUCER_COUNT);
        CountDownLatch consumerLatch = new CountDownLatch(CONSUMER_COUNT);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();
        long start = System.nanoTime();
        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        for (int i = 0; i < PRODUCER_COUNT; i++) {
            Producer p = new Producer(producerLatch, producerResults);
            p.start();
        }

        for (int i = 0; i < PRODUCER_COUNT; i++) {
            Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalProducingTime += result.duration;
        }
        long duration = System.nanoTime() - start;

        System.out.println("-----------------------------------------------");
        System.out.println("Producing test result:");
        System.out.printf("Total test time = %d ns.\n", duration);
        System.out.printf("Total item count = %d\n", ITEM_COUNT);
        System.out.printf("Producer thread number = %d\n", PRODUCER_COUNT);
        System.out.printf("Item message length = %d bytes\n", STRING_LEN);
        System.out.printf("Total producing time =  %d ns.\n", totalProducingTime);
        System.out.printf("Average producint time = %d ns.\n", totalProducingTime / PRODUCER_COUNT);
        System.out.println("-----------------------------------------------");

        // the consumer start
        start = System.nanoTime();
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            Consumer c = new Consumer(consumerLatch, consumerResults);
            c.start();
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalConsumingTime += result.duration;
        }
        duration = System.nanoTime() - start;
        assertEquals(producingItemCount.get(), consumingItemCount.get());
        assertTrue(keysInMemoryQueue.isEmpty());

        System.out.println("Consuming test result:");
        System.out.printf("Total test time = %d ns.\n", duration);
        System.out.printf("Total item count = %d\n", ITEM_COUNT);
        System.out.printf("Consumer thread number = %d\n", CONSUMER_COUNT);
        System.out.printf("Item message length = %d bytes\n", STRING_LEN);
        System.out.printf("Total consuming time =  %d ns.\n", totalConsumingTime);
        System.out.printf("Average consuming time = %d ns.\n", totalConsumingTime / CONSUMER_COUNT);
        System.out.println("-----------------------------------------------");
    }

    private void doRunProduceMixedConsume() throws InterruptedException {
        CountDownLatch allLatch = new CountDownLatch(PRODUCER_COUNT + CONSUMER_COUNT);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();

        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        long start = System.nanoTime();
        //run testing
        for (int i = 0; i < PRODUCER_COUNT; i++) {
            Producer p = new Producer(allLatch, producerResults);
            p.start();
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            Consumer c = new Consumer(allLatch, consumerResults);
            c.start();
        }

        //verify and report
        for (int i = 0; i < PRODUCER_COUNT; i++) {
            Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalProducingTime += result.duration;
        }

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalConsumingTime += result.duration;
        }

        long duration = System.nanoTime() - start;

        assertEquals(producingItemCount.get(), consumingItemCount.get());

        System.out.println("-----------------------------------------------");
        System.out.printf("Total test time = %d ns.\n", duration);
        System.out.printf("Total item count = %d\n", ITEM_COUNT);
        System.out.printf("Producer thread number = %d\n", PRODUCER_COUNT);
        System.out.printf("Consumer thread number = %d\n", CONSUMER_COUNT);
        System.out.printf("Item message length = %d bytes\n", STRING_LEN);
        System.out.printf("Total consuming time =  %d ns.\n", totalConsumingTime);
        System.out.printf("Average consuming time = %d ns.\n", totalConsumingTime / CONSUMER_COUNT);
        System.out.printf("Total producing time =  %d ns.\n", totalProducingTime);
        System.out.printf("Average producing time = %d ns.\n", totalProducingTime / PRODUCER_COUNT);
        System.out.println("-----------------------------------------------");
    }

    private void close() throws IOException {
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

    private class Producer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;

        public Producer(CountDownLatch latch, Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
        }

        public void run() {
            Result result = new Result();
            try {
                latch.countDown();
                latch.await();

                long start = System.nanoTime();
                while (true) {
                    int count = producingItemCount.incrementAndGet();
                    if (count > ITEM_COUNT)
                        break;

                    String string = TestUtil.randomString(STRING_LEN);

                    keysInMemoryQueue.offer(string);
                    cache.put(string, string.getBytes());
                }
                result.status = Status.SUCCESS;
                result.duration = System.nanoTime() - start;
            } catch (Exception e) {
                e.printStackTrace();
                result.status = Status.ERROR;
            }
            resultQueue.offer(result);
        }
    }

    private class Consumer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;

        public Consumer(CountDownLatch latch, Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
        }

        public void run() {
            Result result = new Result();
            result.status = Status.SUCCESS;
            try {
                latch.countDown();
                latch.await();

                long start = System.nanoTime();
                while (true) {
                    int count = consumingItemCount.getAndIncrement();
                    if (count >= ITEM_COUNT)
                        break;

                    String key = keysInMemoryQueue.take();
                    if (key != null && !key.isEmpty()) {
                        byte[] valueBytes = cache.get(key);
                        // wait a moment for k/v to be put in the DB
                        // may cause dead lock
                        while (valueBytes == null) {
                            valueBytes = cache.get(key);
                        }
                        if (!key.equals(new String(valueBytes))) {
                            result.status = Status.ERROR;
                        }
                    }
                }
                result.duration = System.nanoTime() - start;
            } catch (Exception e) {
                e.printStackTrace();
                result.status = Status.ERROR;
            }
            resultQueue.offer(result);
        }
    }

    private static enum Status {
        ERROR,
        SUCCESS
    }

    private static class Result {
        Status status;
        long duration;
    }
}
