package com.spring2go.bigcache;

import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.spring2go.bigcache.CacheConfig.StorageMode;

/**
 * Created on Jul, 2020 by @author bobo
 */
@RunWith(Parameterized.class)
public class BigCacheCleanerTest {
    private static final int[] PRIMES = {2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97};
    private static final Random random = new Random();
    //@NotThreadSafe
    private static int randomPrime() {
        return PRIMES[random.nextInt(PRIMES.length)];
    }

    private static int randomInt(int max) {
        return random.nextInt(max);
    }

    // count
    private final AtomicLong writeCounter = new AtomicLong();
    private final AtomicLong readCounter = new AtomicLong();
    private final AtomicLong deleteCounter = new AtomicLong();

    // size of payload
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private final AtomicLong deleteBytes = new AtomicLong();

    // time counsumed
    private final AtomicLong writeConsumed = new AtomicLong();
    private final AtomicLong readConsumed = new AtomicLong();
    private final AtomicLong deleteConsumed = new AtomicLong();

    // workers
    private final AtomicLong writeWorkers = new AtomicLong();
    private final AtomicLong readWorkers = new AtomicLong();
    private final AtomicLong deleteWorkers = new AtomicLong();

    private BigCache cache;

    private ThreadPoolExecutor pool;
    private List<Future<Long>> futures;
    private CountDownLatch latch;

    @Parameterized.Parameter(value = 0)
    public StorageMode storageMode;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<StorageMode[]> data() throws IOException {
        StorageMode[][] data = { { StorageMode.PureFile },
                { StorageMode.MemoryMappedPlusFile },
                { StorageMode.OffHeapPlusFile } };
        return Arrays.asList(data);
    }

    @Before
    public void setUp() {
        // big cache
        CacheConfig config = new CacheConfig();
        config.setCapacityPerBlock(16 * 1024 * 1024)
                .setConcurrencyLevel(10)
                .setInitialNumberOfBlocks(8)
                .setPurgeInterval(5 * 1000)
                .setMergeInterval(5 * 1000)
                .setDirtyRatioLimit(0.5)
                .setStorageMode(storageMode);
        try {
            cache = new BigCache(TestUtil.TEST_BASE_DIR, config);
        } catch (IOException e) {
            throw new RuntimeException("fail to create cache", e);
        }

        // thread pools
        pool = new ThreadPoolExecutor(16, 100, 5*1000, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());
        futures = new ArrayList<Future<Long>>();
        latch = new CountDownLatch(1);
        System.out.println("---------START----------");
    }

    @After
    public void tearDown() throws IOException {
        System.out.println("----------END-----------");
        System.out.println();

        // clear now;
        futures.clear();
        pool.shutdown();

        try {
            cache.close();
            FileUtil.deleteDirectory(new File(TestUtil.TEST_BASE_DIR));
        } catch (IllegalStateException e) {
            System.gc();
            try {
                FileUtil.deleteDirectory(new File(TestUtil.TEST_BASE_DIR));
            } catch (IllegalStateException e1) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e2) {
                }
                FileUtil.deleteDirectory(new File(TestUtil.TEST_BASE_DIR));
            }
        }
    }

    @Test
    public void cacheDestroyTest() throws IOException {
        cache.put("aa", "bb".getBytes());
        cache.get("aa");
        TestUtil.sleepQuietly(1000);
        System.out.println(new String(cache.get("aa")));
    }

    @Test
    public void simpleThreadPurgeTest() throws IOException {
        String testStr = "thisisfortest";
        cache.put("keywithoutttl", testStr.getBytes());
        cache.put("keywithttl", testStr.getBytes(), 2 * 1000);

        assertEquals(testStr, new String(cache.get("keywithttl")));
        assertEquals(testStr, new String(cache.get("keywithoutttl")));

        /**
         * sleep for 4 seconds, so the entry with ttl will expired
         */
        TestUtil.sleepQuietly(4 * 1000);
        assertEquals(testStr, new String(cache.get("keywithoutttl")));
        assertEquals(null, cache.get("keywithttl"));
        // still have 2 entries
        assertEquals(2, cache.pointerMap.size());

        // sleep a bit more than 1 minute for purge, and there is only one left
        TestUtil.sleepQuietly(15 * 1000);
        assertEquals(1, cache.pointerMap.size());

        // remove the only one entry
        assertTrue(cache.storageManager.getUsed() > 0);
        cache.delete("keywithoutttl");
        assertTrue(cache.storageManager.getUsed() == 0);
    }

    @Test
    public void singleThreadMergeTest() throws IOException {
        byte[] value = new byte[1000000]; // 1m-length value
        // 10 entry with ttl, 6 without ttl
        for (int i = 0; i < 10 ; i++) {
            cache.put("block1-keywithttl-" + i, value, 5 * 1000);
        }
        for (int i = 10; i < 16; i++) {
            cache.put("block1-keywithoutttl-" + i, value);
        }

        // do the same thing for the second block
        for (int i = 0; i < 10 ; i++) {
            cache.put("block2-keywithttl-" + i, value, 5 * 1000);
        }
        for (int i = 10; i < 16; i++) {
            cache.put("block2-keywithoutttl-" + i, value);
        }

        assertEquals(2, cache.storageManager.getUsedBlockCount());
        TestUtil.sleepQuietly(15000*2);
        assertEquals(1, cache.storageManager.getUsedBlockCount());
        assertEquals(12*1000*1000, cache.storageManager.getUsed());
    }

    @Test
    public void multipleThreadPurgeTest() {
        int count = 500000;
        int workers = 1;
        int valueLen = 4;
        int keyMax = count; // every key will be inserted once
        int ttl = 1000; // 1 second
        int sleepAfter = 0;

        // 8m data with 5s ttl
        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
                , sleepAfter);

        // another worker for 16m data
        valueLen = 8;
        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
                , sleepAfter);
        execute();

        TestUtil.sleepQuietly(30000); // wait for cleaner
        assertEquals(count * 2, cache.getStats().getCacheExpire());
        assertEquals(0, cache.count());
    }

    @Test
    public void multipleThreadMergeTest() {
        int count = 100000; // 100k rounds
        int workers = 1;
        int valueLen = 100;
        int keyMax = count; // every key will be inserted once
        int ttl = 5 * 1000;
        int sleepAfter = 0;

        count = 100000;
        valueLen = 4;
        ttl = 0;
        // write 400k data that will never expire
        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
                , sleepAfter);

        execute(false);

        valueLen = 84;
        ttl = 1 * 1000;
        // write a bit more than 8m data that will expires in 5 seconds slowly
        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
                , sleepAfter);

        execute();

        // The previous test is fast enough before the first run of cleaner
        TestUtil.sleepQuietly(30000); // wait for cleaner
        assertEquals(count, cache.getStats().getCacheMove());
    }

    /**
     * The following tests are commented out as we have other tests.
     */
//    @Test
//    public void mixOperationTest() {
//        /**
//         * For each value length in [16, 1000, 100000], three will be 4 workers for reading,
//         * 4 workers for writing, 4 workers for deleting.
//         */
//        // default configs
//        int count = 1000000;
//        int workers = 4;
//        int valueLen = 16;
//        int keyMax = 500;
//        int ttl = 5 * 1000;
//        int sleepAfter = 1000;
//
//        /**
//         * valueLen = 16, sleepAfter = 100(after), ttl = 0 --> many small data which will never expires
//         */
//        valueLen = 16;
//        ttl = 0;
//        sleepAfter = 100;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        // partial delete
//        createWorkers(WORKERTYPE.delete, count, workers, valueLen, keyMax/20, ttl
//                , sleepAfter);
//
//        /**
//         * valueLen = 1000
//         */
//        valueLen = 1000;
//        sleepAfter = 2000;
//        ttl = -2000; // negative value means the ttl will be "abs(ttl) * random[0, 5]"
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        createWorkers(WORKERTYPE.delete, count, workers, valueLen, keyMax/10, ttl
//                , sleepAfter);
//
//        /**
//         * valueLen = 100000
//         */
//        valueLen = 100000;
//        sleepAfter = 1000;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax*100, ttl
//                , sleepAfter);
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        createWorkers(WORKERTYPE.delete, count/10/*less times*/, workers, valueLen, keyMax/10, ttl
//                , sleepAfter/10/*less frequent*/);
//
//        TestUtil.sleepQuietly(20000);
//
//        execute();
//    }
//
//    @Test
//    public void write_Count_500000_Workers_16_ValLen_4() {
//        int count = 500000;
//        int workers = 16;
//        int valueLen = 4;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//    }
//
//    @Test
//    public void write_Count_50000_Workers_16_ValLen_16() {
//        int count = 50000;
//        int workers = 16;
//        int valueLen = 16;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//    }
//
//    @Test
//    public void write_Count_5000_Workers_16_ValLen_10k() {
//        int count = 5000;
//        int workers = 16;
//        int valueLen = 10000;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//    }
//
//    @Test
//    public void write_Count_50_Workers_4_ValLen_100k() {
//        int count = 50;
//        int workers = 4;
//        int valueLen = 100000;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//    }
//
//    @Test
//    public void write_Count_50_Workers_4_ValLen_1000k() {
//        int count = 5;
//        int workers = 4;
//        int valueLen = 1000000;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//    }
//
//    @Test
//    public void read_Count_500000_Workers_16_ValLen_4() {
//        // first populate data
//        int count = 600;
//        int workers = 1;
//        int valueLen = 4;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute(false);
//
//        count = 500000;
//        workers = 16;
//        valueLen = 4;
//        keyMax = 500;
//        ttl = -1;
//        sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//        assertEquals(1.0, cache.hitRatio(), 1e-6);
//    }
//
//    @Test
//    public void read_Count_50000_Workers_16_ValLen_16() {
//        // first populate data
//        int count = 600;
//        int workers = 1;
//        int valueLen = 16;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute(false);
//
//        count = 50000;
//        workers = 16;
//        valueLen = 16;
//        keyMax = 500;
//        ttl = -1;
//        sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//        assertEquals(1.0, cache.hitRatio(), 1e-6);
//    }
//
//    @Test
//    public void read_Count_5000_Workers_16_ValLen_10k() {
//        // first populate data
//        int count = 600;
//        int workers = 1;
//        int valueLen = 10000;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute(false);
//
//        count = 5000;
//        workers = 16;
//        valueLen = 10000;
//        keyMax = 500;
//        ttl = -1;
//        sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//        assertEquals(1.0, cache.hitRatio(), 1e-6);
//    }
//
//    @Test
//    public void read_Count_50_Workers_4_ValLen_100k() {
//        // first populate data
//        int count = 600;
//        int workers = 1;
//        int valueLen = 100000;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute(false);
//
//        count = 50;
//        workers = 4;
//        valueLen = 100000;
//        keyMax = 500;
//        ttl = -1;
//        sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//        assertEquals(1.0, cache.hitRatio(), 1e-6);
//    }
//
//    @Test
//    public void read_Count_50_Workers_4_ValLen_1000k() {
//        // first populate data
//        int count = 600;
//        int workers = 1;
//        int valueLen = 1000000;
//        int keyMax = 500;
//        int ttl = -1;
//        int sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.write, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute(false);
//
//        count = 50;
//        workers = 4;
//        valueLen = 1000000;
//        keyMax = 500;
//        ttl = -1;
//        sleepAfter = -1;
//
//        createWorkers(WORKERTYPE.read, count, workers, valueLen, keyMax, ttl
//                , sleepAfter);
//
//        execute();
//        assertEquals(1.0, cache.hitRatio(), 1e-6);
//    }

    // do the work
    private void execute(boolean needPrintStats) {
        start();
        waitToComplete();
        if (needPrintStats) {
            printStats();
        }
        clearStats();
    }

    private void execute() {
        execute(true);
    }

    private void start() {
        latch.countDown();
    }

    private void waitToComplete() {
        for(Future<Long> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        futures.clear();
    }

    private void printStats() {
        System.out.println("---------STATS----------");

        if (writeWorkers.get() > 0) {
            printWorkerStats(WORKERTYPE.write, writeCounter.get(),
                    writeConsumed.get(), writeBytes.get(), writeWorkers.get());
        }

        if (readWorkers.get() > 0) {
            printWorkerStats(WORKERTYPE.read, readCounter.get(),
                    readConsumed.get(), readBytes.get(), readWorkers.get());
        }

        if (deleteWorkers.get() > 0) {
            printWorkerStats(WORKERTYPE.delete, deleteCounter.get(),
                    deleteConsumed.get(), deleteBytes.get(), deleteWorkers.get());
        }

        System.out.println("cache hit ratio:" + cache.hitRatio());
        System.out.println("purge count:" + cache.getStats().getCacheExpire());
        System.out.println("move count:" + cache.getStats().getCacheMove());
        System.out.println(TestUtil.printMemoryFootprint());
    }

    private void printWorkerStats(WORKERTYPE type, long count, long time, long bytes, long workers) {
        String template = "%,d %s consumed %,d nano seconds, %.2f per second, %,.2f bytes per second";
        String stats = String.format(template, count, type.toString(), time, 1.0 * 1e9 * count / time, 1.0 * 1e9 * bytes / time);
        System.out.println(String.format("%d workers for %s", workers, type.toString()));
        System.out.println(stats);
    }

    private void clearStats() {
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field f : fields) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            try {
                Object o = f.get(this);
                if (!(o instanceof AtomicLong)) {
                    continue;
                }
                ((AtomicLong) o).set(0);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    enum WORKERTYPE {
        write,
        read,
        delete
    }

    private void createWorkers(WORKERTYPE type, int count, int workers, int valueLen, int keyMax, int ttl, int sleepAfter) {
        System.out.println(String.format("Start %d %s workers, each run %d rounds, with value length: %d"
                , workers, type.toString(), count, valueLen));
        for(int i = 0; i < workers; i++) {
            WorkerConfig config = new WorkerConfig();
            int step = randomPrime();
            while(keyMax % step == 0) {
                step = randomPrime();
            }
            config.setCount(count)
                    .setTtl(ttl)
                    .setKeyMax(keyMax)
                    .setKeyStart(randomInt(keyMax))
                    .setKeyStep(step)
                    .setSleepAfter(sleepAfter)
                    .setValueLen(valueLen);
            if (type == WORKERTYPE.delete) {
                new DeleteWorker(config);
            }
            if (type == WORKERTYPE.read) {
                new ReadWorker(config);
            }
            if (type == WORKERTYPE.write) {
                new WriteWorker(config);
            }
        }

    }

    static class WorkerConfig {
        // ttl if put/update, -1 means for ever
        int ttl = -1;
        // key should be in [0, keyMax), and keys is generated:keyStart, (keyStart+keyStep)%keyMax,...,(keyStart+n*keyStep)%keyMax
        int keyMax = 1000;
        int keyStart = randomInt(1000);
        int keyStep = randomPrime();
        // valueLen % 4 == 0, and is filled with key's hashcode
        int valueLen = 16;
        // how many keys is operated
        int count = 10000;
        long sleepAfter = 10000;

        static WorkerConfig getConfig() {
            WorkerConfig result = new WorkerConfig();
            return result;
        }

        public int getTtl() {
            return ttl;
        }

        public int getKeyMax() {
            return keyMax;
        }

        public int getKeyStart() {
            return keyStart;
        }

        public int getKeyStep() {
            return keyStep;
        }

        public int getValueLen() {
            return valueLen;
        }

        public int getCount() {
            return count;
        }

        public long getSleepAfter() {
            return sleepAfter;
        }

        public WorkerConfig setTtl(int ttl) {
            this.ttl = ttl;
            return this;
        }

        public WorkerConfig setKeyMax(int keyMax) {
            this.keyMax = keyMax;
            return this;
        }

        public WorkerConfig setKeyStart(int keyStart) {
            this.keyStart = keyStart;
            return this;
        }

        public WorkerConfig setKeyStep(int keyStep) {
            this.keyStep = keyStep;
            return this;
        }

        public WorkerConfig setValueLen(int valueLen) {
            valueLen = (valueLen + 3) /4 * 4;
            this.valueLen = valueLen;
            return this;
        }

        public WorkerConfig setCount(int count) {
            this.count = count;
            return this;
        }

        public WorkerConfig setSleepAfter(long sleepAfter) {
            this.sleepAfter = sleepAfter;
            return this;
        }

        public WorkerConfig build() {
            WorkerConfig result = new WorkerConfig();
            result.ttl = ttl;
            result.count = count;
            result.sleepAfter = sleepAfter;
            result.keyMax = keyMax;
            result.keyStart = keyStart;
            result.keyStep = keyStep;
            result.valueLen = valueLen;
            return result;
        }
    }

    abstract class Worker implements Callable<Long> {
        private long lastSleep = System.currentTimeMillis();
        protected WorkerConfig config;

        Worker(WorkerConfig config) {
            this.config = config;
            futures.add(pool.submit(this));
        }

        @Override
        public Long call() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeConsumed = 0;
            for (int i = 0; i < config.getCount(); i++) {
                long current = System.currentTimeMillis();
                if (config.getSleepAfter() > 0) {
                    if (current - lastSleep > config.getSleepAfter()* 1000) {
                        TestUtil.sleepQuietly(1000);
                        lastSleep = System.currentTimeMillis();
                    }
                }

                int keyNum = (config.getKeyStart()+ i * config.getKeyStep()) % config.getKeyMax();
                String key = config.getValueLen()+ ":" + keyNum;
                byte[] value = toBytes(keyNum, config.getValueLen());
                try {
                    timeConsumed += doCacheOperation(key, value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            assert timeConsumed != 0;
            return timeConsumed;
        }

        private byte[] toBytes(int v, int length) {
            assert length % 4 == 0;
            assert length > 0;
            byte[] result = new byte[length];

            result[0] = (byte)(v & 0xFF);
            result[1] = (byte)(v>>>8 & 0xFF);
            result[2] = (byte)(v>>>16 & 0xFF);
            result[3] = (byte)(v>>>24 & 0xFF);

            for (int i = 4; i < length;) {
                int copyLength = i <= length/2 ? i : length-i;
                System.arraycopy(result, 0,
                        result, i,
                        copyLength);
                i += copyLength;
            }
            return result;
        }

        /**
         * Do operation with key and return the time consumed.
         */
        protected abstract long doCacheOperation(String key, byte[] value) throws IOException;
    }

    class ReadWorker extends Worker {

        ReadWorker(WorkerConfig config) {
            super(config);
            readWorkers.incrementAndGet();
        }

        @Override
        protected long doCacheOperation(String key, byte[] value) throws IOException {
            long start = System.nanoTime();
            byte[] returnValue = cache.get(key);
            long result = System.nanoTime() - start;

            readCounter.incrementAndGet();
            readConsumed.addAndGet(result);
            if (returnValue != null) {
                readBytes.addAndGet(returnValue.length);
            }
            if (returnValue != null && !byteArrayEquals(value, returnValue)) {
                throw new IllegalStateException("invalid return value");
            }
            return result;
        }
    }

    class WriteWorker extends Worker {

        WriteWorker(WorkerConfig config) {
            super(config);
            writeWorkers.incrementAndGet();
        }

        @Override
        protected long doCacheOperation(String key, byte[] value) throws IOException {
            long ttl = config.getTtl();
            if (ttl < 0) {
                ttl = ttl * -1 * new Random().nextInt(5);
            }
            long start = System.nanoTime();
            cache.put(key, value, ttl);
            long result = System.nanoTime() - start;

            writeCounter.incrementAndGet();
            writeConsumed.addAndGet(result);
            writeBytes.addAndGet(value.length);
            return result;
        }
    }

    class DeleteWorker extends Worker {

        protected DeleteWorker(WorkerConfig config) {
            super(config);
            deleteWorkers.incrementAndGet();
        }

        @Override
        protected long doCacheOperation(String key, byte[] value) throws IOException {
            long start = System.nanoTime();
            byte[] returnValue = cache.delete(key);
            long result = System.nanoTime() - start;

            deleteCounter.incrementAndGet();
            deleteConsumed.addAndGet(result);
            if (returnValue != null) {
                deleteBytes.addAndGet(returnValue.length);
            }
            if (returnValue != null && !byteArrayEquals(value, returnValue)) {
                throw new IllegalStateException("invalid return value");
            }
            return result;
        }
    }

    private boolean byteArrayEquals(byte[] left, byte[] right) {
        if (left != null && right != null) {
            if (left.length != right.length) return false;
            for(int i = 0; i < left.length; i++) {
                if (left[i] != right[i]) {
                    return false;
                }
            }
            return true;
        }

        if (left == null && right == null) return true;
        return false;
    }
}
