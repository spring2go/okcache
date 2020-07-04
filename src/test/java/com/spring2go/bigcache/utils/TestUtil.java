package com.spring2go.bigcache.utils;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class TestUtil {
    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static Random rnd = new Random();

    private static final NumberFormat MEM_FMT = new DecimalFormat("##,###.##");

    public static String randomString(int len )
    {
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }

    public static void sleepQuietly(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static final String TEST_BASE_DIR = "/Users/william/test/bigcache_test/";

    public static String kbString(long memBytes) {
        return MEM_FMT.format(memBytes / 1024) + " kb";
    }

    public static String printMemoryFootprint() {
        Runtime run = Runtime.getRuntime();
        String memoryInfo = "Memory - free: " + kbString(run.freeMemory()) + " - max:" + kbString(run.maxMemory()) + "- total:" + kbString(run.totalMemory());
        return memoryInfo;
    }

    public static byte[] getBytes(Object o) {
        if (o instanceof String) {
            return ((String)o).getBytes();
        } else if (o instanceof byte[]) {
            return (byte[])o;
        } else if (o instanceof Serializable) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(o);
                byte[] bytes = bos.toByteArray();
                return bytes;
            } catch(Exception e) {
                return null;
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                } catch (IOException ex) {
                    // ignore close exception
                }
                try {
                    bos.close();
                } catch (IOException ex) {
                    // ignore close exception
                }
            }
        }
        throw new RuntimeException("Fail to convert object to bytes");
    }

    public static String convertToString(byte[] bytes){
        return  new String(bytes);
    }

}
