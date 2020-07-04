package com.spring2go.bigcache.utils;

import java.io.*;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class TestSample implements Serializable {
    private static final long serialVersionUID = 1L;
    public String stringA = "aaaaaaaaaa";
    public String stringB = "bbbbbbbbbb";
    public BuySell enumA = BuySell.Buy;
    public BuySell enumB = BuySell.Sell;
    public int intA = 123456;
    public int intB = 654321;
    public double doubleA = 1.23456789;
    public double doubleB = 9.87654321;
    public long longA = 987654321;
    public long longB = 123456789;

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            byte[] yourBytes = bos.toByteArray();
            return yourBytes;
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

    public static TestSample fromBytes(byte[] bytes) throws ClassNotFoundException, IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
            return (TestSample) o;
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    enum BuySell {
        Buy, Sell
    }

    public static String users(StringBuilder user, int i) {
        user.setLength(0);
        user.append("user:");
        user.append(i);
        return user.toString();
    }
}
