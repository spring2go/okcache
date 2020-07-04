package com.spring2go.okcache;

import java.io.*;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class JavaSerializer implements Serializer {
    public byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.flush();
        return bos.toByteArray();
    }

    public Object deserialize(byte[] value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return ois.readObject();
    }

    @Override
    public <K extends Serializable> K deserialize(byte[] value, Class<K> clazz) throws Exception {
        return (K)deserialize(value);
    }
}
