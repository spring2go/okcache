package com.spring2go.lrucache.v2;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LruCache实现，V1版本，线程安全
 *
 * Created on Jul, 2020 by @author bobo
 */
public class LruCacheV2<K, V> {
    private int maxCapacity;
    private Map<K, Node<K, V>> map;
    private Node<K, V> head, tail;

    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock writeLock = lock.writeLock();
    private Lock readLock = lock.readLock();

    private static class Node<K, V> {
        private V value;
        private K key;
        private Node<K, V> next, prev;

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    // 从双向链表中移除一个节点
    private void removeNode(Node<K, V> node) {
        if (node == null) return;

        if (node.prev != null) {
            node.prev.next = node.next;
        } else {
            head = node.next;
        }

        if (node.next != null) {
            node.next.prev = node.prev;
        } else {
            tail = node.prev;
        }
    }

    // 向双向链表的尾部添加一个节点
    private void offerNode(Node<K, V> node) {
        if (node == null) return;

        if (head == null) {
            head = tail = node;
        } else {
            tail.next = node;
            node.prev = tail;
            node.next = null;
            tail = node;
        }
    }

    public LruCacheV2(final int maxCapacity) {
        this.maxCapacity = maxCapacity;
        map = new HashMap<>();
    }

    public void put(K key, V value) {
        writeLock.lock();
        try {
            if (map.containsKey(key)) {
                Node<K, V> node = map.get(key);
                node.value = value;
                removeNode(node);
                offerNode(node);
            } else {
                if (map.size() >= maxCapacity) {
                    map.remove(head.key);
                    removeNode(head);
                }
                Node<K, V> node = new Node<>(key, value);
                offerNode(node);
                map.put(key, node);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public V get(K key) {
        writeLock.lock();
        try {
            Node<K, V> node = map.get(key);
            if (node == null) return null;
            removeNode(node);
            offerNode(node);
            return node.value;
        } finally {
            writeLock.unlock();
        }
    }

    public int size() {
        readLock.lock();
        try {
            return map.size();
        } finally {
            readLock.unlock();
        }
    }
}
