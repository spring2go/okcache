package com.spring2go.okcache;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created on Jul, 2020 by @author bobo
 */
public abstract class AbstractDeque<E> implements Deque<E> {
    protected AbstractDeque() {
    }

    @Override
    public void addFirst(E e) {
        if (!offerFirst(e))
            throw new IllegalStateException("Queue full");

    }

    @Override
    public void addLast(E e) {
        if (!offerLast(e))
            throw new IllegalStateException("Queue full");
    }

    @Override
    public E removeFirst() {
        E x = pollFirst();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    @Override
    public E removeLast() {
        E x = pollLast();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    @Override
    public E getFirst() {
        E x = peekFirst();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    @Override
    public E getLast() {
        E x = peekLast();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(E e) {
        if (offer(e))
            return true;
        else
            throw new IllegalStateException("Queue full");
    }

    @Override
    public boolean offer(E e) {
        return offerLast(e);
    }

    @Override
    public E remove() {
        E x = poll();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    @Override
    public E poll() {
        return pollFirst();
    }

    @Override
    public E element() {
        E x = peek();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    @Override
    public E peek() {
        return peekFirst();
    }

    @Override
    public void push(E e) {
        addFirst(e);
    }

    @Override
    public E pop() {
        return removeFirst();
    }

    @Override
    public boolean remove(Object o) {
        return removeFirstOccurrence(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        while (poll() != null)
            ;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> descendingIterator() {
        throw new UnsupportedOperationException();
    }
}
