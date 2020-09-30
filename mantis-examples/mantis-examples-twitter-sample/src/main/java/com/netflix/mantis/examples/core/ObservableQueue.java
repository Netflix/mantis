/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.mantis.examples.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;


/**
 * An Observable that acts as a blocking queue. It is backed by a <code>Subject</code>
 *
 * @param <T>
 */
public class ObservableQueue<T> implements BlockingQueue<T>, Closeable {

    private final Subject<T, T> subject = PublishSubject.<T>create().toSerialized();

    public Observable<T> observe() {
        return subject;
    }

    @Override
    public boolean add(T t) {
        return offer(t);
    }

    @Override
    public boolean offer(T t) {
        subject.onNext(t);
        return true;
    }

    @Override
    public void close() throws IOException {
        subject.onCompleted();
    }

    @Override
    public T remove() {
        return noSuchElement();
    }

    @Override
    public T poll() {
        return null;
    }

    @Override
    public T element() {
        return noSuchElement();
    }

    private T noSuchElement() {
        throw new NoSuchElementException();
    }

    @Override
    public T peek() {
        return null;
    }

    @Override
    public void put(T t) throws InterruptedException {
        offer(t);
    }

    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(t);
    }

    @Override
    public T take() throws InterruptedException {
        throw new UnsupportedOperationException("Use observe() instead");
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        c.forEach(this::offer);
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return a;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        return 0;
    }

}