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
package io.reactivx.mantis.operators;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Operator;
import rx.Observer;
import rx.Producer;
import rx.Subscriber;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.internal.operators.NotificationLite;
import rx.observables.GroupedObservable;
import rx.subjects.Subject;


/**
 * Groups the items emitted by an Observable according to a specified criterion, and emits these
 * grouped items as Observables, one Observable per group.
 * <p>
 * <img width="640" height="360" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/groupBy.png" alt="">
 *
 * @param <K>
 *            the key type
 * @param <T>
 *            the source and group value type
 * @param <R>
 *            the value type of the groups
 */
public class OperatorGroupBy<T, K, R> implements Operator<GroupedObservable<K, R>, T> {

    private final static Func1<Object, Object> IDENTITY = new Func1<Object, Object>() {
        @Override
        public Object call(Object t) {
            return t;
        }
    };
    final Func1<? super T, ? extends K> keySelector;
    final Func1<? super T, ? extends R> valueSelector;

    @SuppressWarnings("unchecked")
    public OperatorGroupBy(final Func1<? super T, ? extends K> keySelector) {
        this(keySelector, (Func1<T, R>) IDENTITY);
    }

    public OperatorGroupBy(
            Func1<? super T, ? extends K> keySelector,
            Func1<? super T, ? extends R> valueSelector) {
        this.keySelector = keySelector;
        this.valueSelector = valueSelector;
    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super GroupedObservable<K, R>> child) {
        return new GroupBySubscriber<K, T, R>(keySelector, valueSelector, child);
    }

    static final class GroupBySubscriber<K, T, R> extends Subscriber<T> {

        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<GroupBySubscriber> COMPLETION_EMITTED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(GroupBySubscriber.class, "completionEmitted");
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<GroupBySubscriber> TERMINATED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(GroupBySubscriber.class, "terminated");
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<GroupBySubscriber> REQUESTED = AtomicLongFieldUpdater.newUpdater(GroupBySubscriber.class, "requested");
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<GroupBySubscriber> BUFFERED_COUNT = AtomicLongFieldUpdater.newUpdater(GroupBySubscriber.class, "bufferedCount");
        private static final int MAX_QUEUE_SIZE = 1024;
        final GroupBySubscriber<K, T, R> self = this;
        final Func1<? super T, ? extends K> keySelector;
        final Func1<? super T, ? extends R> elementSelector;
        final Subscriber<? super GroupedObservable<K, R>> child;
        private final ConcurrentHashMap<K, GroupState<K, T>> groups = new ConcurrentHashMap<K, GroupState<K, T>>();
        volatile int completionEmitted;
        volatile int terminated;
        volatile long requested;
        volatile long bufferedCount;

        public GroupBySubscriber(
                Func1<? super T, ? extends K> keySelector,
                Func1<? super T, ? extends R> elementSelector,
                Subscriber<? super GroupedObservable<K, R>> child) {
            super();
            this.keySelector = keySelector;
            this.elementSelector = elementSelector;
            this.child = child;
        }

        @Override
        public void onStart() {
            REQUESTED.set(this, MAX_QUEUE_SIZE);
            request(MAX_QUEUE_SIZE);
        }

        @Override
        public void onCompleted() {
            if (TERMINATED_UPDATER.compareAndSet(this, 0, 1)) {
                // if we receive onCompleted from our parent we onComplete children
                // for each group check if it is ready to accept more events if so pass the oncomplete through else buffer it.
                for (GroupState<K, T> group : groups.values()) {
                    emitItem(group, NotificationLite.completed());
                }

                // special case (no groups emitted ... or all unsubscribed)
                if (groups.size() == 0) {
                    // we must track 'completionEmitted' seperately from 'completed' since `completeInner` can result in childObserver.onCompleted() being emitted
                    if (COMPLETION_EMITTED_UPDATER.compareAndSet(this, 0, 1)) {
                        child.onCompleted();
                    }
                }
            }
        }

        @Override
        public void onError(Throwable e) {
            if (TERMINATED_UPDATER.compareAndSet(this, 0, 1)) {
                // we immediately tear everything down if we receive an error
                child.onError(e);
            }
        }

        void requestFromGroupedObservable(long n, GroupState<K, T> group) {
            group.requested.getAndAdd(n);
            if (group.count.getAndIncrement() == 0) {
                pollQueue(group);
            }
        }

        // The grouped observable propagates the 'producer.request' call from it's subscriber to this method
        // Here we keep track of the requested count for each group
        // If we already have items queued when a request comes in we vend those and decrement the outstanding request count

        @Override
        public void onNext(T t) {
            try {
                final K key = keySelector.call(t);
                GroupState<K, T> group = groups.get(key);
                if (group == null) {
                    // this group doesn't exist
                    if (child.isUnsubscribed()) {
                        // we have been unsubscribed on the outer so won't send any  more groups
                        return;
                    }
                    group = createNewGroup(key);
                }
                emitItem(group, NotificationLite.next(t));
            } catch (Throwable e) {
                onError(OnErrorThrowable.addValueAsLastCause(e, t));
            }
        }

        private GroupState<K, T> createNewGroup(final K key) {
            final GroupState<K, T> groupState = new GroupState<K, T>();

            GroupedObservable<K, R> go = GroupedObservable.create(key, new OnSubscribe<R>() {

                @Override
                public void call(final Subscriber<? super R> o) {
                    o.setProducer(new Producer() {

                        @Override
                        public void request(long n) {
                            requestFromGroupedObservable(n, groupState);
                        }

                    });

                    final AtomicBoolean once = new AtomicBoolean();

                    groupState.getObservable().doOnUnsubscribe(new Action0() {

                        @Override
                        public void call() {
                            if (once.compareAndSet(false, true)) {
                                // done once per instance, either onComplete or onUnSubscribe
                                cleanupGroup(key);
                            }
                        }

                    }).unsafeSubscribe(new Subscriber<T>(o) {

                        @Override
                        public void onCompleted() {
                            o.onCompleted();
                            // eagerly cleanup instead of waiting for unsubscribe
                            if (once.compareAndSet(false, true)) {
                                // done once per instance, either onComplete or onUnSubscribe
                                cleanupGroup(key);
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            o.onError(e);
                        }

                        @Override
                        public void onNext(T t) {
                            try {
                                o.onNext(elementSelector.call(t));
                            } catch (Throwable e) {
                                onError(OnErrorThrowable.addValueAsLastCause(e, t));
                            }
                        }
                    });
                }
            });

            GroupState<K, T> putIfAbsent = groups.putIfAbsent(key, groupState);
            if (putIfAbsent != null) {
                // this shouldn't happen (because we receive onNext sequentially) and would mean we have a bug
                throw new IllegalStateException("Group already existed while creating a new one");
            }
            child.onNext(go);

            return groupState;
        }

        private void cleanupGroup(K key) {
            GroupState<K, T> removed;
            removed = groups.remove(key);
            if (removed != null) {
                if (removed.buffer.size() > 0) {
                    BUFFERED_COUNT.addAndGet(self, -removed.buffer.size());
                }
                completeInner();
                // since we may have unsubscribed early with items in the buffer
                // we remove those above and have freed up room to request more
                // so give it a chance to request more now
                requestMoreIfNecessary();
            }
        }

        private void emitItem(GroupState<K, T> groupState, Object item) {
            Queue<Object> q = groupState.buffer;
            AtomicLong keyRequested = groupState.requested;
            REQUESTED.decrementAndGet(this);
            // short circuit buffering
            if (keyRequested != null && keyRequested.get() > 0 && (q == null || q.isEmpty())) {
                @SuppressWarnings("unchecked")
                Observer<Object> obs = (Observer<Object>) groupState.getObserver();
                NotificationLite.accept(obs, item);
                keyRequested.decrementAndGet();
            } else {
                q.add(item);
                BUFFERED_COUNT.incrementAndGet(this);

                if (groupState.count.getAndIncrement() == 0) {
                    pollQueue(groupState);
                }
            }
            requestMoreIfNecessary();
        }

        private void pollQueue(GroupState<K, T> groupState) {
            do {
                drainIfPossible(groupState);
                long c = groupState.count.decrementAndGet();
                if (c > 1) {

                    /*
                     * Set down to 1 and then iterate again.
                     * we lower it to 1 otherwise it could have grown very large while in the last poll loop
                     * and then we can end up looping all those times again here before existing even once we've drained
                     */
                    groupState.count.set(1);
                    // we now loop again, and if anything tries scheduling again after this it will increment and cause us to loop again after
                }
            } while (groupState.count.get() > 0);
        }

        private void requestMoreIfNecessary() {
            if (REQUESTED.get(this) < (MAX_QUEUE_SIZE) && terminated == 0) {
                long requested = REQUESTED.get(this);
                long toRequest = MAX_QUEUE_SIZE - REQUESTED.get(this) - BUFFERED_COUNT.get(this);
                if (toRequest > 0 && REQUESTED.compareAndSet(this, requested, toRequest + requested)) {
                    request(toRequest);
                }
            }
        }

        private void drainIfPossible(GroupState<K, T> groupState) {
            while (groupState.requested.get() > 0) {
                Object t = groupState.buffer.poll();
                if (t != null) {
                    @SuppressWarnings("unchecked")
                    Observer<Object> obs = (Observer<Object>) groupState.getObserver();
                    NotificationLite.accept(obs, t);
                    groupState.requested.decrementAndGet();
                    BUFFERED_COUNT.decrementAndGet(this);

                    // if we have used up all the events we requested from upstream then figure out what to ask for this time based on the empty space in the buffer
                    requestMoreIfNecessary();
                } else {
                    // queue is empty break
                    break;
                }
            }
        }

        private void completeInner() {
            // if we have no outstanding groups (all completed or unsubscribe) and terminated/unsubscribed on outer
            if (groups.size() == 0 && (terminated == 1 || child.isUnsubscribed())) {
                // completionEmitted ensures we only emit onCompleted once
                if (COMPLETION_EMITTED_UPDATER.compareAndSet(this, 0, 1)) {

                    if (child.isUnsubscribed()) {
                        // if the entire groupBy has been unsubscribed and children are completed we will propagate the unsubscribe up.
                        unsubscribe();
                    }
                    child.onCompleted();
                }
            }
        }

        private static class GroupState<K, T> {

            private final Subject<T, T> s = BufferUntilSubscriber.create();
            private final AtomicLong requested = new AtomicLong();
            private final AtomicLong count = new AtomicLong();
            private final Queue<Object> buffer = new ConcurrentLinkedQueue<Object>(); // TODO should this be lazily created?

            public Observable<T> getObservable() {
                return s;
            }

            public Observer<T> getObserver() {
                return s;
            }

        }

    }

}
