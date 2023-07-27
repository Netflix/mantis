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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Observer;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.internal.operators.NotificationLite;
import rx.subscriptions.Subscriptions;


@SuppressWarnings("unchecked")
public class BufferOnBackPressureOperator<T> implements Operator<T, T> {

    public static final String METRICS_NAME_PREFIX = "DropOperator_";
    private static final Logger logger = LoggerFactory.getLogger(BufferOnBackPressureOperator.class);
    private static final int DEFAULT_SIZE = 4096;
    private final int size;
    private final ArrayBlockingQueue<Object> queue;
    private final Counter next;
    private final Counter error;
    private final Counter complete;
    private final Gauge subscribe;
    private AtomicLong subscribedValue = new AtomicLong(0);
    private final Gauge requestedGauge;
    private AtomicLong requestedValue = new AtomicLong(0);
    private final Counter dropped;
    private final Gauge bufferedGauge;
    private AtomicLong bufferedValue = new AtomicLong(0);
    private String name;
    public BufferOnBackPressureOperator(MeterRegistry meterRegistry,String name) {
        this(meterRegistry, name, DEFAULT_SIZE);
    }

    public BufferOnBackPressureOperator(MeterRegistry meterRegistry, int size) {
        this.size = size;
        this.queue = new ArrayBlockingQueue<Object>(size);

        next = Counter.builder("onNext").register(meterRegistry);
        error = Counter.builder("onError").register(meterRegistry);
        complete = Counter.builder("onComplete").register(meterRegistry);
        subscribe = Gauge.builder("subscribe", subscribedValue::get).register(meterRegistry);
        dropped = Counter.builder("dropped").register(meterRegistry);
        requestedGauge = Gauge.builder("requested", requestedValue::get).register(meterRegistry);
        bufferedGauge = Gauge.builder("bufferedGauge", bufferedValue::get).register(meterRegistry);

    }


    public BufferOnBackPressureOperator(MeterRegistry meterRegistry, String name, int size) {
        this.size = size;
        this.name = METRICS_NAME_PREFIX + name;
        this.queue = new ArrayBlockingQueue<Object>(size);

        next = Counter.builder(name + "-" + "" + Counters.onNext)
            .register(meterRegistry);
        error = Counter.builder(name + "-" + "" + Counters.onError)
            .register(meterRegistry);
        complete = Counter.builder(name + "-" + "" + Counters.onComplete)
            .register(meterRegistry);
        subscribe = Gauge.builder(name + "-" + "" + Gauges.subscribe, subscribedValue::get)
            .register(meterRegistry);
        dropped = Counter.builder(name + "-" + "" + Counters.dropped)
            .register(meterRegistry);
        requestedGauge = Gauge.builder(name + "-" + "" + Gauges.requested, requestedValue::get)
            .register(meterRegistry);
        bufferedGauge = Gauge.builder(name + "-" + "" + Gauges.bufferedGauge, bufferedValue::get)
            .register(meterRegistry);

    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super T> child) {

        subscribedValue.incrementAndGet();
        final AtomicLong requested = new AtomicLong();
        final AtomicInteger completionEmitted = new AtomicInteger();
        final AtomicInteger terminated = new AtomicInteger();

        final AtomicInteger bufferedCount = new AtomicInteger();
        final AtomicBoolean onCompleteReceived = new AtomicBoolean();

        final AtomicInteger wip = new AtomicInteger();
        child.add(Subscriptions.create(new Action0() {
            @Override
            public void call() {
                subscribedValue.decrementAndGet();
            }
        }));

        child.setProducer(new Producer() {

            @Override
            public void request(long n) {
                requested.getAndAdd(n);
                requestedValue.addAndGet(n);

                //         System.out.println("request: " + requested.get());
                pollQueue(child,
                        requested,

                        bufferedCount,
                        onCompleteReceived,
                        completionEmitted,
                        wip);
            }

        });

        Subscriber<T> parent = new Subscriber<T>() {
            @Override
            public void onStart() {
                request(Long.MAX_VALUE);
            }

            @Override
            public void onCompleted() {
                if (terminated.compareAndSet(0, 1)) {
                    complete.increment();
                    onCompleteReceived.set(true);
                    pollQueue(child,
                            requested,

                            bufferedCount,
                            onCompleteReceived,
                            completionEmitted,
                            wip);
                }
            }

            @Override
            public void onError(Throwable e) {
                if (terminated.compareAndSet(0, 1)) {
                    child.onError(e);
                    error.increment();
                    queue.clear();
                }
            }

            @Override
            public void onNext(T t) {
                emitItem(NotificationLite.next(t));
            }

            private void emitItem(Object item) {
                // short circuit buffering
                if (requested.get() > 0 && queue.isEmpty()) {
                    NotificationLite.accept((Observer) child, item);
                    requested.decrementAndGet();
                    requestedValue.decrementAndGet();
                    next.increment();
                    //		System.out.println("next count: " + next.value());
                } else {
                    boolean success = queue.offer(item);
                    if (success) {
                        bufferedCount.incrementAndGet();
                        bufferedValue.incrementAndGet();
                        //				System.out.println("buffered count: " + bufferedGauge.value());
                        drainIfPossible(child, requested, bufferedCount, onCompleteReceived, completionEmitted);

                    } else {
                        dropped.increment();
                        //			System.out.println("dropped count: " + dropped.value());
                        // dropped
                    }
                }
            }


        };
        // if child unsubscribes it should unsubscribe the parent, but not the other way around
        child.add(parent);
        return parent;
    }

    private void drainIfPossible(final Subscriber<? super T> child,
                                 AtomicLong requested,
                                 AtomicInteger bufferedCount,
                                 AtomicBoolean onCompleteReceived,
                                 AtomicInteger completionEmitted
    ) {
        while (requested.get() > 0) {
            Object t = queue.poll();
            if (t != null) {
                NotificationLite.accept((Observer) child, t);
                requested.decrementAndGet();
                requestedValue.decrementAndGet();
                bufferedCount.decrementAndGet();
                bufferedValue.decrementAndGet();
                //		System.out.println("buffered count: " + bufferedGauge.value() + " next " + next.value())  ;
            } else {
                if (onCompleteReceived.get()) {
                    if (completionEmitted.compareAndSet(0, 1)) {
                        child.onCompleted();
                        queue.clear();
                        bufferedValue.set(0);
                    }
                }
                // queue is empty break
                break;
            }
        }
    }

    private void pollQueue(final Subscriber<? super T> child,
                           AtomicLong requested,

                           AtomicInteger bufferedCount,
                           AtomicBoolean onCompleteReceived,
                           AtomicInteger completionEmitted,
                           AtomicInteger wip) {
        do {
            drainIfPossible(child, requested, bufferedCount, onCompleteReceived, completionEmitted);
            long c = wip.decrementAndGet();
            if (c > 1) {
                /*
                 * Set down to 1 and then iterate again.
                 * we lower it to 1 otherwise it could have grown very large while in the last poll loop
                 * and then we can end up looping all those times again here before existing even once we've drained
                 */
                wip.set(1);
                // we now loop again, and if anything tries scheduling again after this it will increment and cause us to loop again after
            }
        } while (wip.get() > 0);
    }

    public enum Counters {
        onNext,
        onError,
        onComplete,
        dropped
    }

    public enum Gauges {
        subscribe,
        requested,
        bufferedGauge
    }


}
