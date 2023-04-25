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

package io.reactivex.mantis.network.push;

import io.mantisrx.common.MantisGroup;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.reactivx.mantis.operators.DisableBackPressureOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.GroupedObservable;
import rx.schedulers.Schedulers;


public final class ObservableTrigger {

    private static final Logger logger = LoggerFactory.getLogger(ObservableTrigger.class);

    private static Scheduler timeoutScheduler = Schedulers.from(Executors.newFixedThreadPool(5));

    private ObservableTrigger() {}


    private static <T> PushTrigger<T> trigger(final String name, final Observable<T> o, final Action0 doOnComplete,
                                              final Action1<Throwable> doOnError) {
        final AtomicReference<Subscription> subRef = new AtomicReference<>();
        final Gauge subscriptionActive;

        Metrics metrics = new Metrics.Builder()
                .name("ObservableTrigger_" + name)
                .addGauge("subscriptionActive")
                .build();

        subscriptionActive = metrics.getGauge("subscriptionActive");

        // Share the Observable so we don't create a new Observable on every subscription.
        final Observable<T> sharedO = o.share();
        Action1<MonitoredQueue<T>> doOnStart = queue -> {
            Subscription oldSub = subRef.getAndSet(
                sharedO
                    .filter((T t1) -> t1 != null)
                    .doOnSubscribe(() -> {
                        logger.info("Subscription is ACTIVE for observable trigger with name: " + name);
                        subscriptionActive.increment();
                    })
                    .doOnUnsubscribe(() -> {
                        logger.info("Subscription is INACTIVE for observable trigger with name: " + name);
                        subscriptionActive.decrement();
                    })
                    .subscribe(
                        (T data) -> queue.write(data),
                        (Throwable e) -> {
                            logger.warn("Observable used to push data errored, on server with name: " + name, e);
                            if (doOnError != null) {
                                doOnError.call(e);
                            }
                        },
                        () -> {
                            logger.info("Observable used to push data completed, on server with name: " + name);
                            if (doOnComplete != null) {
                                doOnComplete.call();
                            }
                        }
                    )
            );
            if (oldSub != null) {
                logger.info("A new subscription is ACTIVE. " +
                    "Unsubscribe from previous subscription observable trigger with name: " + name);
                oldSub.unsubscribe();
            }
        };

        Action1<MonitoredQueue<T>> doOnStop = t1 -> {
            if (subRef.get() != null) {
                logger.warn("Connections from next stage has dropped to 0. Do not propagate unsubscribe");
                //	subRef.get().unsubscribe();
            }
        };

        return new PushTrigger<>(doOnStart, doOnStop, metrics);
    }

    private static <T> PushTrigger<T> ssetrigger(final String name, final Observable<T> o, final Action0 doOnComplete,
                                                 final Action1<Throwable> doOnError) {
        final AtomicReference<Subscription> subRef = new AtomicReference<>();
        final Gauge subscriptionActive;

        Metrics metrics = new Metrics.Builder()
                .name("ObservableTrigger_" + name)
                .addGauge("subscriptionActive")
                .build();

        subscriptionActive = metrics.getGauge("subscriptionActive");

        Action1<MonitoredQueue<T>> doOnStart = queue -> subRef.set(
                o
                        .filter((T t1) -> t1 != null)
                        .doOnSubscribe(() -> {
                            logger.info("Subscription is ACTIVE for observable trigger with name: " + name);
                            subscriptionActive.increment();
                        })
                        .doOnUnsubscribe(() -> {
                            logger.info("Subscription is INACTIVE for observable trigger with name: " + name);
                            subscriptionActive.decrement();
                        })
                        .subscribe(
                            (T data) -> queue.write(data),
                            (Throwable e) -> {
                                logger.warn("Observable used to push data errored, on server with name: " + name, e);
                                if (doOnError != null) {
                                    doOnError.call(e);
                                }
                            },
                            () -> {
                                logger.info("Observable used to push data completed, on server with name: " + name);
                                if (doOnComplete != null) {
                                    doOnComplete.call();
                                }
                            }
                        )
        );

        Action1<MonitoredQueue<T>> doOnStop = t1 -> {
            if (subRef.get() != null) {
                logger.warn("Connections from next stage has dropped to 0 for SSE stage. propagate unsubscribe");
                subRef.get().unsubscribe();
            }
        };

        return new PushTrigger<>(doOnStart, doOnStop, metrics);
    }


    private static <K, V> PushTrigger<KeyValuePair<K, V>> groupTrigger(final String name, final Observable<GroupedObservable<K, V>> o, final Action0 doOnComplete,
                                                                       final Action1<Throwable> doOnError, final long groupExpirySeconds, final Func1<K, byte[]> keyEncoder, final HashFunction hashFunction) {
        final AtomicReference<Subscription> subRef = new AtomicReference<>();
        final Gauge subscriptionActive;

        Metrics metrics = new Metrics.Builder()
                .name("ObservableTrigger_" + name)
                .addGauge("subscriptionActive")
                .build();

        subscriptionActive = metrics.getGauge("subscriptionActive");

        // Share the Observable so we don't create a new Observable on every subscription.
        final Observable<GroupedObservable<K, V>> sharedO = o.share();
        Action1<MonitoredQueue<KeyValuePair<K, V>>> doOnStart = queue -> {
            Subscription oldSub = subRef.getAndSet(
                sharedO
                    .doOnSubscribe(() -> {
                        logger.info("Subscription is ACTIVE for observable trigger with name: " + name);
                        subscriptionActive.increment();
                    })
                    .doOnUnsubscribe(() -> {
                        logger.info("Subscription is INACTIVE for observable trigger with name: " + name);
                        subscriptionActive.decrement();
                    })
                    .flatMap((final GroupedObservable<K, V> group) -> {
                            final byte[] keyBytes = keyEncoder.call(group.getKey());
                            final long keyBytesHashed = hashFunction.computeHash(keyBytes);
                            return
                                group
                                    .timeout(groupExpirySeconds, TimeUnit.SECONDS, Observable.empty(), timeoutScheduler)
                                    .lift(new DisableBackPressureOperator<V>())
                                    .buffer(250, TimeUnit.MILLISECONDS)
                                    .filter((List<V> t1) -> t1 != null && !t1.isEmpty())
                                    .map((List<V> list) -> {
                                            List<KeyValuePair<K, V>> keyPairList = new ArrayList<>(list.size());
                                            for (V data : list) {
                                                keyPairList.add(new KeyValuePair<>(keyBytesHashed, keyBytes, data));
                                            }
                                            return keyPairList;
                                        }
                                    );
                        }
                    )
                    .subscribe(
                        (List<KeyValuePair<K, V>> list) -> {
                            for (KeyValuePair<K, V> data : list) {
                                queue.write(data);
                            }
                        },
                        (Throwable e) -> {
                            logger.warn("Observable used to push data errored, on server with name: " + name, e);
                            if (doOnError != null) {
                                doOnError.call(e);
                            }
                        },
                        () -> {
                            logger.info("Observable used to push data completed, on server with name: " + name);
                            if (doOnComplete != null) {
                                doOnComplete.call();
                            }
                        }
                    )
            );
            if (oldSub != null) {
                logger.info("A new subscription is ACTIVE. " +
                    "Unsubscribe from previous subscription observable trigger with name: " + name);
                oldSub.unsubscribe();
            }
        };

        Action1<MonitoredQueue<KeyValuePair<K, V>>> doOnStop = t1 -> {
            if (subRef.get() != null) {
                logger.warn("Connections from next stage has dropped to 0. " +
                    "Do not propagate unsubscribe until a new connection is made.");
                //subRef.get().unsubscribe();
            }
        };

        return new PushTrigger<>(doOnStart, doOnStop, metrics);
    }


    private static <K, V> PushTrigger<KeyValuePair<K, V>> mantisGroupTrigger(final String name, final Observable<MantisGroup<K, V>> o, final Action0 doOnComplete,
                                                                             final Action1<Throwable> doOnError, final long groupExpirySeconds, final Func1<K, byte[]> keyEncoder, final HashFunction hashFunction) {
        final AtomicReference<Subscription> subRef = new AtomicReference<>();
        final Gauge subscriptionActive;

        Metrics metrics = new Metrics.Builder()
                .name("ObservableTrigger_" + name)
                .addGauge("subscriptionActive")
                .build();

        subscriptionActive = metrics.getGauge("subscriptionActive");

        // Share the Observable so we don't create a new Observable on every subscription.
        final Observable<MantisGroup<K, V>> sharedO = o.share();
        Action1<MonitoredQueue<KeyValuePair<K, V>>> doOnStart = queue -> {
            Subscription oldSub = subRef.getAndSet(
                sharedO
                    .doOnSubscribe(() -> {
                        logger.info("Subscription is ACTIVE for observable trigger with name: " + name);
                        subscriptionActive.increment();
                    })
                    .doOnUnsubscribe(() -> {
                        logger.info("Subscription is INACTIVE for observable trigger with name: " + name);
                        subscriptionActive.decrement();
                    })
                    .map((MantisGroup<K, V> data) -> {
                        final byte[] keyBytes = keyEncoder.call(data.getKeyValue());
                        final long keyBytesHashed = hashFunction.computeHash(keyBytes);
                        return (new KeyValuePair<K, V>(keyBytesHashed, keyBytes, data.getValue()));
                    })
                    .subscribe(
                        (KeyValuePair<K, V> data) -> queue.write(data),
                        (Throwable e) -> {
                            logger.warn("Observable used to push data errored, on server with name: " + name, e);
                            if (doOnError != null) {
                                doOnError.call(e);
                            }
                        },
                        () -> {
                            logger.info("Observable used to push data completed, on server with name: " + name);
                            if (doOnComplete != null) {
                                doOnComplete.call();
                            }
                        }
                    )
            );
            if (oldSub != null) {
                logger.info("A new subscription is ACTIVE. " +
                    "Unsubscribe from previous subscription observable trigger with name: " + name);
                oldSub.unsubscribe();
            }
        };

        Action1<MonitoredQueue<KeyValuePair<K, V>>> doOnStop = t1 -> {
            if (subRef.get() != null) {
                logger.warn("Connections from next stage has dropped to 0. " +
                    "Do not propagate unsubscribe until a new connection is made.");
                //	subRef.get().unsubscribe();
            }
        };

        return new PushTrigger<>(doOnStart, doOnStop, metrics);
    }


    public static <T> PushTrigger<T> o(String name, final Observable<T> o,
                                       Action0 doOnComplete,
                                       Action1<Throwable> doOnError) {
        return ssetrigger(name, o, doOnComplete, doOnError);
    }

    public static <T> PushTrigger<T> oo(String name, final Observable<Observable<T>> oo,
                                        Action0 doOnComplete,
                                        Action1<Throwable> doOnError) {
        return trigger(name, Observable.merge(oo), doOnComplete, doOnError);
    }

    public static <K, V> PushTrigger<KeyValuePair<K, V>> oogo(String name, final Observable<Observable<GroupedObservable<K, V>>> oo,
                                                              Action0 doOnComplete,
                                                              Action1<Throwable> doOnError,
                                                              long groupExpirySeconds,
                                                              final Func1<K, byte[]> keyEncoder,
                                                              HashFunction hashFunction) {
        return groupTrigger(name, Observable.merge(oo), doOnComplete, doOnError, groupExpirySeconds, keyEncoder, hashFunction);
    }

    // NJ
    public static <K, V> PushTrigger<KeyValuePair<K, V>> oomgo(String name, final Observable<Observable<MantisGroup<K, V>>> oo,
                                                               Action0 doOnComplete,
                                                               Action1<Throwable> doOnError,
                                                               long groupExpirySeconds,
                                                               final Func1<K, byte[]> keyEncoder,
                                                               HashFunction hashFunction) {
        return mantisGroupTrigger(name, Observable.merge(oo), doOnComplete, doOnError, groupExpirySeconds, keyEncoder, hashFunction);
    }
}
