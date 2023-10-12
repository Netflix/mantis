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

package io.reactivex.mantis.remote.observable;

import io.mantisrx.common.codec.Encoder;
import io.mantisrx.common.network.HashFunctions;
import io.mantisrx.server.core.ServiceRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.reactivex.mantis.remote.observable.filter.ServerSideFilters;
import io.reactivex.mantis.remote.observable.slotting.ConsistentHashing;
import io.reactivex.mantis.remote.observable.slotting.SlottingStrategy;
import io.reactivx.mantis.operators.DisableBackPressureOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Notification;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.GroupedObservable;


public class ServeGroupedObservable<K, V> extends ServeConfig<K, Group<String, V>> {

    private static final Logger logger = LoggerFactory.getLogger(ServeGroupedObservable.class);

    private Encoder<String> keyEncoder;
    private Encoder<V> valueEncoder;
    private int groupBufferTimeMSec = 250;
    private long expiryInSecs = Long.MAX_VALUE;
    private Counter groupsExpiredCounter;
    private MeterRegistry meterRegistry;

    public ServeGroupedObservable(Builder<K, V> builder, MeterRegistry meterRegistry) {
        super(builder.name, builder.slottingStrategy, builder.filterFunction,
                builder.maxWriteAttempts);
        this.meterRegistry = meterRegistry;

        // TODO this should be pushed into builder, default is 0 buffer
        String groupBufferTimeMSecStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.remoteObservable.groupBufferMSec", "250");
        if (groupBufferTimeMSecStr != null && !groupBufferTimeMSecStr.equals("250")) {
            groupBufferTimeMSec = Integer.parseInt(groupBufferTimeMSecStr);
        }

        this.keyEncoder = builder.keyEncoder;
        this.valueEncoder = builder.valueEncoder;
        this.expiryInSecs = builder.expiryTimeInSecs;

        groupsExpiredCounter = meterRegistry.counter("ServeGroupedObservable_groupsExpiredCounter");

        applySlottingSideEffectToObservable(builder.observable, builder.minConnectionsToSubscribe);
    }

    private void applySlottingSideEffectToObservable(
            Observable<Observable<GroupedObservable<String, V>>> o,
            final Observable<Integer> minConnectionsToSubscribe) {

        final AtomicInteger currentMinConnectionsToSubscribe = new AtomicInteger();
        minConnectionsToSubscribe
                .subscribe(new Action1<Integer>() {
                    @Override
                    public void call(Integer t1) {
                        currentMinConnectionsToSubscribe.set(t1);
                    }
                });

        Observable<Observable<List<Group<String, V>>>> listOfGroups = o
                .map(new Func1<Observable<GroupedObservable<String, V>>, Observable<List<Group<String, V>>>>() {
                    @Override
                    public Observable<List<Group<String, V>>> call(
                            Observable<GroupedObservable<String, V>> og) {
                        return
                                og
                                        .flatMap(new Func1<GroupedObservable<String, V>, Observable<List<Group<String, V>>>>() {
                                            @Override
                                            public Observable<List<Group<String, V>>> call(
                                                    final GroupedObservable<String, V> group) {
                                                final byte[] keyBytes = keyEncoder.encode(group.getKey());
                                                final String keyValue = group.getKey();
                                                return
                                                        group
                                                                // comment out as this causes a NPE to happen in merge. supposedly fixed in rxjava 1.0
                                                                .doOnUnsubscribe(new Action0() {
                                                                    @Override
                                                                    public void call() {
                                                                        //logger.info("Expiring group stage in serveGroupedObservable " + group.getKey());
                                                                        groupsExpiredCounter.increment();
                                                                    }
                                                                })
                                                                .timeout(expiryInSecs, TimeUnit.SECONDS, (Observable<? extends V>) Observable.empty())
                                                                .materialize()
                                                                .lift(new DisableBackPressureOperator<Notification<V>>())
                                                                .buffer(groupBufferTimeMSec, TimeUnit.MILLISECONDS)
                                                                .filter(new Func1<List<Notification<V>>, Boolean>() {
                                                                    @Override
                                                                    public Boolean call(List<Notification<V>> t1) {
                                                                        return t1 != null && !t1.isEmpty();
                                                                    }
                                                                })
                                                                .map(new Func1<List<Notification<V>>, List<Group<String, V>>>() {
                                                                    @Override
                                                                    public List<Group<String, V>> call(List<Notification<V>> notifications) {
                                                                        List<Group<String, V>> groups = new ArrayList<>(notifications.size());
                                                                        for (Notification<V> notification : notifications) {
                                                                            groups.add(new Group<String, V>(keyValue, keyBytes, notification));
                                                                        }
                                                                        return groups;
                                                                    }
                                                                });
                                            }
                                        });
                    }
                });
        final Observable<List<Group<String, V>>> withSideEffects =
                Observable
                        .merge(
                                listOfGroups

                        )
                        .doOnEach(new Observer<List<Group<String, V>>>() {
                            @Override
                            public void onCompleted() {
                                slottingStrategy.completeAllConnections();
                            }

                            @Override
                            public void onError(Throwable e) {
                                e.printStackTrace();
                                slottingStrategy.errorAllConnections(e);
                            }

                            @Override
                            public void onNext(List<Group<String, V>> listOfGroups) {
                                for (Group<String, V> group : listOfGroups) {
                                    slottingStrategy.writeOnSlot(group.getKeyBytes(), group);
                                }
                            }
                        });


        final MutableReference<Subscription> subscriptionRef = new MutableReference<>();
        final AtomicInteger connectionCount = new AtomicInteger(0);
        final AtomicBoolean isSubscribed = new AtomicBoolean();

        slottingStrategy.registerDoOnEachConnectionAdded(new Action0() {
            @Override
            public void call() {
                Integer minNeeded = currentMinConnectionsToSubscribe.get();
                Integer current = connectionCount.incrementAndGet();
                if (current >= minNeeded) {
                    if (isSubscribed.compareAndSet(false, true)) {
                        logger.info("MinConnectionsToSubscribe: " + minNeeded + ", has been met, subscribing to observable, current connection count: " + current);
                        subscriptionRef.setValue(withSideEffects.subscribe());
                    }
                } else {
                    logger.info("MinConnectionsToSubscribe: " + minNeeded + ", has NOT been met, current connection count: " + current);
                }
            }
        });

        slottingStrategy.registerDoAfterLastConnectionRemoved(new Action0() {
            @Override
            public void call() {
                subscriptionRef.getValue().unsubscribe();
                logger.info("All connections deregistered, unsubscribed to observable, resetting current connection count: 0");
                connectionCount.set(0);
                isSubscribed.set(false);
            }
        });

    }

    public Encoder<String> getKeyEncoder() {
        return keyEncoder;
    }

    public Encoder<V> getValueEncoder() {
        return valueEncoder;
    }

    public static class Builder<K, V> {

        private String name;
        private Observable<Observable<GroupedObservable<String, V>>> observable;
        private SlottingStrategy<Group<String, V>> slottingStrategy =
                new ConsistentHashing<>(name, HashFunctions.ketama());
        private Encoder<String> keyEncoder;
        private Encoder<V> valueEncoder;
        private Func1<Map<String, String>, Func1<K, Boolean>> filterFunction = ServerSideFilters.noFiltering();
        private int maxWriteAttempts = 3;
        private long expiryTimeInSecs = Long.MAX_VALUE;
        private Observable<Integer> minConnectionsToSubscribe = Observable.just(1);
        private MeterRegistry meterRegistry;

        public Builder<K, V> name(String name) {
            if (name != null && name.length() > 127) {
                throw new IllegalArgumentException("Observable name must be less than 127 characters");
            }
            this.name = name;
            return this;
        }

        public Builder<K, V> observable(Observable<Observable<GroupedObservable<String, V>>> observable) {
            this.observable = observable;
            return this;
        }

        public Builder<K, V> maxWriteAttempts(int maxWriteAttempts) {
            this.maxWriteAttempts = maxWriteAttempts;
            return this;
        }

        public Builder<K, V> withExpirySecs(long expiryInSecs) {
            this.expiryTimeInSecs = expiryInSecs;
            return this;
        }

        public Builder<K, V> minConnectionsToSubscribe(Observable<Integer> minConnectionsToSubscribe) {
            this.minConnectionsToSubscribe = minConnectionsToSubscribe;
            return this;
        }

        public Builder<K, V> slottingStrategy(SlottingStrategy<Group<String, V>> slottingStrategy) {
            this.slottingStrategy = slottingStrategy;
            return this;
        }

        public Builder<K, V> keyEncoder(Encoder<String> keyEncoder) {
            this.keyEncoder = keyEncoder;
            return this;
        }

        public Builder<K, V> valueEncoder(Encoder<V> valueEncoder) {
            this.valueEncoder = valueEncoder;
            return this;
        }

        public Builder<K, V> serverSideFilter(
                Func1<Map<String, String>, Func1<K, Boolean>> filterFunc) {
            this.filterFunction = filterFunc;
            return this;
        }

        public Builder<K, V> registry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            return this;
        }

        public ServeGroupedObservable<K, V> build() {
            return new ServeGroupedObservable<K, V>(this, meterRegistry);
        }
    }
}
