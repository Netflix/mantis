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

package io.reactivex.mantis.remote.observable.reconciliator;

import io.mantisrx.common.network.Endpoint;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.reactivex.mantis.remote.observable.DynamicConnectionSet;
import io.reactivex.mantis.remote.observable.EndpointChange;
import io.reactivex.mantis.remote.observable.EndpointChange.Type;
import io.reactivex.mantis.remote.observable.EndpointInjector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.PublishSubject;


public class Reconciliator<T> {

    private static final Logger logger = LoggerFactory.getLogger(Reconciliator.class);

    private static final AtomicBoolean startedReconciliation = new AtomicBoolean(false);
    private String name;
    private Subscription subscription;

    private DynamicConnectionSet<T> connectionSet;
    private PublishSubject<Set<Endpoint>> currentExpectedSet = PublishSubject.create();
    private EndpointInjector injector;
    private PublishSubject<EndpointChange> reconciledChanges = PublishSubject.create();
    private MeterRegistry meterRegistry;
//    private Metrics metrics;
    private Counter reconciliationCheck;
    private Gauge running;
    private AtomicLong runningValue = new AtomicLong(0);
    private Gauge expectedSetSize;
    private AtomicLong expectedSetSizeValue = new AtomicLong(0);

    Reconciliator(Builder<T> builder) {
        this.name = builder.name;
        this.injector = builder.injector;
        this.connectionSet = builder.connectionSet;

        reconciliationCheck = meterRegistry.counter("Reconciliator_" + name+ "reconciliationCheck");
        running = Gauge.builder("Reconciliator_" + name + "running", runningValue::get)
            .register(meterRegistry);
        expectedSetSize = Gauge.builder("Reconciliator_" + name + "expectedSetSize", expectedSetSizeValue::get)
            .register(meterRegistry);
    }

    public List<Meter> getMetrics() {
        List<Meter> meters = new ArrayList<>();
        meters.add(reconciliationCheck);
        meters.add(running);
        meters.add(expectedSetSize);
        return meters;
    }

    private Observable<EndpointChange> deltas() {

        final Map<String, Endpoint> sideEffectState = new HashMap<String, Endpoint>();
        final PublishSubject<Integer> stopReconciliator = PublishSubject.create();

        return
                Observable.merge(
                        reconciledChanges
                                .takeUntil(stopReconciliator)
                                .doOnCompleted(() -> {
                                    logger.info("onComplete triggered for reconciledChanges");
                                })
                                .doOnError(e -> logger.error("caught exception for reconciledChanges {}", e.getMessage(), e))
                        ,
                        injector
                                .deltas()
                                .doOnCompleted(new Action0() {
                                    @Override
                                    public void call() {
                                        // injector has completed recieving updates, complete reconciliator
                                        // observable
                                        logger.info("Stopping reconciliator, injector completed.");
                                        stopReconciliator.onNext(1);
                                        stopReconciliation();
                                    }
                                })
                                .doOnError(e -> logger.error("caught exception for injector deltas {}", e.getMessage(), e))
                                .doOnNext(new Action1<EndpointChange>() {
                                    @Override
                                    public void call(EndpointChange newEndpointChange) {
                                        String id = Endpoint.uniqueHost(newEndpointChange.getEndpoint().getHost(),
                                                newEndpointChange.getEndpoint().getPort(), newEndpointChange.getEndpoint().getSlotId());
                                        if (sideEffectState.containsKey(id)) {
                                            if (newEndpointChange.getType() == Type.complete) {
                                                // remove from expecected set
                                                expectedSetSizeValue.decrementAndGet();
                                                sideEffectState.remove(id);
                                                currentExpectedSet.onNext(new HashSet<Endpoint>(sideEffectState.values()));
                                            }
                                        } else {
                                            if (newEndpointChange.getType() == Type.add) {
                                                expectedSetSizeValue.incrementAndGet();
                                                sideEffectState.put(id, new Endpoint(newEndpointChange.getEndpoint().getHost(),
                                                        newEndpointChange.getEndpoint().getPort(), newEndpointChange.getEndpoint().getSlotId()));
                                                currentExpectedSet.onNext(new HashSet<Endpoint>(sideEffectState.values()));
                                            }
                                        }
                                    }
                                })
                )
                        .doOnError(t -> logger.error("caught error processing reconciliator deltas {}", t.getMessage(), t))
                        .doOnSubscribe(
                                new Action0() {
                                    @Override
                                    public void call() {
                                        logger.info("Subscribed to deltas for {}, clearing active connection set", name);
                                        connectionSet.resetActiveConnections();
                                        startReconciliation();
                                    }
                                })
                        .doOnUnsubscribe(new Action0() {
                            @Override
                            public void call() {
                                logger.info("Unsubscribed from deltas for {}", name);
                            }
                        });
    }

    private void startReconciliation() {
        if (startedReconciliation.compareAndSet(false, true)) {
            logger.info("Starting reconciliation for name: " + name);
            runningValue.incrementAndGet();
            subscription =
                    Observable
                            .combineLatest(currentExpectedSet, connectionSet.activeConnections(),
                                    new Func2<Set<Endpoint>, Set<Endpoint>, Void>() {
                                        @Override
                                        public Void call(Set<Endpoint> expected, Set<Endpoint> actual) {
                                            reconciliationCheck.increment();
                                            boolean check = expected.equals(actual);
                                            logger.debug("Check result: " + check + ", size expected: " + expected.size() + " actual: " + actual.size() + ", for values expected: " + expected + " versus actual: " + actual);
                                            if (!check) {
                                                // reconcile adds
                                                Set<Endpoint> expectedDiff = new HashSet<Endpoint>(expected);
                                                expectedDiff.removeAll(actual);
                                                if (expectedDiff.size() > 0) {
                                                    for (Endpoint endpoint : expectedDiff) {
                                                        logger.info("Connection missing from expected set, adding missing connection: " + endpoint);
                                                        reconciledChanges.onNext(new EndpointChange(Type.add, endpoint));
                                                    }
                                                }
                                                // reconile removes
                                                Set<Endpoint> actualDiff = new HashSet<Endpoint>(actual);
                                                actualDiff.removeAll(expected);
                                                if (actualDiff.size() > 0) {
                                                    for (Endpoint endpoint : actualDiff) {
                                                        logger.info("Unexpected connection in active set, removing connection: " + endpoint);
                                                        reconciledChanges.onNext(new EndpointChange(Type.complete, endpoint));
                                                    }
                                                }
                                            }
                                            return null;
                                        }
                                    })
                            .onErrorResumeNext(new Func1<Throwable, Observable<? extends Void>>() {
                                @Override
                                public Observable<? extends Void> call(Throwable throwable) {
                                    logger.error("caught error in Reconciliation for {}", name, throwable);
                                    return Observable.empty();
                                }
                            })
                            .doOnCompleted(new Action0() {
                                @Override
                                public void call() {
                                    logger.error("onComplete in Reconciliation observable chain for {}", name);
                                    stopReconciliation();
                                }
                            })
                            .subscribe();
        } else {
            logger.info("reconciliation already started for {}", name);
        }
    }

    private void stopReconciliation() {
        if (startedReconciliation.compareAndSet(true, false)) {
            logger.info("Stopping reconciliation for name: " + name);
            runningValue.decrementAndGet();
            subscription.unsubscribe();
        } else {
            logger.info("reconciliation already stopped for name: " + name);
        }
    }

    public Observable<Observable<T>> observables() {
        connectionSet.setEndpointInjector(new EndpointInjector() {
            @Override
            public Observable<EndpointChange> deltas() {
                return Reconciliator.this.deltas();
            }
        });
        return connectionSet.observables();
    }

    public static class Builder<T> {

        private String name;
        private EndpointInjector injector;
        private DynamicConnectionSet<T> connectionSet;

        public Builder<T> connectionSet(DynamicConnectionSet<T> connectionSet) {
            this.connectionSet = connectionSet;
            return this;
        }

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> injector(EndpointInjector injector) {
            this.injector = injector;
            return this;
        }

        public Reconciliator<T> build() {
            return new Reconciliator<T>(this);
        }
    }
}
