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

import io.mantisrx.common.MantisGroup;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.network.Endpoint;
import io.reactivex.mantis.remote.observable.reconciliator.ConnectionSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func3;
import rx.observables.GroupedObservable;
import rx.subjects.PublishSubject;


public class DynamicConnectionSet<T> implements ConnectionSet<T> {

    private static final Logger logger = LoggerFactory.getLogger(DynamicConnectionSet.class);
    private static final SpscArrayQueue<MantisGroup<?, ?>> inputQueue = new SpscArrayQueue<MantisGroup<?, ?>>(1000);
    private static int MIN_TIME_SEC_DEFAULT = 1;
    private static int MAX_TIME_SEC_DEFAULT = 10;
    private EndpointInjector endpointInjector;
    private PublishSubject<EndpointChange> reconciliatorConnector = PublishSubject.create();
    private Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<T>> toObservableFunc;
    private Metrics connectionMetrics;
    private PublishSubject<Set<Endpoint>> activeConnectionsSubject = PublishSubject.create();
    private Lock activeConnectionsLock = new ReentrantLock();
    private Map<String, Endpoint> currentActiveConnections = new HashMap<>();
    private int minTimeoutOnUnexpectedTerminateSec;
    private int maxTimeoutOnUnexpectedTerminateSec;
    private Gauge activeConnectionsGauge;
    private Gauge closedConnections;
    private Gauge forceCompletedConnections;
    private Random random = new Random();

    public DynamicConnectionSet(Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<T>>
                                        toObservableFunc, int minTimeoutOnUnexpectedTerminateSec, int maxTimeoutOnUnexpectedTerminateSec) {
        this.toObservableFunc = toObservableFunc;
        connectionMetrics = new Metrics.Builder()
                .name("DynamicConnectionSet")
                .addGauge("activeConnections")
                .addGauge("closedConnections")
                .addGauge("forceCompletedConnections")
                .build();

        activeConnectionsGauge = connectionMetrics.getGauge("activeConnections");
        closedConnections = connectionMetrics.getGauge("closedConnections");
        forceCompletedConnections = connectionMetrics.getGauge("forceCompletedConnections");

        this.minTimeoutOnUnexpectedTerminateSec = minTimeoutOnUnexpectedTerminateSec;
        this.maxTimeoutOnUnexpectedTerminateSec = maxTimeoutOnUnexpectedTerminateSec;
    }

    public DynamicConnectionSet(Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<T>>
                                        toObservableFunc) {
        this(toObservableFunc, MIN_TIME_SEC_DEFAULT, MAX_TIME_SEC_DEFAULT);
    }

    public static <K, V> DynamicConnectionSet<GroupedObservable<K, V>> create(
            final ConnectToGroupedObservable.Builder<K, V> config, int maxTimeBeforeDisconnectSec) {
        Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<GroupedObservable<K, V>>> toObservableFunc = new
                Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<GroupedObservable<K, V>>>() {
                    @Override
                    public RemoteRxConnection<GroupedObservable<K, V>> call(Endpoint endpoint, Action0 disconnectCallback,
                                                                            PublishSubject<Integer> closeConnectionTrigger) {
                        // copy config, change host, port and id
                        ConnectToGroupedObservable.Builder<K, V> configCopy = new ConnectToGroupedObservable.Builder<K, V>(config);
                        configCopy
                                .host(endpoint.getHost())
                                .port(endpoint.getPort())
                                .closeTrigger(closeConnectionTrigger)
                                .connectionDisconnectCallback(disconnectCallback)
                                .slotId(endpoint.getSlotId());
                        return RemoteObservable.connect(configCopy.build());
                    }
                };
        return new DynamicConnectionSet<GroupedObservable<K, V>>(toObservableFunc, MIN_TIME_SEC_DEFAULT, maxTimeBeforeDisconnectSec);
    }

    // NJ
    public static <K, V> DynamicConnectionSet<MantisGroup<K, V>> createMGO(
            final ConnectToGroupedObservable.Builder<K, V> config, int maxTimeBeforeDisconnectSec, final SpscArrayQueue<MantisGroup<?, ?>> inputQueue) {
        Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<MantisGroup<K, V>>> toObservableFunc
                = new Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<MantisGroup<K, V>>>() {
            @Override
            public RemoteRxConnection<MantisGroup<K, V>> call(Endpoint endpoint, Action0 disconnectCallback,
                                                              PublishSubject<Integer> closeConnectionTrigger) {
                // copy config, change host, port and id
                ConnectToGroupedObservable.Builder<K, V> configCopy = new ConnectToGroupedObservable.Builder<K, V>(config);
                configCopy
                        .host(endpoint.getHost())
                        .port(endpoint.getPort())
                        .closeTrigger(closeConnectionTrigger)
                        .connectionDisconnectCallback(disconnectCallback)
                        .slotId(endpoint.getSlotId());
                return RemoteObservable.connectToMGO(configCopy.build(), inputQueue);
            }
        };
        return new DynamicConnectionSet<MantisGroup<K, V>>(toObservableFunc, MIN_TIME_SEC_DEFAULT, maxTimeBeforeDisconnectSec);
    }

    public static <K, V> DynamicConnectionSet<GroupedObservable<K, V>> create(
            final ConnectToGroupedObservable.Builder<K, V> config) {
        return create(config, MAX_TIME_SEC_DEFAULT);
    }

    // NJ
    public static <K, V> DynamicConnectionSet<MantisGroup<K, V>> createMGO(
            final ConnectToGroupedObservable.Builder<K, V> config) {
        return createMGO(config, MAX_TIME_SEC_DEFAULT, inputQueue);
    }

    public static <T> DynamicConnectionSet<T> create(
            final ConnectToObservable.Builder<T> config, int maxTimeBeforeDisconnectSec) {
        Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<T>> toObservableFunc = new
                Func3<Endpoint, Action0, PublishSubject<Integer>, RemoteRxConnection<T>>() {
                    @Override
                    public RemoteRxConnection<T> call(Endpoint endpoint, Action0 disconnectCallback,
                                                      PublishSubject<Integer> closeConnectionTrigger) {
                        // copy config, change host, port and id
                        ConnectToObservable.Builder<T> configCopy = new ConnectToObservable.Builder<T>(config);
                        configCopy
                                .host(endpoint.getHost())
                                .port(endpoint.getPort())
                                .closeTrigger(closeConnectionTrigger)
                                .connectionDisconnectCallback(disconnectCallback)
                                .slotId(endpoint.getSlotId());
                        return RemoteObservable.connect(configCopy.build());
                    }
                };
        return new DynamicConnectionSet<T>(toObservableFunc, MIN_TIME_SEC_DEFAULT, maxTimeBeforeDisconnectSec);
    }

    public static <T> DynamicConnectionSet<T> create(
            final ConnectToObservable.Builder<T> config) {
        return create(config, MAX_TIME_SEC_DEFAULT);
    }

    public void setEndpointInjector(EndpointInjector endpointInjector) {
        this.endpointInjector = endpointInjector;
    }

    public Observer<EndpointChange> reconciliatorObserver() {
        return reconciliatorConnector;
    }

    public Metrics getConnectionMetrics() {
        return connectionMetrics;
    }

    public Observable<Observable<T>> observables() {
        return
                endpointInjector
                        .deltas()
                        .doOnCompleted(() -> logger.info("onComplete on injector deltas"))
                        .doOnError(t -> logger.error("caught unexpected error {}", t.getMessage(), t))
                        .doOnSubscribe(new Action0() {
                            @Override
                            public void call() {
                                logger.info("Subscribing, clearing active connection set");
                                resetActiveConnections();
                            }
                        })
                        .groupBy(t1 -> Endpoint.uniqueHost(t1.getEndpoint().getHost(), t1.getEndpoint().getPort(), t1.getEndpoint().getSlotId()))
                        .flatMap(new Func1<GroupedObservable<String, EndpointChange>, Observable<Observable<T>>>() {
                            @Override
                            public Observable<Observable<T>> call(
                                    final GroupedObservable<String, EndpointChange> group) {

                                final PublishSubject<Integer> closeConnectionTrigger = PublishSubject.create();
                                return group
                                        .doOnNext(new Action1<EndpointChange>() {
                                            @Override
                                            public void call(EndpointChange change) {
                                                // side effect to force complete
                                                if (EndpointChange.Type.complete == change.getType() &&
                                                        activeConnectionsContains(group.getKey(), change.getEndpoint())) {
                                                    logger.info("Received complete request, removing connection from active set, " + change.getEndpoint().getHost() +
                                                            " port: " + change.getEndpoint().getPort() + " id: " + change.getEndpoint().getSlotId());
                                                    forceCompletedConnections.increment();
                                                    removeConnection(group.getKey(), change.getEndpoint());
                                                    closeConnectionTrigger.onNext(1);
                                                }
                                            }
                                        })
                                        .filter(new Func1<EndpointChange, Boolean>() {
                                            @Override
                                            public Boolean call(EndpointChange change) {
                                                // active connection check is to ensure
                                                // latent adds are not allowed.  This can
                                                // occur if dynamicConnection set
                                                // and reconciliator are chained together.
                                                // Reconciliator will "see" changes
                                                // before dynamic connection set add will
                                                // assume connection is missing from set
                                                boolean contains = activeConnectionsContains(group.getKey(), change.getEndpoint());
                                                if (contains) {
                                                    logger.info("Skipping latent add for endpoint, already in active set: " + change);
                                                }
                                                return EndpointChange.Type.add == change.getType() &&
                                                        !contains;
                                            }
                                        })
                                        .map(new Func1<EndpointChange, Observable<T>>() {
                                            @Override
                                            public Observable<T> call(final EndpointChange toAdd) {
                                                logger.info("Received add request, adding connection to active set, " + toAdd.getEndpoint().getHost() +
                                                        " port: " + toAdd.getEndpoint().getPort() + ", with client id: " + toAdd.getEndpoint().getSlotId());
                                                addConnection(group.getKey(), toAdd.getEndpoint());
                                                Action0 disconnectCallback = new Action0() {
                                                    @Override
                                                    public void call() {
                                                        int timeToWait = random.nextInt((maxTimeoutOnUnexpectedTerminateSec - minTimeoutOnUnexpectedTerminateSec) + 1)
                                                                + minTimeoutOnUnexpectedTerminateSec;
                                                        logger.info("Connection disconnected, waiting " + timeToWait + " seconds before removing from active set of connections: " + toAdd);
                                                        Observable.timer(timeToWait, TimeUnit.SECONDS)
                                                                .doOnCompleted(new Action0() {
                                                                    @Override
                                                                    public void call() {
                                                                        logger.warn("Removing connection from active set, " + toAdd);
                                                                        closedConnections.increment();
                                                                        removeConnection(group.getKey(), toAdd.getEndpoint());
                                                                    }
                                                                }).subscribe();
                                                    }
                                                };
                                                RemoteRxConnection<T> connection = toObservableFunc.call(toAdd.getEndpoint(), disconnectCallback,
                                                        closeConnectionTrigger);
                                                return connection.getObservable()
                                                        .doOnCompleted(toAdd.getEndpoint().getCompletedCallback())
                                                        .doOnError(toAdd.getEndpoint().getErrorCallback());

                                            }
                                        });
                            }
                        });
    }

    public Observable<Set<Endpoint>> activeConnections() {
        return activeConnectionsSubject;
    }

    public boolean activeConnectionsContains(String id, Endpoint endpoint) {
        try {
            activeConnectionsLock.lock();
            return currentActiveConnections.containsKey(id);
        } finally {
            activeConnectionsLock.unlock();
        }
    }

    public void resetActiveConnections() {
        try {
            activeConnectionsLock.lock();
            currentActiveConnections.clear();
            activeConnectionsGauge.set(0);
            activeConnectionsSubject.onNext(new HashSet<Endpoint>());
        } finally {
            activeConnectionsLock.unlock();
        }
    }


    public void addConnection(String id, Endpoint toAdd) {
        try {
            activeConnectionsLock.lock();
            if (!currentActiveConnections.containsKey(id)) {
                currentActiveConnections.put(id, new Endpoint(toAdd.getHost(),
                        toAdd.getPort(), toAdd.getSlotId(),
                        toAdd.getCompletedCallback(),
                        toAdd.getErrorCallback()));
                activeConnectionsGauge.increment();
                activeConnectionsSubject.onNext(new HashSet<Endpoint>(currentActiveConnections.values()));
            }
        } finally {
            activeConnectionsLock.unlock();
        }
    }

    public void removeConnection(String id, Endpoint toRemove) {
        try {
            activeConnectionsLock.lock();
            if (currentActiveConnections.containsKey(id)) {
                currentActiveConnections.remove(id);
                activeConnectionsGauge.decrement();
                activeConnectionsSubject.onNext(new HashSet<Endpoint>(currentActiveConnections.values()));
            }
        } finally {
            activeConnectionsLock.unlock();
        }
    }
}
