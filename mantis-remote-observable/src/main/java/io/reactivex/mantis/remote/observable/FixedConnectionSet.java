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

import io.mantisrx.common.network.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.GroupedObservable;
import rx.subscriptions.BooleanSubscription;


public class FixedConnectionSet<T> {

    private static final Logger logger = LoggerFactory.getLogger(FixedConnectionSet.class);

    private EndpointInjector endpointInjector;
    private Func1<Endpoint, Observable<T>> toObservableFunc;
    private MergedObservable<T> mergedObservable;

    public FixedConnectionSet(int expectedTerminalCount, EndpointInjector endpointInjector,
                              Func1<Endpoint, Observable<T>> toObservableFunc) {
        this.endpointInjector = endpointInjector;
        this.toObservableFunc = toObservableFunc;
        this.mergedObservable = MergedObservable.create(expectedTerminalCount);
    }

    public static <K, V> FixedConnectionSet<GroupedObservable<K, V>> create(int expectedTerminalCount,
                                                                            final ConnectToGroupedObservable.Builder<K, V> config, EndpointInjector endpointService) {
        Func1<Endpoint, Observable<GroupedObservable<K, V>>> toObservableFunc = new
                Func1<Endpoint, Observable<GroupedObservable<K, V>>>() {
                    @Override
                    public Observable<GroupedObservable<K, V>> call(Endpoint endpoint) {
                        config.host(endpoint.getHost())
                                .port(endpoint.getPort());
                        return RemoteObservable.connect(config.build()).getObservable();
                    }
                };
        return new FixedConnectionSet<GroupedObservable<K, V>>(expectedTerminalCount, endpointService, toObservableFunc);
    }

    public static <T> FixedConnectionSet<T> create(int expectedTerminalCount,
                                                   final ConnectToObservable.Builder<T> config, EndpointInjector endpointService) {
        Func1<Endpoint, Observable<T>> toObservableFunc = new
                Func1<Endpoint, Observable<T>>() {
                    @Override
                    public Observable<T> call(Endpoint endpoint) {
                        config.host(endpoint.getHost())
                                .port(endpoint.getPort());
                        return RemoteObservable.connect(config.build()).getObservable();
                    }
                };
        return new FixedConnectionSet<T>(expectedTerminalCount, endpointService, toObservableFunc);
    }

    public Observable<Observable<T>> getObservables() {
        return Observable.create(new OnSubscribe<Observable<T>>() {
            @Override
            public void call(final Subscriber<? super Observable<T>> subscriber) {

                final BooleanSubscription subscription = new BooleanSubscription();

                // clean up state if unsubscribe
                subscriber.add(new Subscription() {
                    @Override
                    public void unsubscribe() {
                        // NOTE, this assumes one
                        // unsubscribe should
                        // clear all state.  Which
                        // is ok if the O<O<T>>
                        // is published.refCounted()
                        mergedObservable.clear();
                        subscription.unsubscribe();
                    }

                    @Override
                    public boolean isUnsubscribed() {
                        return subscription.isUnsubscribed();
                    }
                });

                subscriber.add(mergedObservable.get().subscribe(new Observer<Observable<T>>() {
                    @Override
                    public void onCompleted() {
                        subscriber.onCompleted();
                    }

                    @Override
                    public void onError(Throwable e) {
                        subscriber.onError(e);
                    }

                    @Override
                    public void onNext(Observable<T> t) {
                        subscriber.onNext(t);
                    }
                }));

                subscriber.add(endpointInjector.deltas().subscribe(new Action1<EndpointChange>() {
                    @Override
                    public void call(EndpointChange ec) {
                        String id = Endpoint.uniqueHost(ec.getEndpoint().getHost(), ec.getEndpoint().getPort(), ec.getEndpoint().getSlotId());
                        if (EndpointChange.Type.add == ec.getType()) {
                            logger.info("Adding new connection to host: " + ec.getEndpoint().getHost() + " at port: " + ec.getEndpoint().getPort() +
                                    " with id: " + id);
                            mergedObservable.mergeIn(id, toObservableFunc.call(ec.getEndpoint()), ec.getEndpoint().getErrorCallback(),
                                    ec.getEndpoint().getCompletedCallback());
                        } else if (EndpointChange.Type.complete == ec.getType()) {
                            logger.info("Forcing connection to complete host: " + ec.getEndpoint().getHost() + " at port: " +
                                    ec.getEndpoint().getPort() + " with id: " + id);
                            mergedObservable.forceComplete(id);
                        }
                    }
                }));
            }

        });
    }
}
