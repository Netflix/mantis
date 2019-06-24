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
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.GroupedObservable;
import rx.subjects.PublishSubject;


public class DynamicConnection<T> {

    private static final Logger logger = LoggerFactory.getLogger(DynamicConnection.class);

    private Observable<Endpoint> changeEndpointObservable;
    private PublishSubject<Observable<T>> subject = PublishSubject.create();
    private Func1<Endpoint, Observable<T>> toObservableFunc;

    DynamicConnection(Func1<Endpoint, Observable<T>> toObservableFunc, Observable<Endpoint> changeEndpointObservable) {
        this.changeEndpointObservable = changeEndpointObservable;
        this.toObservableFunc = toObservableFunc;
    }

    public static <K, V> DynamicConnection<GroupedObservable<K, V>> create(
            final ConnectToGroupedObservable.Builder<K, V> config, Observable<Endpoint> endpoints) {
        Func1<Endpoint, Observable<GroupedObservable<K, V>>> toObservableFunc = new
                Func1<Endpoint, Observable<GroupedObservable<K, V>>>() {
                    @Override
                    public Observable<GroupedObservable<K, V>> call(Endpoint endpoint) {
                        // copy config, change host, port and id
                        ConnectToGroupedObservable.Builder<K, V> configCopy = new ConnectToGroupedObservable.Builder<K, V>(config);
                        configCopy
                                .host(endpoint.getHost())
                                .port(endpoint.getPort())
                                .slotId(endpoint.getSlotId());
                        return RemoteObservable.connect(configCopy.build()).getObservable();
                    }
                };
        return new DynamicConnection<GroupedObservable<K, V>>(toObservableFunc, endpoints);
    }

    public static <T> DynamicConnection<T> create(
            final ConnectToObservable.Builder<T> config, Observable<Endpoint> endpoints) {
        Func1<Endpoint, Observable<T>> toObservableFunc = new
                Func1<Endpoint, Observable<T>>() {
                    @Override
                    public Observable<T> call(Endpoint endpoint) {
                        // copy config, change host, port and id
                        ConnectToObservable.Builder<T> configCopy = new ConnectToObservable.Builder<T>(config);
                        configCopy
                                .host(endpoint.getHost())
                                .port(endpoint.getPort())
                                .slotId(endpoint.getSlotId());
                        return RemoteObservable.connect(configCopy.build()).getObservable();
                    }
                };
        return new DynamicConnection<T>(toObservableFunc, endpoints);
    }

    public void close() {
        subject.onCompleted();
    }

    public Observable<T> observable() {
        return Observable.create(new OnSubscribe<T>() {
            @Override
            public void call(final Subscriber<? super T> subscriber) {
                subscriber.add(subject.flatMap(new Func1<Observable<T>, Observable<T>>() {
                    @Override
                    public Observable<T> call(Observable<T> t1) {
                        return t1;
                    }
                }).subscribe(new Observer<T>() {

                    @Override
                    public void onCompleted() {
                        subscriber.onCompleted();
                    }

                    @Override
                    public void onError(Throwable e) {
                        subscriber.onError(e);
                    }

                    @Override
                    public void onNext(T t) {
                        subscriber.onNext(t);
                    }
                }));
                subscriber.add(changeEndpointObservable.subscribe(new Action1<Endpoint>() {
                    @Override
                    public void call(Endpoint endpoint) {
                        logger.debug("New endpoint: " + endpoint);
                        subject.onNext(toObservableFunc.call(endpoint));
                    }
                }));
            }
        });
    }
}
