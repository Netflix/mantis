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

package io.mantisrx.runtime.source.http.impl;

import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.runtime.source.http.HttpServerProvider;
import io.mantisrx.runtime.source.http.ServerPoller;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;


public class DefaultHttpServerProvider implements HttpServerProvider {

    private final ServerPoller serverPoller;
    private final Gauge discoveryActiveGauge;
    private final Gauge newServersGauge;
    private final Gauge removedServersGauge;

    protected DefaultHttpServerProvider(ServerPoller serverPoller) {
        this.serverPoller = serverPoller;

        Metrics m = new Metrics.Builder()
                .name("DefaultHttpServerProvider")
                .addGauge("discoveryActiveGauge")
                .addGauge("newServersGauge")
                .addGauge("removedServersGauge")
                .build();

        m = MetricsRegistry.getInstance().registerAndGet(m);

        discoveryActiveGauge = m.getGauge("discoveryActiveGauge");
        newServersGauge = m.getGauge("newServersGauge");
        removedServersGauge = m.getGauge("removedServersGauge");
    }

    private static Set<ServerInfo> diff(Set<ServerInfo> left, Set<ServerInfo> right) {
        Set<ServerInfo> result = new HashSet<>(left);
        result.removeAll(right);

        return result;
    }

    public Set<ServerInfo> getServers() {
        return serverPoller.getServers();
    }

    @Override
    public final Observable<ServerInfo> getServersToAdd() {
        // We use an Observable.create instead of a simple serverPoller.servers().flatMap(...)
        // because we want to create an activeServers object for each subscription
        return Observable.create(new OnSubscribe<ServerInfo>() {
            @Override
            public void call(final Subscriber<? super ServerInfo> subscriber) {
                // Single out the assignment to make type inference happy
                Set<ServerInfo> empty = Collections.emptySet();
                final AtomicReference<Set<ServerInfo>> activeServers = new AtomicReference<>(empty);

                Subscription subs = serverPoller.servers()
                        .subscribe(new Subscriber<Set<ServerInfo>>() {
                            @Override
                            public void onCompleted() {
                                subscriber.onCompleted();
                            }

                            @Override
                            public void onError(Throwable e) {
                                subscriber.onError(e);
                            }

                            @Override
                            public void onNext(Set<ServerInfo> servers) {
                                discoveryActiveGauge.set(servers.size());
                                Set<ServerInfo> currentServers = activeServers.getAndSet(servers);
                                Set<ServerInfo> newServers = diff(servers, currentServers);
                                newServersGauge.set(newServers.size());
                                //                            for (ServerInfo server : newServers) {
                                //                                subscriber.onNext(server);
                                //                            }
                                // always send down all active server list, let the client figure out if it is already connected
                                for (ServerInfo server : servers) {
                                    subscriber.onNext(server);
                                }

                            }
                        });
                // We need to make sure if a subscriber unsubscribes, the server poller
                // should stop sending data to the subscriber
                subscriber.add(subs);
            }
        });
    }

    @Override
    public Observable<ServerInfo> getServersToRemove() {
        return Observable.create(new OnSubscribe<ServerInfo>() {
            @Override
            public void call(final Subscriber<? super ServerInfo> subscriber) {
                // Single out the assignment to make type inference happy
                Set<ServerInfo> empty = Collections.emptySet();
                final AtomicReference<Set<ServerInfo>> activeServers = new AtomicReference<>(empty);

                Subscription subs = serverPoller.servers()
                        .subscribe(new Subscriber<Set<ServerInfo>>() {
                            @Override
                            public void onCompleted() {
                                subscriber.onCompleted();
                            }

                            @Override
                            public void onError(Throwable e) {
                                subscriber.onError(e);
                            }

                            @Override
                            public void onNext(Set<ServerInfo> servers) {
                                Set<ServerInfo> currentServers = activeServers.getAndSet(servers);
                                Set<ServerInfo> serversToRemove = diff(currentServers, servers);
                                removedServersGauge.set(serversToRemove.size());
                                for (ServerInfo server : serversToRemove) {
                                    subscriber.onNext(server);
                                }
                            }
                        });
                // We need to make sure if a subscriber unsubscribes, the server poller
                // should stop sending data to the subscriber
                subscriber.add(subs);
            }
        });
    }
}
