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

import io.mantisrx.runtime.source.http.ServerPoller;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.functions.Action0;
import rx.schedulers.Schedulers;


public class StaticServerPoller implements ServerPoller {

    private final Set<ServerInfo> servers;
    private final int periodSeconds;
    private final Scheduler scheduler;

    public StaticServerPoller(Set<ServerInfo> servers, int periodSeconds, Scheduler scheduler) {
        this.servers = Collections.unmodifiableSet(servers);
        this.periodSeconds = periodSeconds;
        this.scheduler = scheduler;
    }

    public StaticServerPoller(Set<ServerInfo> servers, int periodSeconds) {
        this(servers, periodSeconds, Schedulers.computation());
    }

    private Worker schedulePolling(final Subscriber<? super Set<ServerInfo>> subscriber) {
        final Worker worker = this.scheduler.createWorker();
        worker.schedulePeriodically(
                new Action0() {
                    @Override
                    public void call() {
                        if (subscriber.isUnsubscribed()) {
                            worker.unsubscribe();
                        } else {
                            subscriber.onNext(servers);
                        }
                    }
                },
                0,
                this.periodSeconds,
                TimeUnit.SECONDS
        );

        return worker;
    }

    @Override
    public Observable<Set<ServerInfo>> servers() {
        return Observable.create((OnSubscribe<Set<ServerInfo>>) this::schedulePolling);
    }

    @Override
    public Set<ServerInfo> getServers() {

        return servers;
    }
}
