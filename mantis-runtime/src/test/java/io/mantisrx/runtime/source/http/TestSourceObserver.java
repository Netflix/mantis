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

package io.mantisrx.runtime.source.http;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType;
import io.mantisrx.runtime.source.http.impl.OperatorResumeOnCompleted;
import io.mantisrx.runtime.source.http.impl.ResumeOnCompletedPolicy;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.subjects.PublishSubject;


public class TestSourceObserver implements Observer<HttpSourceEvent> {

    private final AtomicInteger completionCount = new AtomicInteger();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final ConcurrentMap<EventType, AtomicInteger> sourceEventCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<EventType, ConcurrentHashMap<ServerInfo, AtomicInteger>> serverEventCounters;

    public TestSourceObserver() {
        serverEventCounters = new ConcurrentHashMap<>();
        for (EventType type : EventType.values()) {
            serverEventCounters.put(type, new ConcurrentHashMap<ServerInfo, AtomicInteger>());
        }
    }

    public static void main(String[] args) throws Exception {
        final PublishSubject<Object> subject = PublishSubject.create();

        final CountDownLatch done = new CountDownLatch(1);
        Observable.interval(10, TimeUnit.MILLISECONDS)
                .lift(new OperatorResumeOnCompleted<>(
                        new ResumeOnCompletedPolicy<Long>() {
                            @Override
                            public Observable<Long> call(Integer attempts) {
                                return Observable.just(99L);
                            }
                        }
                ))
                .takeUntil(subject)
                .subscribe(new Subscriber<Long>() {
                    @Override
                    public void onCompleted() {
                        System.out.println("completed. ");
                        done.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {

                    }

                    @Override
                    public void onNext(Long aLong) {
                        if (aLong > 10) {
                            subject.onNext("done");
                        }
                    }
                });

        done.await();


        subject.onNext("abc");

    }

    @Override
    public void onCompleted() {
        completionCount.incrementAndGet();
    }

    @Override
    public void onError(Throwable e) {
        System.err.println("Run into error" + e.getMessage());
        errorCount.incrementAndGet();
    }

    @Override
    public void onNext(HttpSourceEvent event) {
        sourceEventCounters.putIfAbsent(event.getEventType(), new AtomicInteger());
        sourceEventCounters.get(event.getEventType()).incrementAndGet();

        ConcurrentHashMap<ServerInfo, AtomicInteger> counters = serverEventCounters.get(event.getEventType());
        counters.putIfAbsent(event.getServer(), new AtomicInteger());
        counters.get(event.getServer()).incrementAndGet();

        System.out.println(String.format("Event: %s for server %s:%s", event.getEventType(), event.getServer().getHost(), event.getServer().getPort()));
    }

    public int getCompletionCount() {
        return completionCount.get();
    }

    public int getErrorCount() {
        return errorCount.get();
    }

    public int getEventCount(EventType eventType) {
        return sourceEventCounters.get(eventType).get();
    }

    public Set<EventType> getEvents() {
        return sourceEventCounters.keySet();
    }

    public int getCount(ServerInfo server, EventType eventType) {
        AtomicInteger value = serverEventCounters.get(eventType).get(server);
        if (value == null) {
            return 0;
        }
        return value.get();
    }
}
