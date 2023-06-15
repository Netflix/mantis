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

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.common.network.Endpoint;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.observables.MathObservable;
import rx.subjects.ReplaySubject;


public class DynamicConnectionSetTest {

    @Test
    public void testMergeInConnections() throws InterruptedException {

        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        final int server1Port = portSelector.acquirePort();
        final int server2Port = portSelector.acquirePort();

        // setup servers
        RemoteRxServer server1 = RemoteObservable.serve(server1Port, Observable.range(1, 50), Codecs.integer());
        RemoteRxServer server2 = RemoteObservable.serve(server2Port, Observable.range(51, 50), Codecs.integer());

        server1.start();
        server2.start();

        EndpointInjector staticEndpoints = new EndpointInjector() {
            @Override
            public Observable<EndpointChange> deltas() {
                return Observable.create(new OnSubscribe<EndpointChange>() {
                    @Override
                    public void call(Subscriber<? super EndpointChange> subscriber) {
                        subscriber.onNext(new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", server1Port, "1")));
                        subscriber.onNext(new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", server2Port, "2")));
                        subscriber.onCompleted();
                    }
                });
            }
        };

        DynamicConnectionSet<Integer> cm
                = DynamicConnectionSet.create(new ConnectToObservable.Builder<Integer>()
                .decoder(Codecs.integer()));
        cm.setEndpointInjector(staticEndpoints);

        int sum = MathObservable.sumInteger(Observable.merge(cm.observables()))
                .toBlocking()
                .last();
        Assertions.assertEquals(5050, sum);

    }

    @Test
    public void testMergeInWithDeltaEndpointService() {
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        final int server1Port = portSelector.acquirePort();
        final int server2Port = portSelector.acquirePort();

        // setup servers
        RemoteRxServer server1 = RemoteObservable.serve(server1Port, Observable.range(1, 50), Codecs.integer());
        RemoteRxServer server2 = RemoteObservable.serve(server2Port, Observable.range(51, 50), Codecs.integer());

        server1.start();
        server2.start();

        ReplaySubject<List<Endpoint>> subject = ReplaySubject.create();
        List<Endpoint> endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", server1Port));
        endpoints.add(new Endpoint("localhost", server2Port));
        subject.onNext(endpoints);
        subject.onCompleted();

        DynamicConnectionSet<Integer> cm
                = DynamicConnectionSet.create(new ConnectToObservable.Builder<Integer>()
                .decoder(Codecs.integer()));
        cm.setEndpointInjector(new ToDeltaEndpointInjector(subject));

        int sum = MathObservable.sumInteger(Observable.merge(cm.observables()))
                .toBlocking()
                .last();
        Assertions.assertEquals(5050, sum);

    }

    @Test
    public void testMergeInBadObservable() throws InterruptedException {
        Assertions.assertThrows(RuntimeException.class, () -> {
            PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
            final int server1Port = portSelector.acquirePort();
            final int server2Port = portSelector.acquirePort();

            Observable<Integer> badObservable = Observable.create(new OnSubscribe<Integer>() {
                @Override
                public void call(Subscriber<? super Integer> subscriber) {
                    for (int i = 100; i < 200; i++) {
                        subscriber.onNext(i);
                        if (i == 150) {
                            subscriber.onError(new Exception("error"));
                        }
                    }
                }
            });

            // setup servers
            RemoteRxServer server1 = RemoteObservable.serve(server1Port, Observable.range(1, 50), Codecs.integer());
            RemoteRxServer server2 = RemoteObservable.serve(server2Port, badObservable, Codecs.integer());

            server1.start();
            server2.start();

            EndpointInjector staticEndpoints = new EndpointInjector() {
                @Override
                public Observable<EndpointChange> deltas() {
                    return Observable.create(new OnSubscribe<EndpointChange>() {
                        @Override
                        public void call(Subscriber<? super EndpointChange> subscriber) {
                            subscriber.onNext(new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", server1Port, "1")));
                            subscriber.onNext(new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", server2Port, "2")));
                            subscriber.onCompleted();
                        }
                    });
                }
            };

            DynamicConnectionSet<Integer> cm
                = DynamicConnectionSet.create(new ConnectToObservable.Builder<Integer>()
                .decoder(Codecs.integer()));
            cm.setEndpointInjector(staticEndpoints);

            int sum = MathObservable.sumInteger(Observable.merge(cm.observables()))
                .toBlocking()
                .last();

            Assertions.assertEquals(5050, sum);
        });
    }
}
