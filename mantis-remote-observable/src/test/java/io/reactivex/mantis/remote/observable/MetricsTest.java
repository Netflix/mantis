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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.observables.MathObservable;


public class MetricsTest {

    @Test
    public void testConnectionMetrics() {
        // setup
        Observable<Integer> os = Observable.range(1, 1000);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
        server.start();
        // connect
        ConnectToObservable<Integer> cc = new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .decoder(Codecs.integer())
                .build();

        RemoteRxConnection<Integer> rc = RemoteObservable.connect(cc);
        // assert
        MathObservable.sumInteger(rc.getObservable()).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(500500, t1.intValue()); // sum of number 0-100
            }
        });

        Assertions.assertEquals(1000, rc.getMetrics().getOnNextCount());
        Assertions.assertEquals(0, rc.getMetrics().getOnErrorCount());
        Assertions.assertEquals(1, rc.getMetrics().getOnCompletedCount());
    }

    @Test
    public void testServerMetrics() {
        // setup
        Observable<Integer> os = Observable.range(1, 1000);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
        server.start();
        // connect
        ConnectToObservable<Integer> cc = new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .decoder(Codecs.integer())
                .build();

        Observable<Integer> oc = RemoteObservable.connect(cc).getObservable();
        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(500500, t1.intValue()); // sum of number 0-100
            }
        });

        Assertions.assertEquals(1000, server.getMetrics().getOnNextCount());
        Assertions.assertEquals(0, server.getMetrics().getOnErrorCount());
        Assertions.assertEquals(1, server.getMetrics().getOnCompletedCount());
    }

    @Test
    public void testMutlipleConnectionsSingleServerMetrics() throws InterruptedException {
        // setup
        Observable<Integer> os = Observable.range(1, 1000);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
        server.start();
        // connect
        ConnectToObservable<Integer> cc = new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .decoder(Codecs.integer())
                .build();

        RemoteRxConnection<Integer> ro1 = RemoteObservable.connect(cc);
        // assert
        MathObservable.sumInteger(ro1.getObservable()).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(500500, t1.intValue()); // sum of number 0-100
            }
        });

        RemoteRxConnection<Integer> ro2 = RemoteObservable.connect(cc);
        // assert
        MathObservable.sumInteger(ro2.getObservable()).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(500500, t1.intValue()); // sum of number 0-100
            }
        });

        // client asserts
        Assertions.assertEquals(1000, ro1.getMetrics().getOnNextCount());
        Assertions.assertEquals(0, ro1.getMetrics().getOnErrorCount());
        Assertions.assertEquals(1, ro1.getMetrics().getOnCompletedCount());

        Assertions.assertEquals(1000, ro2.getMetrics().getOnNextCount());
        Assertions.assertEquals(0, ro2.getMetrics().getOnErrorCount());
        Assertions.assertEquals(1, ro2.getMetrics().getOnCompletedCount());

        // server asserts
        Assertions.assertEquals(2000, server.getMetrics().getOnNextCount());
        Assertions.assertEquals(0, server.getMetrics().getOnErrorCount());
        Assertions.assertEquals(2, server.getMetrics().getOnCompletedCount());
        Assertions.assertEquals(2, server.getMetrics().getSubscribedCount());
        Thread.sleep(1000); // allow time for unsub, connections to close
        Assertions.assertEquals(2, server.getMetrics().getUnsubscribedCount());
    }

    @Test
    public void testMutlipleConnectionsSingleServerErrorsMetrics() throws InterruptedException {
        // setup
        Observable<Integer> o = Observable.create(new OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                for (int i = 0; i < 10; i++) {
                    if (i == 5) {
                        subscriber.onError(new RuntimeException("error"));
                    }
                    subscriber.onNext(i);
                }
            }
        });
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, o, Codecs.integer());
        server.start();
        // connect
        ConnectToObservable<Integer> cc = new ConnectToObservable.Builder<Integer>()
                .subscribeAttempts(1)
                .host("localhost")
                .port(serverPort)
                .decoder(Codecs.integer())
                .build();

        RemoteRxConnection<Integer> ro1 = RemoteObservable.connect(cc);
        try {
            MathObservable.sumInteger(ro1.getObservable()).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(500500, t1.intValue()); // sum of number 0-100
                }
            });
        } catch (Exception e) {
            // noOp
        }

        RemoteRxConnection<Integer> ro2 = RemoteObservable.connect(cc);
        try {
            MathObservable.sumInteger(ro2.getObservable()).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(500500, t1.intValue()); // sum of number 0-100
                }
            });
        } catch (Exception e) {
            // noOp
        }

        // client asserts
        Assertions.assertEquals(5, ro1.getMetrics().getOnNextCount());
        Assertions.assertEquals(1, ro1.getMetrics().getOnErrorCount());
        Assertions.assertEquals(0, ro1.getMetrics().getOnCompletedCount());

        Assertions.assertEquals(5, ro2.getMetrics().getOnNextCount());
        Assertions.assertEquals(1, ro2.getMetrics().getOnErrorCount());
        Assertions.assertEquals(0, ro2.getMetrics().getOnCompletedCount());

        // server asserts
        Assertions.assertEquals(10, server.getMetrics().getOnNextCount());
        Assertions.assertEquals(2, server.getMetrics().getOnErrorCount());
        Assertions.assertEquals(0, server.getMetrics().getOnCompletedCount());
        Assertions.assertEquals(2, server.getMetrics().getSubscribedCount());
        Thread.sleep(1000); // allow time for unsub, connections to close
        Assertions.assertEquals(2, server.getMetrics().getUnsubscribedCount());
    }

    @Test
    public void testConnectionOnErrorCount() {
        // setup
        Observable<Integer> o = Observable.create(new OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                for (int i = 0; i < 10; i++) {
                    if (i == 5) {
                        subscriber.onError(new RuntimeException("error"));
                    }
                    subscriber.onNext(i);
                }
            }
        });
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, o, Codecs.integer());
        server.start();
        // connect
        ConnectToObservable<Integer> cc = new ConnectToObservable.Builder<Integer>()
                .subscribeAttempts(1)
                .host("localhost")
                .port(serverPort)
                .decoder(Codecs.integer())
                .build();

        RemoteRxConnection<Integer> rc = RemoteObservable.connect(cc);

        // assert
        try {
            MathObservable.sumInteger(rc.getObservable()).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(500500, t1.intValue()); // sum of number 0-100
                }
            });
        } catch (Exception e) {
            // noOp
        }

        Assertions.assertEquals(5, rc.getMetrics().getOnNextCount());
        Assertions.assertEquals(1, rc.getMetrics().getOnErrorCount());
        Assertions.assertEquals(0, rc.getMetrics().getOnCompletedCount());
    }

}
