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
import io.reactivex.mantis.remote.observable.filter.ServerSideFilters;
import io.reactivex.mantis.remote.observable.slotting.RoundRobin;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observables.MathObservable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;


public class RemoteObservableTest {

    @AfterEach
    public void waitForServerToShutdown() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testServeObservable() throws InterruptedException {
        // setup
        Observable<Integer> os = Observable.range(0, 101).subscribeOn(Schedulers.io());
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
        server.start();
        // connect
        Observable<Integer> oc = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
    }

    @Test
    public void testServeNestedObservable() throws InterruptedException {
        // setup
        Observable<Observable<Integer>> os = Observable.just(Observable.range(1, 100),
                Observable.range(1, 100), Observable.range(1, 100));
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeNestedObservable.Builder<Integer>()
                        .encoder(Codecs.integer())
                        .observable(os)
                        .build())
                .build();

        server.start();
        // connect
        Observable<Integer> oc = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        // assert

        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(15150, t1.intValue()); // sum of number 0-100
            }
        });
    }

    @Test
    public void testServeManyObservable() throws InterruptedException {
        // setup
        Observable<Integer> os = Observable.range(0, 101);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
        server.start();
        // connect
        Observable<Integer> oc1 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        // assert
        MathObservable.sumInteger(oc1).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
        // connect
        Observable<Integer> oc2 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        // assert
        MathObservable.sumInteger(oc2).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
        // connect
        Observable<Integer> oc3 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        // assert
        MathObservable.sumInteger(oc3).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
    }

    //	@Test
    public void testServeManyWithErrorObservable() throws InterruptedException {
        // setup
        Observable<Integer> os = Observable.create(new OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                for (int i = 0; i < 5; i++) {
                    subscriber.onNext(i);
                    if (i == 2) {
                        throw new RuntimeException("error!");
                    }
                }
            }
        });
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
        server.start();
        int errorCount = 0;
        // connect
        Observable<Integer> oc1 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        try {
            MathObservable.sumInteger(oc1).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
                }
            });
        } catch (RuntimeException e) {
            errorCount++;
        }
        Observable<Integer> oc2 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        try {
            MathObservable.sumInteger(oc2).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
                }
            });
        } catch (RuntimeException e) {
            errorCount++;
        }
        Observable<Integer> oc3 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        try {
            MathObservable.sumInteger(oc3).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
                }
            });
        } catch (RuntimeException e) {
            errorCount++;
        }
        Assertions.assertEquals(3, errorCount);
    }


    @Test
    public void testServeUsingByteEncodedObservable() throws InterruptedException {
        // manual encode data to byte[]
        Observable<byte[]> os = Observable.range(0, 101)
                // convert to bytes
                .map(new Func1<Integer, byte[]>() {
                    @Override
                    public byte[] call(Integer value) {
                        return ByteBuffer.allocate(4).putInt(value).array();
                    }
                });
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.bytearray());
        server.start();
        // connect
        Observable<Integer> oc = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
    }

    @Test
    public void testServeObservableByName() throws InterruptedException {
        // setup
        Observable<Integer> os = Observable.range(0, 101);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, "integers-from-0-100", os, Codecs.integer());
        server.start();
        // connect to observable by name
        Observable<Integer> oc = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .name("integers-from-0-100")
                .decoder(Codecs.integer())
                .build())
                .getObservable();
        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
    }

    @Test
    public void testFailedToConnect() throws InterruptedException {
        Assertions.assertThrows(RuntimeException.class, () -> {
            // setup
            Observable<Integer> os = Observable.range(0, 101);
            // serve
            PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
            int serverPort = portSelector.acquirePort();
            int wrongPort = portSelector.acquirePort();

            RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
            server.start();
            // connect
            Observable<Integer> oc = RemoteObservable.connect("localhost", wrongPort, Codecs.integer());
            // assert
            MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
                }
            });
        });
    }

    @Test
    public void testServeTwoObservablesOnSamePort() throws InterruptedException {
        // setup
        Observable<Integer> os1 = Observable.range(0, 101);
        Observable<String> os2 = Observable.from(new String[] {"a", "b", "c", "d", "e"});
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeObservable.Builder<Integer>()
                        .name("ints")
                        .encoder(Codecs.integer())
                        .observable(os1)
                        .build())
                .addObservable(new ServeObservable.Builder<String>()
                        .name("strings")
                        .encoder(Codecs.string())
                        .observable(os2)
                        .build())
                .build();

        server.start();
        // connect to observable by name
        Observable<Integer> ro1 = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .name("ints")
                .decoder(Codecs.integer())
                .build())
                .getObservable();

        Observable<String> ro2 = RemoteObservable.connect(new ConnectToObservable.Builder<String>()
                .host("localhost")
                .port(serverPort)
                .name("strings")
                .decoder(Codecs.string())
                .build())
                .getObservable();

        // assert
        MathObservable.sumInteger(ro1).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
        ro2.reduce(new Func2<String, String, String>() {
            @Override
            public String call(String t1, String t2) {
                return t1 + t2; // concat string
            }
        }).toBlocking().forEach(new Action1<String>() {
            @Override
            public void call(String t1) {
                Assertions.assertEquals("abcde", t1);
            }
        });
    }

    @Test
    public void testServedMergedObservables() {
        // setup
        Observable<Integer> os1 = Observable.range(0, 101);
        Observable<Integer> os2 = Observable.range(100, 101);
        ReplaySubject<Observable<Integer>> subject = ReplaySubject.create();
        subject.onNext(os1);
        subject.onNext(os2);
        subject.onCompleted();

        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int port = portSelector.acquirePort();

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(port)
                .addObservable(new ServeObservable.Builder<Integer>()
                        .encoder(Codecs.integer())
                        .observable(Observable.merge(subject))
                        .build())
                .build();

        // serve
        server.start();

        // connect
        Observable<Integer> oc = RemoteObservable.connect("localhost", port, Codecs.integer());
        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(20200, t1.intValue()); // sum of number 0-200
            }
        });
    }

    @Test
    public void testServedMergedObservablesAddAfterServe() {
        // setup
        Observable<Integer> os1 = Observable.range(0, 100);
        Observable<Integer> os2 = Observable.range(100, 100);
        ReplaySubject<Observable<Integer>> subject = ReplaySubject.create();
        subject.onNext(os1);
        subject.onNext(os2);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeObservable.Builder<Integer>()
                        .encoder(Codecs.integer())
                        .observable(Observable.merge(subject))
                        .build())
                .build();
        server.start();

        // add after serve
        Observable<Integer> os3 = Observable.range(200, 101);
        subject.onNext(os3);
        subject.onCompleted();

        // connect
        Observable<Integer> oc = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(45150, t1.intValue()); // sum of number 0-200
            }
        });
    }

    @Test
    public void testServedMergedObservablesAddAfterConnect() {
        // setup
        Observable<Integer> os1 = Observable.range(0, 100);
        Observable<Integer> os2 = Observable.range(100, 100);
        ReplaySubject<Observable<Integer>> subject = ReplaySubject.create();
        subject.onNext(os1);
        subject.onNext(os2);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeObservable.Builder<Integer>()
                        .encoder(Codecs.integer())
                        .observable(Observable.merge(subject))
                        .build())
                .build();
        server.start();

        // add after serve
        Observable<Integer> os3 = Observable.range(200, 100);
        subject.onNext(os3);

        // connect
        Observable<Integer> oc = RemoteObservable.connect("localhost", serverPort, Codecs.integer());

        // add after connect
        Observable<Integer> os4 = Observable.range(300, 101);
        subject.onNext(os4);
        subject.onCompleted();

        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(80200, t1.intValue()); // sum of number 0-200
            }
        });
    }

    //	@Test
    public void testRoundRobinSlottingServer() {
        // setup
        Observable<Integer> os = Observable.range(1, 100);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeObservable.Builder<Integer>()
                        .encoder(Codecs.integer())
                        .observable(os)
                        .slottingStrategy(new RoundRobin<Integer>())
                        .build())
                .build();
        server.start();

        // connect with 2 remotes
        Observable<Integer> oc1 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        Observable<Integer> oc2 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());

        // merge results
        Observable<Integer> merged = Observable.merge(oc1, oc2);
        // assert
        MathObservable.sumInteger(merged).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });

    }

    @Test
    public void testChainedRemoteObservables() throws InterruptedException {

        // first node
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        Observable<Integer> os = Observable.range(0, 100);

        int serverPort = portSelector.acquirePort();

        RemoteObservable.serve(serverPort, "source", os, Codecs.integer())
                .start();

        // middle node, receiving input from first node

        Observable<Integer> oc = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .name("source")
                .decoder(Codecs.integer())
                .build())
                .getObservable();

        // transform stream from first node
        Observable<Integer> transformed = oc.map(new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer t1) {
                return t1 + 1; // shift sequence by one
            }
        });
        int serverPort2 = portSelector.acquirePort();

        RemoteObservable.serve(serverPort2, "transformed", transformed, Codecs.integer())
                .start();

        // connect to second node
        Observable<Integer> oc2 = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort2)
                .name("transformed")
                .decoder(Codecs.integer())
                .build())
                .getObservable();


        MathObservable.sumInteger(oc2).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
    }

    @Test
    public void testError() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
            Observable<Integer> os = Observable.create(new OnSubscribe<Integer>() {
                @Override
                public void call(Subscriber<? super Integer> subscriber) {
                    subscriber.onNext(1);
                    subscriber.onError(new Exception("test-exception"));
                }
            });
            int serverPort = portSelector.acquirePort();
            RemoteObservable.serve(serverPort, os, Codecs.integer())
                .start();
            Observable<Integer> oc = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
            MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
                }
            });
        });
    }

    @Test
    public void testUnsubscribeForRemoteObservable() throws InterruptedException {
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);

        // serve up first observable
        final MutableReference<Boolean> sourceSubscriptionUnsubscribed = new MutableReference<Boolean>();
        Observable<Integer> os = Observable.create(new OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                int i = 0;
                sourceSubscriptionUnsubscribed.setValue(subscriber.isUnsubscribed());
                while (!sourceSubscriptionUnsubscribed.getValue()) {
                    subscriber.onNext(i++);
                    sourceSubscriptionUnsubscribed.setValue(subscriber.isUnsubscribed());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).subscribeOn(Schedulers.io());

        int serverPort = portSelector.acquirePort();
        RemoteObservable.serve(serverPort, os, Codecs.integer())
                .start();

        // connect to remote observable
        Observable<Integer> oc = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        Subscription sub = oc.subscribe();

        Assertions.assertEquals(false, sub.isUnsubscribed());
        Thread.sleep(1000); // allow a few iterations
        sub.unsubscribe();
        Thread.sleep(5000); // allow time for unsubscribe to propagate
        Assertions.assertEquals(true, sub.isUnsubscribed());
        Assertions.assertEquals(true, sourceSubscriptionUnsubscribed.getValue());
    }


    @Test
    public void testUnsubscribeForChainedRemoteObservable() throws InterruptedException {

        // serve first node in chain
        final MutableReference<Boolean> sourceSubscriptionUnsubscribed = new MutableReference<Boolean>();
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        Observable<Integer> os = Observable.create(new OnSubscribe<Integer>() {
            @Override
            public void call(Subscriber<? super Integer> subscriber) {
                int i = 0;
                sourceSubscriptionUnsubscribed.setValue(subscriber.isUnsubscribed());
                while (!sourceSubscriptionUnsubscribed.getValue()) {
                    subscriber.onNext(i++);
                    sourceSubscriptionUnsubscribed.setValue(subscriber.isUnsubscribed());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).subscribeOn(Schedulers.io());
        int serverPort = portSelector.acquirePort();
        RemoteObservable.serve(serverPort, os, Codecs.integer())
                .start();

        // serve second node in chain, using first node's observable
        Observable<Integer> oc1 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        int serverPort2 = portSelector.acquirePort();
        RemoteObservable.serve(serverPort2, oc1, Codecs.integer())
                .start();

        // connect to second node
        Observable<Integer> oc2 = RemoteObservable.connect("localhost", serverPort2, Codecs.integer());

        Subscription subscription = oc2.subscribe();

        // check client subscription
        Assertions.assertEquals(false, subscription.isUnsubscribed());

        Thread.sleep(4000); // allow a few iterations to complete

        // unsubscribe to client subscription
        subscription.unsubscribe();
        Thread.sleep(7000); // allow time for unsubscribe to propagate
        // check client
        Assertions.assertEquals(true, subscription.isUnsubscribed());
        // check source
        Assertions.assertEquals(true, sourceSubscriptionUnsubscribed.getValue());
    }

    @Test
    public void testSubscribeParametersByFilteringOnServer() {

        // setup
        Observable<Integer> os = Observable.range(0, 101);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeObservable.Builder<Integer>()
                        .encoder(Codecs.integer())
                        .observable(os)
                        .serverSideFilter(ServerSideFilters.oddsAndEvens())
                        .build())
                .build();
        server.start();

        // connect
        Map<String, String> subscribeParameters = new HashMap<String, String>();
        subscribeParameters.put("type", "even");

        Observable<Integer> oc = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .subscribeParameters(subscribeParameters)
                .decoder(Codecs.integer())
                .build())
                .getObservable();

        // assert
        MathObservable.sumInteger(oc).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(2550, t1.intValue()); // sum of number 0-100
            }
        });
    }

    @Test
    public void testOnCompletedFromReplaySubject() {
        PublishSubject<Integer> subject = PublishSubject.create();
        subject.onNext(1);
        subject.onNext(2);
        subject.onNext(3);
        subject.onCompleted();
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeObservable.Builder<Integer>()
                        .encoder(Codecs.integer())
                        .observable(subject)
                        .serverSideFilter(ServerSideFilters.oddsAndEvens())
                        .build())
                .build();
        server.start();
        // connect
        Observable<Integer> ro = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        final MutableReference<Boolean> completed = new MutableReference<Boolean>();
        ro.materialize().toBlocking().forEach(new Action1<Notification<Integer>>() {
            @Override
            public void call(Notification<Integer> notification) {
                if (notification.isOnCompleted()) {
                    completed.setValue(true);
                }
            }
        });
        Assertions.assertEquals(true, completed.getValue());
    }

}
