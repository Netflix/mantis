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


public class MergedObservableTest {


    @Test
    public void testMergeCount() {
        MergeCounts counts = new MergeCounts(3);

        Assertions.assertEquals(false, counts.incrementTerminalCountAndCheck());
        Assertions.assertEquals(false, counts.incrementTerminalCountAndCheck());
        Assertions.assertEquals(true, counts.incrementTerminalCountAndCheck());
    }


    @Test
    public void testFixedMerge() throws InterruptedException {

        MergedObservable<Integer> merged = MergedObservable.createWithReplay(2);
        merged.mergeIn("t1", Observable.range(1, 50));
        merged.mergeIn("t2", Observable.range(51, 50));

        MathObservable.sumInteger(Observable.merge(merged.get()))
                .toBlocking()
                .forEach(new Action1<Integer>() {
                    @Override
                    public void call(Integer t1) {
                        Assertions.assertEquals(5050, t1.intValue());
                    }
                });
    }

    @Test
    public void testMergeInBadObservable() throws InterruptedException {
        Assertions.assertThrows(RuntimeException.class, () -> {
            MergedObservable<Integer> merged = MergedObservable.createWithReplay(2);
            merged.mergeIn("t1", Observable.range(1, 50));

            Observable<Integer> badObservable = Observable.create(new OnSubscribe<Integer>() {
                @Override
                public void call(Subscriber<? super Integer> subscriber) {
                    for (int i = 100; i < 200; i++) {
                        subscriber.onNext(i);
                        if (i == 150) {
                            subscriber.onError(new Exception("bad"));
                        }
                    }
                }
            });
            merged.mergeIn("t2", badObservable);

            MathObservable.sumInteger(Observable.merge(merged.get()))
                .toBlocking()
                .forEach(new Action1<Integer>() {
                    @Override
                    public void call(Integer t1) {
                        Assertions.assertEquals(5050, t1.intValue());
                    }
                });
        });
    }

        @Test
        public void testSingleRemoteObservableMerge() {
            // setup
            Observable<Integer> os = Observable.range(0, 101);
            // serve
            PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
            int serverPort = portSelector.acquirePort();
            RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
            server.start();
            // connect
            Observable<Integer> ro = RemoteObservable.connect("localhost", serverPort, Codecs.integer());

            MergedObservable<Integer> merged = MergedObservable.createWithReplay(1);
            merged.mergeIn("t1", ro);

            // assert
            MathObservable.sumInteger(Observable.merge(merged.get()))
                .toBlocking().forEach(new Action1<Integer>() {
                    @Override
                    public void call(Integer t1) {
                        Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
                    }
                });
        }

    @Test
    public void testThreeRemoteObservablesMerge() {
        // setup
        Observable<Integer> os = Observable.range(0, 101);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();
        RemoteRxServer server = RemoteObservable.serve(serverPort, os, Codecs.integer());
        server.start();
        // connect
        Observable<Integer> ro1 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        Observable<Integer> ro2 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());
        Observable<Integer> ro3 = RemoteObservable.connect("localhost", serverPort, Codecs.integer());

        MergedObservable<Integer> merged = MergedObservable.createWithReplay(3);
        merged.mergeIn("t1", ro1);
        merged.mergeIn("t2", ro2);
        merged.mergeIn("t3", ro3);

        // assert
        MathObservable.sumInteger(Observable.merge(merged.get()))
                .toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(15150, t1.intValue()); // sum of number 0-100
            }
        });
    }

    //@Test
    public void testMergeFromMultipleSources() {
        // setup
        Observable<Integer> os1 = Observable.range(1, 100);
        Observable<Integer> os2 = Observable.range(101, 100);
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort1 = portSelector.acquirePort();
        int serverPort2 = portSelector.acquirePort();


        RemoteRxServer server1 = RemoteObservable.serve(serverPort1, os1, Codecs.integer());
        server1.start();

        RemoteRxServer server2 = RemoteObservable.serve(serverPort2, os2, Codecs.integer());
        server2.start();

        // connect
        Observable<Integer> ro1 = RemoteObservable.connect("localhost", serverPort1, Codecs.integer());
        Observable<Integer> ro2 = RemoteObservable.connect("localhost", serverPort2, Codecs.integer());

        MergedObservable<Integer> merged = MergedObservable.createWithReplay(2);
        merged.mergeIn("t1", ro1);
        merged.mergeIn("t2", ro2);

        // assert
        MathObservable.sumInteger(Observable.merge(merged.get()))
                .toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(20100, t1.intValue()); // sum of number 0-100
            }
        });
    }

}
