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
import io.reactivex.mantis.remote.observable.slotting.ConsistentHashing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observables.GroupedObservable;
import rx.schedulers.Schedulers;


public class RemoteGroupedObservableTest {

    @Test
    public void testConsistentSlottingServer() throws InterruptedException {
        // setup
        Observable<Observable<GroupedObservable<String, Integer>>> go = Observable.just(
                Observable.range(1, 10)
                        // subscribeOn to unblock range operator
                        .subscribeOn(Schedulers.io())
                        .groupBy(new Func1<Integer, String>() {
                            @Override
                            public String call(Integer t1) {
                                if (t1 % 2 == 0) {
                                    return "even";
                                } else {
                                    return "odd";
                                }
                            }
                        }));
        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeGroupedObservable.Builder<String, Integer>()
                        .name("grouped")
                        .keyEncoder(Codecs.string())
                        .valueEncoder(Codecs.integer())
                        .observable(go)
                        .slottingStrategy(new ConsistentHashing<Group<String, Integer>>())
                        .build())
                .build();
        server.start();

        Observable<GroupedObservable<String, Integer>> ro1 = RemoteObservable
                .connect(new ConnectToGroupedObservable.Builder<String, Integer>()
                        .host("localhost")
                        .port(serverPort)
                        .name("grouped")
                        .keyDecoder(Codecs.string())
                        .valueDecoder(Codecs.integer())
                        .build())
                .getObservable();

        Observable<GroupedObservable<String, Integer>> ro2 = RemoteObservable
                .connect(new ConnectToGroupedObservable.Builder<String, Integer>()
                        .host("localhost")
                        .port(serverPort)
                        .name("grouped")
                        .keyDecoder(Codecs.string())
                        .valueDecoder(Codecs.integer())
                        .build())
                .getObservable();


        final MutableReference<Integer> intRef = new MutableReference<>(0);
        Observable.merge(ro1, ro2)
                .flatMap(new Func1<GroupedObservable<String, Integer>, Observable<Result>>() {
                    @Override
                    public Observable<Result> call(final
                                                   GroupedObservable<String, Integer> group) {
                        return
                                group.reduce(new Func2<Integer, Integer, Integer>() {
                                    @Override
                                    public Integer call(Integer t1, Integer t2) {
                                        return t1 + t2;
                                    }
                                })
                                        .map(new Func1<Integer, Result>() {
                                            @Override
                                            public Result call(Integer t1) {
                                                return new Result(group.getKey(), t1);
                                            }
                                        });
                    }
                }).toBlocking().forEach(new Action1<Result>() {
            @Override
            public void call(Result result) {
                if (result.getKey().equals("odd")) {
                    Assertions.assertEquals(25, result.getResults().intValue());
                    intRef.setValue(intRef.getValue() + 1);
                } else {
                    Assertions.assertEquals(30, result.getResults().intValue());
                    intRef.setValue(intRef.getValue() + 1);
                }
            }
        });

        Assertions.assertEquals(2, intRef.getValue().intValue());
    }
}
