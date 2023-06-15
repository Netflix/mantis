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
import rx.functions.Action1;
import rx.observables.MathObservable;


public class ServeNestedTest {

    @Test
    public void testServeNested() {

        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        Observable<Observable<Integer>> oo = Observable.just(Observable.range(1, 100));

        RemoteRxServer server = new RemoteRxServer.Builder()
                .port(serverPort)
                .addObservable(new ServeNestedObservable.Builder<Integer>()
                        .name("ints")
                        .encoder(Codecs.integer())
                        .observable(oo)
                        .build())
                .build();

        server.start();

        Observable<Integer> ro = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort)
                .name("ints")
                .decoder(Codecs.integer())
                .build())
                .getObservable();


        MathObservable.sumInteger(ro).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
    }

    @Test
    public void testServeNestedChained() {

        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort1 = portSelector.acquirePort();
        int serverPort2 = portSelector.acquirePort();

        Observable<Observable<Integer>> oo = Observable.just(Observable.range(1, 100));

        RemoteRxServer server1 = new RemoteRxServer.Builder()
                .port(serverPort1)
                .addObservable(new ServeNestedObservable.Builder<Integer>()
                        .name("ints")
                        .encoder(Codecs.integer())
                        .observable(oo)
                        .build())
                .build();

        server1.start();

        Observable<Integer> ro1 = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort1)
                .name("ints")
                .decoder(Codecs.integer())
                .build())
                .getObservable();

        RemoteRxServer server2 = new RemoteRxServer.Builder()
                .port(serverPort2)
                .addObservable(new ServeNestedObservable.Builder<Integer>()
                        .name("ints")
                        .encoder(Codecs.integer())
                        .observable(Observable.just(ro1))
                        .build())
                .build();

        server2.start();

        Observable<Integer> ro2 = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .port(serverPort2)
                .name("ints")
                .decoder(Codecs.integer())
                .build())
                .getObservable();

        MathObservable.sumInteger(ro2).toBlocking().forEach(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                Assertions.assertEquals(5050, t1.intValue()); // sum of number 0-100
            }
        });
    }
}
