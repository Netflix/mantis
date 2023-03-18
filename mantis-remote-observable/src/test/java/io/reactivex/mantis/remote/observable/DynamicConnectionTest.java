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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.observables.MathObservable;


public class DynamicConnectionTest {

    @Test
    public void dynamicConnectionTest() throws InterruptedException {

        // serve
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        final int serverPort1 = portSelector.acquirePort();
        final int serverPort2 = portSelector.acquirePort();

        RemoteRxServer server1 = RemoteObservable.serve(serverPort1, Observable.range(1, 100), Codecs.integer());
        server1.start();
        RemoteRxServer server2 = RemoteObservable.serve(serverPort2, Observable.range(101, 100), Codecs.integer());
        server2.start();


        Observable<Endpoint> endpointSwitch = Observable.create(new OnSubscribe<Endpoint>() {
            @Override
            public void call(Subscriber<? super Endpoint> subscriber) {
                subscriber.onNext(new Endpoint("localhost", serverPort1));
                // switch connection
                subscriber.onNext(new Endpoint("localhost", serverPort2));
            }
        });

        ConnectToObservable.Builder<Integer> config = new ConnectToObservable.Builder<Integer>()
                .decoder(Codecs.integer());

        DynamicConnection<Integer> connection = DynamicConnection.create(config, endpointSwitch);


        MathObservable.sumInteger(connection.observable())
                .last()
                .subscribe(new Action1<Integer>() {
                    @Override
                    public void call(Integer t1) {
                        Assertions.assertEquals(20100, t1.intValue());
                    }
                });

        Thread.sleep(1000); // wait for async computation

        connection.close();

    }
}
