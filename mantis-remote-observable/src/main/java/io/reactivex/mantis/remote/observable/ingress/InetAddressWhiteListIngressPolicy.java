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

package io.reactivex.mantis.remote.observable.ingress;

import io.reactivex.mantis.remote.observable.RemoteRxEvent;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import rx.Observable;
import rx.functions.Action1;


public class InetAddressWhiteListIngressPolicy implements IngressPolicy {

    private AtomicReference<Set<String>> whiteList = new AtomicReference<Set<String>>();

    InetAddressWhiteListIngressPolicy(Observable<Set<String>> allowedIpAddressesObservable) {
        allowedIpAddressesObservable.subscribe(new Action1<Set<String>>() {
            @Override
            public void call(Set<String> newList) {
                whiteList.set(newList);
            }
        });
    }

    @Override
    public boolean allowed(
            ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {
        InetSocketAddress inetSocketAddress
                = (InetSocketAddress) connection.getChannel().remoteAddress();
        return whiteList.get().contains(inetSocketAddress.getAddress().getHostAddress());
    }

}
