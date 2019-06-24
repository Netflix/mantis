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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.reactivex.mantis.remote.observable.RemoteRxEvent;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import rx.Observable;
import rx.subjects.ReplaySubject;


public class IngressPolicies {

    private IngressPolicies() {}

    public static IngressPolicy whiteListPolicy(Observable<Set<String>> whiteList) {
        return new InetAddressWhiteListIngressPolicy(whiteList);
    }

    public static IngressPolicy allowOnlyLocalhost() {
        ReplaySubject<Set<String>> subject = ReplaySubject.create();
        Set<String> list = new HashSet<String>();
        list.add("127.0.0.1");
        subject.onNext(list);
        return new InetAddressWhiteListIngressPolicy(subject);
    }

    public static IngressPolicy allowAll() {
        return new IngressPolicy() {
            @Override
            public boolean allowed(
                    ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {
                return true;
            }
        };
    }
}
