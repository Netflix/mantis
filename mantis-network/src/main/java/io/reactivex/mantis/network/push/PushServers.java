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

package io.reactivex.mantis.network.push;

import java.util.List;
import java.util.Map;

import io.mantisrx.common.MantisGroup;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observables.GroupedObservable;
import rx.subjects.PublishSubject;


public class PushServers {

    private PushServers() {}

    public static <T> LegacyTcpPushServer<T> infiniteStreamLegacyTcpNested(ServerConfig<T> config, Observable<Observable<T>> o) {
        final PublishSubject<String> serverSignals = PublishSubject.create();
        final String serverName = config.getName();
        Action0 onComplete = new Action0() {
            @Override
            public void call() {
                serverSignals.onNext("ILLEGAL_STATE_COMPLETED");
                throw new IllegalStateException("OnComplete signal received, Server: " + serverName + " is pushing an infinite stream, should not complete");
            }
        };

        Action1<Throwable> onError = new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {
                serverSignals.onError(t);
            }
        };

        PushTrigger<T> trigger = ObservableTrigger.oo(serverName, o, onComplete, onError);
        return new LegacyTcpPushServer<T>(trigger, config, serverSignals);
    }

    public static <K, V> LegacyTcpPushServer<KeyValuePair<K, V>> infiniteStreamLegacyTcpNestedGroupedObservable(ServerConfig<KeyValuePair<K, V>> config,
                                                                                                                Observable<Observable<GroupedObservable<K, V>>> go,
                                                                                                                long groupExpirySeconds, final Func1<K, byte[]> keyEncoder,
                                                                                                                HashFunction hashFunction) {
        final PublishSubject<String> serverSignals = PublishSubject.create();
        final String serverName = config.getName();
        Action0 onComplete = new Action0() {
            @Override
            public void call() {
                serverSignals.onNext("ILLEGAL_STATE_COMPLETED");
                throw new IllegalStateException("OnComplete signal received, Server: " + serverName + " is pushing an infinite stream, should not complete");
            }
        };

        Action1<Throwable> onError = new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {
                serverSignals.onError(t);
            }
        };

        PushTrigger<KeyValuePair<K, V>> trigger = ObservableTrigger.oogo(serverName, go, onComplete, onError, groupExpirySeconds,
                keyEncoder, hashFunction);
        return new LegacyTcpPushServer<KeyValuePair<K, V>>(trigger, config, serverSignals);
    }

    // NJ
    public static <K, V> LegacyTcpPushServer<KeyValuePair<K, V>> infiniteStreamLegacyTcpNestedMantisGroup(ServerConfig<KeyValuePair<K, V>> config,
                                                                                                          Observable<Observable<MantisGroup<K, V>>> go,
                                                                                                          long groupExpirySeconds, final Func1<K, byte[]> keyEncoder,
                                                                                                          HashFunction hashFunction) {
        final PublishSubject<String> serverSignals = PublishSubject.create();
        final String serverName = config.getName();
        Action0 onComplete = new Action0() {
            @Override
            public void call() {
                serverSignals.onNext("ILLEGAL_STATE_COMPLETED");
                throw new IllegalStateException("OnComplete signal received, Server: " + serverName + " is pushing an infinite stream, should not complete");
            }
        };

        Action1<Throwable> onError = new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {
                serverSignals.onError(t);
            }
        };

        PushTrigger<KeyValuePair<K, V>> trigger = ObservableTrigger.oomgo(serverName, go, onComplete, onError, groupExpirySeconds,
                keyEncoder, hashFunction);
        return new LegacyTcpPushServer<KeyValuePair<K, V>>(trigger, config, serverSignals);
    }

    public static <T, S> PushServerSse<T, S> infiniteStreamSse(ServerConfig<T> config, Observable<T> o,
                                                               Func2<Map<String, List<String>>, S, Void> requestPreprocessor,
                                                               Func2<Map<String, List<String>>, S, Void> requestPostprocessor,
                                                               final Func2<Map<String, List<String>>, S, Void> subscribeProcessor,
                                                               S state, boolean supportLegacyMetrics) {

        final String serverName = config.getName();
        final PublishSubject<String> serverSignals = PublishSubject.create();
        Action0 onComplete = new Action0() {
            @Override
            public void call() {
                serverSignals.onNext("ILLEGAL_STATE_COMPLETED");
                throw new IllegalStateException("OnComplete signal received, Server: " + serverName + " is pushing an infinite stream, should not complete");
            }
        };

        Action1<Throwable> onError = new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {
                serverSignals.onError(t);
            }
        };

        PushTrigger<T> trigger = ObservableTrigger.o(serverName, o, onComplete, onError);

        return new PushServerSse<T, S>(trigger, config, serverSignals,
                requestPreprocessor, requestPostprocessor,
                subscribeProcessor, state, supportLegacyMetrics);
    }

    public static <T> PushServerSse<T, Void> infiniteStreamSse(ServerConfig<T> config, Observable<T> o) {
        return
                infiniteStreamSse(config, o, null, null, null, null, false);
    }
}
