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

package io.mantisrx.runtime.source.http.impl;

import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_ATTEMPTED;

import io.mantisrx.runtime.source.http.HttpRequestFactory;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;


public class ServerClientContext<R, E> extends ServerContext<HttpClient<R, E>> {

    private final HttpRequestFactory<R> requestFactory;
    private final Observer<HttpSourceEvent> sourceObserver;

    private final Action1<ServerInfo> noOpAction = new Action1<ServerInfo>() {
        @Override
        public void call(ServerInfo t) {}
    };

    public ServerClientContext(ServerInfo server, HttpClient<R, E> client, HttpRequestFactory<R> requestFactory, Observer<HttpSourceEvent> sourceObserver) {
        super(server, client);

        this.requestFactory = requestFactory;
        this.sourceObserver = sourceObserver;

    }

    public HttpClient<R, E> getClient() {
        return getValue();
    }

    public Observable<HttpClientResponse<E>> newResponse(Action1<ServerInfo> connectionAttemptedCallback) {
        CONNECTION_ATTEMPTED.newEvent(sourceObserver, getServer());
        connectionAttemptedCallback.call(getServer());
        return getClient().submit(requestFactory.create());
    }

    public Observable<HttpClientResponse<E>> newResponse() {
        return newResponse(noOpAction);
    }
}
