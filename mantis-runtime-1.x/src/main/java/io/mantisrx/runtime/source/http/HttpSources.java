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

package io.mantisrx.runtime.source.http;

import java.nio.charset.Charset;

import io.mantisrx.runtime.source.http.HttpSource.Builder;
import io.mantisrx.runtime.source.http.impl.HttpClientFactories;
import io.mantisrx.runtime.source.http.impl.HttpRequestFactories;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl;
import io.mantisrx.runtime.source.http.impl.ServerClientContext;
import io.mantisrx.runtime.source.http.impl.ServerContext;
import io.netty.buffer.ByteBuf;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func2;


public class HttpSources {

    public static <E> Builder<E, E> source(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory) {
        return HttpSource.builder(clientFactory, requestFactory);
    }


    public static <E> Builder<E, E> sourceWithResume(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory, ClientResumePolicy<ByteBuf, E> policy) {
        return HttpSource.builder(clientFactory, requestFactory, policy);
    }

    public static <E> ContextualHttpSource.Builder<E> contextualSource(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory) {
        return ContextualHttpSource.builder(clientFactory, requestFactory);
    }

    public static <E> ContextualHttpSource.Builder<E> contextualSourceWithResume(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory, ClientResumePolicy<ByteBuf, E> policy) {
        return ContextualHttpSource.builder(clientFactory, requestFactory, policy);
    }

    /**
     * Create a {@code HttpSource} that infinitely polls the give server with the given URL
     *
     * @param host The host name of the server to be queried
     * @param port The port used for query
     * @param uri  The URI used for query. The URI should be relative to http://host:port/
     *
     * @return A builder that will return an implementation that polls the give server infinitely
     */
    public static Builder<ByteBuf, String> pollingSource(final String host, final int port, String uri) {
        HttpClientFactory<ByteBuf, ByteBuf> clientFactory = HttpClientFactories.defaultFactory();
        HttpSourceImpl.Builder<ByteBuf, ByteBuf, String> builderImpl = HttpSourceImpl
                .builder(
                        clientFactory,
                        HttpRequestFactories.createGetFactory(uri),
                        new Func2<ServerContext<HttpClientResponse<ByteBuf>>, ByteBuf, String>() {
                            @Override
                            public String call(ServerContext<HttpClientResponse<ByteBuf>> context, ByteBuf content) {
                                return content.toString(Charset.defaultCharset());
                            }
                        })
                .withServerProvider(new HttpServerProvider() {
                    @Override
                    public Observable<ServerInfo> getServersToAdd() {
                        return Observable.just(new ServerInfo(host, port));
                    }

                    @Override
                    public Observable<ServerInfo> getServersToRemove() {
                        return Observable.empty();
                    }
                })
                .resumeWith(new ClientResumePolicy<ByteBuf, ByteBuf>() {
                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> onError(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts, Throwable error) {
                        return clientContext.newResponse();
                    }

                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> onCompleted(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts) {
                        return clientContext.newResponse();
                    }
                });

        return HttpSource.builder(builderImpl);

    }
}
