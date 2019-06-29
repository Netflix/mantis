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

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent;
import io.netty.buffer.ByteBuf;
import rx.Observable;
import rx.Observer;


public class HttpSource<E, T> implements Source<T> {

    private final HttpSourceImpl<ByteBuf, E, T> impl;

    private HttpSource(HttpSourceImpl<ByteBuf, E, T> impl) {
        this.impl = impl;
    }

    public static <E, T> Builder<E, T> builder(HttpSourceImpl.Builder<ByteBuf, E, T> builderImpl) {
        return new Builder<>(builderImpl);
    }

    public static <E> Builder<E, E> builder(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory) {
        HttpSourceImpl.Builder<ByteBuf, E, E> builderImpl = HttpSourceImpl
                .builder(
                        clientFactory,
                        requestFactory,
                        HttpSourceImpl.<E>identityConverter());

        return new Builder<>(builderImpl);
    }

    public static <E> Builder<E, E> builder(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory, ClientResumePolicy<ByteBuf, E> policy) {
        HttpSourceImpl.Builder<ByteBuf, E, E> builderImpl = HttpSourceImpl
                .builder(
                        clientFactory,
                        requestFactory,
                        HttpSourceImpl.<E>identityConverter(),
                        policy);

        return new Builder<>(builderImpl);
    }

    @Override
    public Observable<Observable<T>> call(Context context, Index index) {
        return impl.call(context, index);
    }

    public static class Builder<E, T> {

        private final HttpSourceImpl.Builder<ByteBuf, E, T> builderImpl;

        private Builder(HttpSourceImpl.Builder<ByteBuf, E, T> builderImpl) {
            this.builderImpl = builderImpl;
        }

        public Builder<E, T> withServerProvider(HttpServerProvider serverProvider) {
            builderImpl.withServerProvider(serverProvider);

            return this;
        }

        public Builder<E, T> withActivityObserver(Observer<HttpSourceEvent> observer) {
            builderImpl.withActivityObserver(observer);

            return this;
        }

        public Builder<E, T> resumeWith(ClientResumePolicy<ByteBuf, E> policy) {
            builderImpl.resumeWith(policy);

            return this;
        }

        public HttpSource<E, T> build() {
            return new HttpSource<>(builderImpl.build());
        }
    }
}