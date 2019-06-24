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
import io.mantisrx.runtime.source.http.impl.ServerContext;
import io.netty.buffer.ByteBuf;
import rx.Observable;
import rx.Observer;


public class ContextualHttpSource<E> implements Source<ServerContext<E>> {

    private final HttpSourceImpl<ByteBuf, E, ServerContext<E>> impl;

    private ContextualHttpSource(HttpSourceImpl<ByteBuf, E, ServerContext<E>> impl) {
        this.impl = impl;
    }

    public static <E> Builder<E> builder(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory) {
        HttpSourceImpl.Builder<ByteBuf, E, ServerContext<E>> builderImpl = HttpSourceImpl
                .builder(
                        clientFactory,
                        requestFactory,
                        HttpSourceImpl.<E>contextWrapper());

        return new Builder<>(builderImpl);
    }

    public static <E> Builder<E> builder(HttpClientFactory<ByteBuf, E> clientFactory, HttpRequestFactory<ByteBuf> requestFactory, ClientResumePolicy<ByteBuf, E> policy) {
        HttpSourceImpl.Builder<ByteBuf, E, ServerContext<E>> builderImpl = HttpSourceImpl
                .builder(
                        clientFactory,
                        requestFactory,
                        HttpSourceImpl.<E>contextWrapper(),
                        policy);

        return new Builder<>(builderImpl);
    }

    @Override
    public Observable<Observable<ServerContext<E>>> call(Context context, Index t2) {
        return impl.call(context, t2);
    }

    public static class Builder<E> {

        private final HttpSourceImpl.Builder<ByteBuf, E, ServerContext<E>> builderImpl;

        private Builder(HttpSourceImpl.Builder<ByteBuf, E, ServerContext<E>> builderImpl) {
            this.builderImpl = builderImpl;
        }

        public Builder<E> withServerProvider(HttpServerProvider serverProvider) {
            builderImpl.withServerProvider(serverProvider);

            return this;
        }

        public Builder<E> withActivityObserver(Observer<HttpSourceEvent> observer) {
            builderImpl.withActivityObserver(observer);

            return this;
        }

        public Builder<E> resumeWith(ClientResumePolicy<ByteBuf, E> policy) {
            builderImpl.resumeWith(policy);

            return this;
        }


        public ContextualHttpSource<E> build() {
            return new ContextualHttpSource<>(builderImpl.build());
        }
    }
}