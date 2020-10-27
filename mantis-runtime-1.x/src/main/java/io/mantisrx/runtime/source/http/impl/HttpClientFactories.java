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

import io.mantisrx.runtime.source.http.HttpClientFactory;
import io.netty.buffer.ByteBuf;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;


public class HttpClientFactories {

    public static HttpClientFactory<ByteBuf, ByteBuf> defaultFactory() {
        return new DefaultHttpClientFactory();
    }

    public static HttpClientFactory<ByteBuf, ServerSentEvent> sseClientFactory() {
        return new SSEClientFactory();
    }

    public static HttpClientFactory<ByteBuf, ServerSentEvent> sseClientFactory(int readTimeout) {
        return new SSEClientFactory(readTimeout);
    }


    private static class DefaultHttpClientFactory implements HttpClientFactory<ByteBuf, ByteBuf> {

        private final HttpClient.ClientConfig clientConfig;

        public DefaultHttpClientFactory() {
            clientConfig = new HttpClient.HttpClientConfig.Builder()
                    .setFollowRedirect(true)
                    .userAgent("Netflix Mantis HTTP Source")

                    .build();
        }

        @Override
        public HttpClient<ByteBuf, ByteBuf> createClient(ServerInfo server) {
            return new HttpClientBuilder<ByteBuf, ByteBuf>(server.getHost(), server.getPort())
                    .config(clientConfig)
                    .build();
        }
    }

    private static class SSEClientFactory implements HttpClientFactory<ByteBuf, ServerSentEvent> {

        public static final int DEFAULT_READ_TIMEOUT_SECS = 5;
        private int readTimeout = DEFAULT_READ_TIMEOUT_SECS;

        public SSEClientFactory(int readTimeout) {
            this.readTimeout = readTimeout;
        }

        public SSEClientFactory() {
            this(DEFAULT_READ_TIMEOUT_SECS);
        }


        @Override
        public HttpClient<ByteBuf, ServerSentEvent> createClient(ServerInfo server) {
            //        	ClientConfig clientConfig = new HttpClient.HttpClientConfig.Builder()
            //                    .readTimeout(this.readTimeout, TimeUnit.SECONDS)
            //                    .userAgent("Netflix Mantis HTTP Source")
            //                    .build();
            return RxNetty.createHttpClient(
                    server.getHost(),
                    server.getPort(),
                    //PipelineConfigurators.createClientConfigurator(new  SseClientPipelineConfigurator<ByteBuf>(), clientConfig));
                    PipelineConfigurators.<ByteBuf>clientSseConfigurator());
        }
    }
}
