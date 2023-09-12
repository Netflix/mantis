/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.worker.client;

import io.netty.bootstrap.Bootstrap;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientChannelFactoryImpl;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.client.UnpooledClientConnectionFactory;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient.HttpClientConfig.Builder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;

public class MantisHttpClientBuilder<I, O> extends HttpClientBuilder<I, O> {
    public MantisHttpClientBuilder(String host, int port) {
        super(host, port, new Bootstrap());
    }

    public MantisHttpClientBuilder(String host, int port, Bootstrap bootstrap) {
        super(bootstrap, host, port, new UnpooledClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>>(), new ClientChannelFactoryImpl<HttpClientResponse<O>, HttpClientRequest<I>>(bootstrap));
    }

    public MantisHttpClientBuilder(Bootstrap bootstrap, String host, int port, ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionFactory, ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> factory) {
        super(bootstrap, host, port, connectionFactory, factory);
        this.clientConfig = Builder.newDefaultConfig();
        this.pipelineConfigurator(PipelineConfigurators.httpClientConfigurator());
    }

    public MantisHttpClientBuilder(Bootstrap bootstrap, String host, int port, ConnectionPoolBuilder<HttpClientResponse<O>, HttpClientRequest<I>> poolBuilder) {
        super(bootstrap, host, port, poolBuilder);
        this.clientConfig = Builder.newDefaultConfig();
        this.pipelineConfigurator(PipelineConfigurators.httpClientConfigurator());
    }

    @Override
    protected HttpClient<I, O> createClient() {
        if (null == super.poolBuilder) {
            return new MantisHttpClientImpl<I, O>(this.getOrCreateName(), this.serverInfo, this.bootstrap, this.pipelineConfigurator, this.clientConfig, this.channelFactory, this.connectionFactory, this.eventsSubject);
        } else {
            return new MantisHttpClientImpl<I, O>(this.getOrCreateName(), this.serverInfo, this.bootstrap, this.pipelineConfigurator, this.clientConfig, this.poolBuilder, this.eventsSubject);
        }
    }
}
