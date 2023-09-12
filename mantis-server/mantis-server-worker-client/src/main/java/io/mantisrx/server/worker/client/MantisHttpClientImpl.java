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
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import java.util.ArrayList;
import java.util.List;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.client.ConnectionPool;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.metrics.MetricEventsSubject;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientImpl;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class MantisHttpClientImpl<I, O> extends HttpClientImpl<I, O> {
    private static final Logger logger = LoggerFactory.getLogger(MantisHttpClientImpl.class);

    private Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> observableConection;
    private List<Channel> connectionTracker;

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> channelFactory, ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, channelFactory, connectionFactory, eventsSubject);
        this.connectionTracker = new ArrayList<>();
    }

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<HttpClientResponse<O>, HttpClientRequest<I>> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, poolBuilder, eventsSubject);
        this.connectionTracker = new ArrayList<>();
    }

    @Override
    public Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connect() {
        this.observableConection = super.connect();
        return this.observableConection.doOnNext(conn -> {
            logger.info("Tracking connection: {}", conn.getChannel().toString());
            this.connectionTracker.add(conn.getChannel());
        });
    }

    public void closeConn() {
        Channel channel;
        for (Channel value : this.connectionTracker) {
            channel = value;
            logger.info("Closing connection: {}. Status at close: isActive: {}, isOpen: {}, isWritable: {}",
                    channel.toString(), channel.isActive(), channel.isOpen(), channel.isWritable());
            channel.close();
        }
        this.connectionTracker.clear();
    }
}
