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
    protected final String name;
    protected final ServerInfo serverInfo;
    protected final Bootstrap clientBootstrap;
    protected final PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator;
    protected final ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> channelFactory;
    protected final ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionFactory;
    protected final ClientConfig clientConfig;
    protected final MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject;
    protected final ConnectionPool<HttpClientResponse<O>, HttpClientRequest<I>> pool;

    private Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> observableConection;
    private List<Channel> connectionTracker;

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> channelFactory, ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, channelFactory, connectionFactory, eventsSubject);

        this.connectionTracker = new ArrayList<>();

        if (null == name) {
            throw new NullPointerException("Name can not be null.");
        } else if (null == clientBootstrap) {
            throw new NullPointerException("Client bootstrap can not be null.");
        } else if (null == serverInfo) {
            throw new NullPointerException("Server info can not be null.");
        } else if (null == clientConfig) {
            throw new NullPointerException("Client config can not be null.");
        } else if (null == connectionFactory) {
            throw new NullPointerException("Connection factory can not be null.");
        } else if (null == channelFactory) {
            throw new NullPointerException("Channel factory can not be null.");
        } else {
            this.name = name;
            this.pool = null;
            this.eventsSubject = eventsSubject;
            this.clientConfig = clientConfig;
            this.serverInfo = serverInfo;
            this.clientBootstrap = clientBootstrap;
            this.connectionFactory = connectionFactory;
            this.connectionFactory.useMetricEventsSubject(eventsSubject);
            this.channelFactory = channelFactory;
            this.channelFactory.useMetricEventsSubject(eventsSubject);
            this.pipelineConfigurator = pipelineConfigurator;
            final PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> configurator = this.adaptPipelineConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
            this.clientBootstrap.handler(new ChannelInitializer<Channel>() {
                public void initChannel(Channel ch) throws Exception {
                    configurator.configureNewPipeline(ch.pipeline());
                }
            });
        }
    }

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<HttpClientResponse<O>, HttpClientRequest<I>> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, poolBuilder, eventsSubject);

        this.connectionTracker = new ArrayList<>();

        if (null == name) {
            throw new NullPointerException("Name can not be null.");
        } else if (null == clientBootstrap) {
            throw new NullPointerException("Client bootstrap can not be null.");
        } else if (null == serverInfo) {
            throw new NullPointerException("Server info can not be null.");
        } else if (null == clientConfig) {
            throw new NullPointerException("Client config can not be null.");
        } else if (null == poolBuilder) {
            throw new NullPointerException("Pool builder can not be null.");
        } else {
            this.name = name;
            this.eventsSubject = eventsSubject;
            this.clientConfig = clientConfig;
            this.serverInfo = serverInfo;
            this.clientBootstrap = clientBootstrap;
            this.pipelineConfigurator = pipelineConfigurator;
            final PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> configurator = this.adaptPipelineConfigurator(pipelineConfigurator, clientConfig, eventsSubject);
            this.clientBootstrap.handler(new ChannelInitializer<Channel>() {
                public void initChannel(Channel ch) throws Exception {
                    configurator.configureNewPipeline(ch.pipeline());
                }
            });
            this.pool = poolBuilder.build();
            this.channelFactory = poolBuilder.getChannelFactory();
            this.connectionFactory = (ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>>) poolBuilder.getConnectionFactory();
        }
    }

    public Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connect() {
        this.observableConection = super.connect();
        return this.observableConection.doOnNext(x -> trackConnection(x.getChannel()));
    }

    protected void trackConnection(Channel channel) {
        logger.info("Tracking connection: {}", channel.toString());
        this.connectionTracker.add(channel);
    }

    protected void closeConn() {
        Channel channel;
        for (Channel value : this.connectionTracker) {
            channel = value;
            logger.info("Closing connection: {}", channel.toString());
            channel.close();
        }
        this.connectionTracker.clear();
    }

    protected int connectionTrackerSize() {
        return this.connectionTracker.size();
    }

    protected boolean isObservableConectionSet() {
        return (this.observableConection != null);
    }
}
