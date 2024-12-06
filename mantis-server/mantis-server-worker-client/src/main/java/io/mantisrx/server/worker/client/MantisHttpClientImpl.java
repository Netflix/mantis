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

import com.netflix.spectator.api.Tag;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.client.ClientChannelFactory;
import mantis.io.reactivex.netty.client.ClientConnectionFactory;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.client.ConnectionPoolBuilder;
import mantis.io.reactivex.netty.metrics.MetricEventsSubject;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientImpl;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;

@Slf4j
public class MantisHttpClientImpl<I, O> extends HttpClientImpl<I, O> {

    private Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> observableConection;
    private List<Channel> connectionTracker;

    private AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Gauge numConnectionsTracked;
    private final static String connectionTrackerMetricgroup = "ConnectionMonitor";
    private final static String metricName = "numConnectionsTracked";
    private final static String metricTagName = "uuid";

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ClientChannelFactory<HttpClientResponse<O>, HttpClientRequest<I>> channelFactory, ClientConnectionFactory<HttpClientResponse<O>, HttpClientRequest<I>, ? extends ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connectionFactory, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, channelFactory, connectionFactory, eventsSubject);
        this.connectionTracker = new ArrayList<>();

        Tag metricTag = Tag.of(metricTagName, UUID.randomUUID().toString());
        Metrics m = new Metrics.Builder()
                .id(connectionTrackerMetricgroup, metricTag)
                .addGauge(metricName)
                .build();
        this.numConnectionsTracked = m.getGauge(metricName);
    }

    public MantisHttpClientImpl(String name, ServerInfo serverInfo, Bootstrap clientBootstrap, PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> pipelineConfigurator, ClientConfig clientConfig, ConnectionPoolBuilder<HttpClientResponse<O>, HttpClientRequest<I>> poolBuilder, MetricEventsSubject<ClientMetricsEvent<?>> eventsSubject) {
        super(name, serverInfo, clientBootstrap, pipelineConfigurator, clientConfig, poolBuilder, eventsSubject);
        this.connectionTracker = new ArrayList<>();

        Tag metricTag = Tag.of(metricTagName, UUID.randomUUID().toString());
        Metrics m = new Metrics.Builder()
                .id(connectionTrackerMetricgroup, metricTag)
                .addGauge(metricName)
                .build();
        this.numConnectionsTracked = m.getGauge(metricName);
    }

    @Override
    public Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> connect() {
        this.observableConection = super.connect();
        return this.observableConection.doOnNext(conn -> trackConnection(conn.getChannel()));
    }

    protected void trackConnection(Channel channel) {
        log.debug("Tracking connection: {}", channel.toString());
        synchronized (connectionTracker) {
            if (isClosed.get()) {
                log.info("Http client is already closed. Close the channel immediately. {}", channel);
                channel.close();
            } else {
                this.connectionTracker.add(channel);
                numConnectionsTracked.increment();
            }
        }
    }

    protected void closeConn() {
        synchronized (connectionTracker) {
            isClosed.set(true);
            resetConnInternalUnsafe();
        }
    }

    protected void resetConn() {
        synchronized (connectionTracker) {
            resetConnInternalUnsafe();
        }
    }

    /**
     * Not thread safe, must be called with explicit lock.
     */
    private void resetConnInternalUnsafe() {
        for (Channel value : this.connectionTracker) {
            Channel channel = value;
            log.info("Closing connection: {}. Status at close: isActive: {}, isOpen: {}, isWritable: {}",
                channel.toString(), channel.isActive(), channel.isOpen(), channel.isWritable());
            channel.close();
            numConnectionsTracked.decrement();
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
